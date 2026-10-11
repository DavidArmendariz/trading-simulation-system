package bus

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dealmaker/market-sim/gateway/internal/config"
	"github.com/dealmaker/market-sim/gateway/internal/store"
	"github.com/dealmaker/market-sim/gateway/internal/ws"
)

type tradeBatch struct {
	Seq    uint64        `json:"seq"`
	Ts     uint64        `json:"ts"`
	Symbol string        `json:"symbol"`
	Trades []store.Trade `json:"trades"`
}

// Bus is the single Redis subscriber. It fans market data out to the WebSocket
// hub and the persistence layer, and publishes control commands back.
type Bus struct {
	cfg     config.Config
	rdb     *redis.Client
	hub     *ws.Hub
	writer  *store.TradeWriter
	candles *store.CandleAggregator

	engineStats atomic.Value // json.RawMessage
	agentStatus atomic.Value // json.RawMessage

	mu          sync.Mutex
	workerRates map[int]workerRate
}

// A worker that has gone away stops reporting. Its last rate must not linger in
// the total, or a stopped pool keeps claiming throughput.
type workerRate struct {
	perSec int64
	seenAt time.Time
}

const workerRateTTL = 5 * time.Second

func New(cfg config.Config, rdb *redis.Client, hub *ws.Hub, writer *store.TradeWriter,
	candles *store.CandleAggregator) *Bus {
	return &Bus{
		cfg:         cfg,
		rdb:         rdb,
		hub:         hub,
		writer:      writer,
		candles:     candles,
		workerRates: make(map[int]workerRate),
	}
}

func (b *Bus) Run(ctx context.Context) {
	sub := b.rdb.Subscribe(ctx,
		config.ChannelBook,
		config.ChannelTrades,
		config.ChannelStats,
		config.ChannelAgents,
		config.ChannelStatus,
	)
	defer func() { _ = sub.Close() }()
	log.Printf("bus: subscribed to market and agent channels")

	ch := sub.Channel(redis.WithChannelSize(4096))
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			b.dispatch(msg.Channel, []byte(msg.Payload))
		}
	}
}

func (b *Bus) dispatch(channel string, payload []byte) {
	switch channel {
	case config.ChannelBook:
		b.hub.BroadcastSnapshot(ws.Envelope("book", payload))

	case config.ChannelTrades:
		b.onTrades(payload)

	case config.ChannelStats:
		b.engineStats.Store(json.RawMessage(payload))
		b.hub.BroadcastEvent(ws.Envelope("stats", payload))

	case config.ChannelAgents:
		b.onAgentStats(payload)

	case config.ChannelStatus:
		b.agentStatus.Store(json.RawMessage(payload))
		b.hub.BroadcastEvent(ws.Envelope("agents", payload))
	}
}

func (b *Bus) onTrades(payload []byte) {
	var batch tradeBatch
	if err := json.Unmarshal(payload, &batch); err != nil {
		log.Printf("bus: bad trade batch: %v", err)
		return
	}

	// Everything is persisted; the browser only gets the tail of the tape.
	for _, t := range batch.Trades {
		b.writer.Submit(t)
		b.candles.Ingest(t)
	}

	if len(batch.Trades) > b.cfg.TapeLimit {
		batch.Trades = batch.Trades[len(batch.Trades)-b.cfg.TapeLimit:]
	}
	trimmed, err := json.Marshal(batch)
	if err != nil {
		return
	}
	b.hub.BroadcastEvent(ws.Envelope("trades", trimmed))
}

func (b *Bus) onAgentStats(payload []byte) {
	var msg struct {
		WorkerID   int   `json:"worker_id"`
		SentPerSec int64 `json:"sent_per_sec"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}
	b.mu.Lock()
	b.workerRates[msg.WorkerID] = workerRate{perSec: msg.SentPerSec, seenAt: time.Now()}
	b.mu.Unlock()
}

// AgentOrdersPerSec is the summed send rate reported by all agent workers.
func (b *Bus) AgentOrdersPerSec() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total int64
	cutoff := time.Now().Add(-workerRateTTL)
	for id, v := range b.workerRates {
		if v.seenAt.Before(cutoff) {
			delete(b.workerRates, id)
			continue
		}
		total += v.perSec
	}
	return total
}

func (b *Bus) EngineStats() json.RawMessage {
	if v, ok := b.engineStats.Load().(json.RawMessage); ok {
		return v
	}
	return json.RawMessage("null")
}

func (b *Bus) AgentStatus() json.RawMessage {
	if v, ok := b.agentStatus.Load().(json.RawMessage); ok {
		return v
	}
	return json.RawMessage("null")
}

// Publish sends a control command to the engine and the agent supervisor.
func (b *Bus) Publish(ctx context.Context, channel string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.rdb.Publish(ctx, channel, payload).Err()
}

func (b *Bus) SetState(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.rdb.Set(ctx, config.KeySimState, payload, 0).Err()
}

func (b *Bus) GetState(ctx context.Context, out any) error {
	raw, err := b.rdb.Get(ctx, config.KeySimState).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (b *Bus) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return b.rdb.Ping(ctx).Err()
}
