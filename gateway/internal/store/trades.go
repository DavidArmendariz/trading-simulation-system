package store

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type Trade struct {
	ID      uint64 `json:"id"`
	Price   uint64 `json:"price"`
	Qty     uint64 `json:"qty"`
	Side    uint8  `json:"side"`
	TsNanos uint64 `json:"ts_nanos"`
}

func (t Trade) Time() time.Time {
	if t.TsNanos == 0 {
		return time.Now().UTC()
	}
	return time.Unix(0, int64(t.TsNanos)).UTC()
}

// TradeWriter batches inserts. Writing one row per print would put database
// latency in the path of the market data fan-out; CopyFrom every 250ms or
// 1,000 rows keeps the pool busy without ever blocking the Redis subscriber.
type TradeWriter struct {
	db      *DB
	in      chan Trade
	Written atomic.Uint64
	Dropped atomic.Uint64
}

func NewTradeWriter(db *DB) *TradeWriter {
	return &TradeWriter{db: db, in: make(chan Trade, 200_000)}
}

// Submit never blocks: a stalled database must not stall the live feed.
func (w *TradeWriter) Submit(t Trade) {
	select {
	case w.in <- t:
	default:
		w.Dropped.Add(1)
	}
}

func (w *TradeWriter) Run(ctx context.Context) {
	const maxBatch = 1000
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]Trade, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		rows := make([][]any, len(batch))
		for i, t := range batch {
			rows[i] = []any{int64(t.ID), int64(t.Price), int64(t.Qty), int16(t.Side), t.Time()}
		}
		n, err := w.db.Pool.CopyFrom(ctx,
			pgx.Identifier{"trades"},
			[]string{"trade_id", "price_ticks", "qty", "taker_side", "ts"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			log.Printf("store: trade flush failed (%d rows): %v", len(batch), err)
		} else {
			w.Written.Add(uint64(n))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case t := <-w.in:
			batch = append(batch, t)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
