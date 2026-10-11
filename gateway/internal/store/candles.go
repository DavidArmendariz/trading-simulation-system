package store

import (
	"context"
	"log"
	"sync"
	"time"
)

const CandleInterval = "1s"

type Candle struct {
	Time   int64  `json:"time"` // unix seconds, what lightweight-charts wants
	Open   uint64 `json:"open"`
	High   uint64 `json:"high"`
	Low    uint64 `json:"low"`
	Close  uint64 `json:"close"`
	Volume uint64 `json:"volume"`
	Trades int64  `json:"trades"`
}

type bucket struct {
	open, high, low, close uint64
	// volume and trades are deltas since the last flush; the upsert adds them
	// to whatever is already stored, so a partial bucket can be written
	// repeatedly as it fills without double counting.
	volume uint64
	trades int64
	dirty  bool
}

// CandleAggregator folds the trade tape into 1-second OHLCV bars in memory and
// upserts them, so the chart has history after a page reload.
type CandleAggregator struct {
	db      *DB
	mu      sync.Mutex
	buckets map[int64]*bucket
}

func NewCandleAggregator(db *DB) *CandleAggregator {
	return &CandleAggregator{db: db, buckets: make(map[int64]*bucket)}
}

func (a *CandleAggregator) Ingest(t Trade) {
	sec := t.Time().Unix()
	a.mu.Lock()
	defer a.mu.Unlock()

	b, ok := a.buckets[sec]
	if !ok {
		b = &bucket{open: t.Price, high: t.Price, low: t.Price}
		a.buckets[sec] = b
	}
	if t.Price > b.high {
		b.high = t.Price
	}
	if t.Price < b.low {
		b.low = t.Price
	}
	b.close = t.Price
	b.volume += t.Qty
	b.trades++
	b.dirty = true
}

func (a *CandleAggregator) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.flush(context.Background())
			return
		case <-ticker.C:
			a.flush(ctx)
		}
	}
}

const upsertCandle = `
INSERT INTO candles (bucket, interval, open, high, low, close, volume, trades)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (bucket, interval) DO UPDATE SET
  high   = GREATEST(candles.high, EXCLUDED.high),
  low    = LEAST(candles.low, EXCLUDED.low),
  close  = EXCLUDED.close,
  volume = candles.volume + EXCLUDED.volume,
  trades = candles.trades + EXCLUDED.trades`

func (a *CandleAggregator) flush(ctx context.Context) {
	type pending struct {
		sec int64
		b   bucket
	}
	var out []pending
	cutoff := time.Now().Unix()

	a.mu.Lock()
	for sec, b := range a.buckets {
		if !b.dirty {
			if sec < cutoff {
				delete(a.buckets, sec)
			}
			continue
		}
		out = append(out, pending{sec: sec, b: *b})
		// Deltas are consumed by this flush; OHLC stays for merge semantics.
		b.volume, b.trades, b.dirty = 0, 0, false
		if sec < cutoff {
			delete(a.buckets, sec)
		}
	}
	a.mu.Unlock()

	for _, p := range out {
		ts := time.Unix(p.sec, 0).UTC()
		_, err := a.db.Pool.Exec(ctx, upsertCandle, ts, CandleInterval,
			int64(p.b.open), int64(p.b.high), int64(p.b.low), int64(p.b.close),
			int64(p.b.volume), p.b.trades)
		if err != nil {
			log.Printf("store: candle upsert failed for %s: %v", ts, err)
		}
	}
}

// RecentCandles returns the newest `limit` bars in ascending time order.
func (a *CandleAggregator) RecentCandles(ctx context.Context, limit int) ([]Candle, error) {
	const q = `
SELECT EXTRACT(EPOCH FROM bucket)::bigint, open, high, low, close, volume, trades
FROM (
  SELECT * FROM candles WHERE interval = $1 ORDER BY bucket DESC LIMIT $2
) recent
ORDER BY bucket ASC`

	rows, err := a.db.Pool.Query(ctx, q, CandleInterval, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Candle, 0, limit)
	for rows.Next() {
		var c Candle
		var open, high, low, cl, vol int64
		if err := rows.Scan(&c.Time, &open, &high, &low, &cl, &vol, &c.Trades); err != nil {
			return nil, err
		}
		c.Open, c.High, c.Low, c.Close, c.Volume =
			uint64(open), uint64(high), uint64(low), uint64(cl), uint64(vol)
		out = append(out, c)
	}
	return out, rows.Err()
}
