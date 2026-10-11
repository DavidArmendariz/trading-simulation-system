// Command gateway is the API gateway and message router: one Redis subscriber
// feeding a WebSocket hub and the PostgreSQL writers, plus the REST control
// plane the admin panel drives.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dealmaker/market-sim/gateway/internal/api"
	"github.com/dealmaker/market-sim/gateway/internal/bus"
	"github.com/dealmaker/market-sim/gateway/internal/config"
	"github.com/dealmaker/market-sim/gateway/internal/store"
	"github.com/dealmaker/market-sim/gateway/internal/ws"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx, cfg.MigrationsDir); err != nil {
		log.Fatalf("gateway: migrations: %v", err)
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("gateway: bad REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()

	hub := ws.NewHub()
	go hub.Run()

	writer := store.NewTradeWriter(db)
	go writer.Run(ctx)

	candles := store.NewCandleAggregator(db)
	go candles.Run(ctx)

	messageBus := bus.New(cfg, rdb, hub, writer, candles)
	go messageBus.Run(ctx)

	controller := api.NewController(cfg, messageBus, hub, writer, candles)
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewRouter(controller, hub),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("gateway: listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("gateway: server error: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Printf("gateway: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("gateway: shutdown: %v", err)
	}
	os.Exit(0)
}
