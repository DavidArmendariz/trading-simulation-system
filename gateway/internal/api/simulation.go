package api

import (
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/dealmaker/market-sim/gateway/internal/bus"
	"github.com/dealmaker/market-sim/gateway/internal/config"
	"github.com/dealmaker/market-sim/gateway/internal/store"
	"github.com/dealmaker/market-sim/gateway/internal/ws"
)

// SimState is the operator's intent. It is mirrored into Redis so a restarted
// agent supervisor or a freshly loaded dashboard reads the truth rather than
// guessing.
type SimState struct {
	Running     bool `json:"running"`
	AgentCount  int  `json:"agent_count"`
	TargetTPS   int  `json:"target_tps"`
	UpdatedAtMs int64 `json:"updated_at_ms"`
}

type Controller struct {
	cfg     config.Config
	bus     *bus.Bus
	hub     *ws.Hub
	writer  *store.TradeWriter
	candles *store.CandleAggregator

	mu    sync.RWMutex
	state SimState
}

func NewController(cfg config.Config, b *bus.Bus, hub *ws.Hub, writer *store.TradeWriter,
	candles *store.CandleAggregator) *Controller {
	c := &Controller{
		cfg:     cfg,
		bus:     b,
		hub:     hub,
		writer:  writer,
		candles: candles,
		state: SimState{
			Running:    false,
			AgentCount: cfg.DefaultAgentCount,
			TargetTPS:  cfg.DefaultTargetTPS,
		},
	}
	// Adopt any state left by a previous run.
	var previous SimState
	if err := b.GetState(ctxBackground(), &previous); err == nil && previous.AgentCount > 0 {
		c.state = previous
	}
	return c
}

type configRequest struct {
	AgentCount *int `json:"agent_count"`
	TargetTPS  *int `json:"target_tps"`
}

const (
	maxAgents = 20_000
	maxTPS    = 200_000
)

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// apply merges a partial config request into the current state.
func (c *Controller) apply(req configRequest) SimState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.AgentCount != nil {
		c.state.AgentCount = clamp(*req.AgentCount, 1, maxAgents)
	}
	if req.TargetTPS != nil {
		c.state.TargetTPS = clamp(*req.TargetTPS, 1, maxTPS)
	}
	c.state.UpdatedAtMs = nowMs()
	return c.state
}

func (c *Controller) setRunning(running bool) SimState {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Running = running
	c.state.UpdatedAtMs = nowMs()
	return c.state
}

func (c *Controller) snapshot() SimState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *Controller) broadcast(ctx *gin.Context, cmd string, state SimState) error {
	payload := gin.H{
		"cmd": cmd,
		"payload": gin.H{
			"agent_count": state.AgentCount,
			"target_tps":  state.TargetTPS,
		},
	}
	if err := c.bus.Publish(ctx, config.ChannelControl, payload); err != nil {
		return err
	}
	return c.bus.SetState(ctx, state)
}

func (c *Controller) Start(ctx *gin.Context) {
	var req configRequest
	_ = ctx.ShouldBindJSON(&req) // body is optional
	c.apply(req)
	state := c.setRunning(true)

	if err := c.broadcast(ctx, "START", state); err != nil {
		log.Printf("api: start failed: %v", err)
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "starting", "state": state})
}

func (c *Controller) Stop(ctx *gin.Context) {
	state := c.setRunning(false)
	if err := c.broadcast(ctx, "STOP", state); err != nil {
		log.Printf("api: stop failed: %v", err)
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "stopping", "state": state})
}

func (c *Controller) UpdateConfig(ctx *gin.Context) {
	var req configRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "expected agent_count and/or target_tps"})
		return
	}
	if req.AgentCount == nil && req.TargetTPS == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "no configurable field supplied"})
		return
	}
	state := c.apply(req)
	if err := c.broadcast(ctx, "CONFIG_UPDATE", state); err != nil {
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "updated", "state": state})
}

// Reset clears the order book without touching the agent pool.
func (c *Controller) Reset(ctx *gin.Context) {
	var req struct {
		InitialPriceTicks *uint64 `json:"initial_price_ticks"`
	}
	_ = ctx.ShouldBindJSON(&req)
	payload := gin.H{"cmd": "RESET", "payload": gin.H{}}
	if req.InitialPriceTicks != nil {
		payload["payload"] = gin.H{"initial_price_ticks": *req.InitialPriceTicks}
	}
	if err := c.bus.Publish(ctx, config.ChannelControl, payload); err != nil {
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "reset"})
}

func (c *Controller) Status(ctx *gin.Context) {
	state := c.snapshot()
	ctx.JSON(http.StatusOK, gin.H{
		"symbol":               c.cfg.Symbol,
		"state":                state,
		"engine":               c.bus.EngineStats(),
		"agents":               c.bus.AgentStatus(),
		"agent_orders_per_sec": c.bus.AgentOrdersPerSec(),
		"ws_clients":           c.hub.Clients(),
		"trades_persisted":     c.writer.Written.Load(),
		"trades_dropped":       c.writer.Dropped.Load(),
	})
}
