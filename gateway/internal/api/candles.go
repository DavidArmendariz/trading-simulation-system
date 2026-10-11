package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func ctxBackground() context.Context { return context.Background() }

func nowMs() int64 { return time.Now().UnixMilli() }

// Candles backfills the chart on page load; live bars then arrive over the
// WebSocket.
func (c *Controller) Candles(ctx *gin.Context) {
	limit := 500
	if raw := ctx.Query("limit"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			limit = clamp(v, 1, 5000)
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx.Request.Context(), 5*time.Second)
	defer cancel()

	rows, err := c.candles.RecentCandles(reqCtx, limit)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"symbol":   c.cfg.Symbol,
		"interval": "1s",
		"candles":  rows,
	})
}
