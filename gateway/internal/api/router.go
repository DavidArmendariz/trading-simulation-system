package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/dealmaker/market-sim/gateway/internal/ws"
)

// cors keeps the dashboard on port 13000 able to call the gateway on 18080.
func cors() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Header("Access-Control-Allow-Origin", "*")
		ctx.Header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		ctx.Header("Access-Control-Allow-Headers", "Content-Type")
		if ctx.Request.Method == http.MethodOptions {
			ctx.AbortWithStatus(http.StatusNoContent)
			return
		}
		ctx.Next()
	}
}

func NewRouter(c *Controller, hub *ws.Hub) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), cors())

	r.GET("/healthz", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/ws", func(ctx *gin.Context) {
		hub.Handle(ctx.Writer, ctx.Request)
	})

	v1 := r.Group("/api/v1")
	{
		sim := v1.Group("/simulation")
		sim.POST("/start", c.Start)
		sim.POST("/stop", c.Stop)
		sim.POST("/config", c.UpdateConfig)
		sim.POST("/reset", c.Reset)
		sim.GET("/status", c.Status)

		v1.GET("/candles", c.Candles)
	}

	return r
}
