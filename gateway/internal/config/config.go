package config

import (
	"os"
	"strconv"
)

type Config struct {
	ListenAddr        string
	RedisURL          string
	DatabaseURL       string
	MigrationsDir     string
	Symbol            string
	DefaultAgentCount int
	DefaultTargetTPS  int
	// Trades forwarded to each browser per batch. The full batch is always
	// persisted; the UI tape only needs the most recent prints.
	TapeLimit int
}

func str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func num(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func Load() Config {
	return Config{
		ListenAddr:        str("LISTEN_ADDR", ":8080"),
		RedisURL:          str("REDIS_URL", "redis://127.0.0.1:6379"),
		DatabaseURL:       str("DATABASE_URL", "postgres://sim:simpass@127.0.0.1:5432/marketsim?sslmode=disable"),
		MigrationsDir:     str("MIGRATIONS_DIR", "/app/migrations"),
		Symbol:            str("SIM_SYMBOL", "SIMX"),
		DefaultAgentCount: num("DEFAULT_AGENT_COUNT", 2000),
		DefaultTargetTPS:  num("DEFAULT_TARGET_TPS", 5000),
		TapeLimit:         num("TAPE_LIMIT", 50),
	}
}

const (
	ChannelBook    = "market:book"
	ChannelTrades  = "market:trades"
	ChannelStats   = "market:stats"
	ChannelControl = "simulation:control"
	ChannelAgents  = "agents:stats"
	ChannelStatus  = "agents:status"
	KeySimState    = "sim:state"
)
