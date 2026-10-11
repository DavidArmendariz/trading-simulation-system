SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: up down logs rebuild ps test-engine psql redis-cli clean stats

up:
	$(COMPOSE) up --build -d
	@echo "UI      -> http://localhost:$${UI_HOST_PORT:-13000}"
	@echo "Gateway -> http://localhost:$${GATEWAY_HOST_PORT:-18080}"

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

rebuild:
	$(COMPOSE) build --no-cache

test-engine:
	cd engine && cargo test

psql:
	docker exec -it sim-postgres psql -U $${POSTGRES_USER:-sim} -d $${POSTGRES_DB:-marketsim}

redis-cli:
	docker exec -it sim-redis redis-cli

stats:
	docker exec -it sim-redis redis-cli SUBSCRIBE market:stats

start:
	curl -sS -X POST localhost:$${GATEWAY_HOST_PORT:-18080}/api/v1/simulation/start | jq .

stop:
	curl -sS -X POST localhost:$${GATEWAY_HOST_PORT:-18080}/api/v1/simulation/stop | jq .

status:
	curl -sS localhost:$${GATEWAY_HOST_PORT:-18080}/api/v1/simulation/status | jq .
