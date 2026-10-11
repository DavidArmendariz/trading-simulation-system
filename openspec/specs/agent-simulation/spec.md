# agent-simulation Specification

## Purpose
Python agent pool (`agents/`): simulated traders that stream orders to the engine at a configurable target rate, controlled from the gateway over Redis.

## Requirements

### Requirement: Sharded worker processes
The supervisor SHALL shard `AGENT_COUNT` agents and `TARGET_TPS` across `AGENT_WORKERS` processes started with the `spawn` method. Each worker SHALL own one ZeroMQ PUSH socket and one Redis subscription feeding shared market state.

#### Scenario: Pool start
- **WHEN** a START command arrives on `simulation:control`
- **THEN** the supervisor starts `AGENT_WORKERS` processes, each running its share of agents

### Requirement: Strategy mix
The pool SHALL run 70% noise, 20% momentum, and 10% mean-reversion agents.

#### Scenario: Mix at 2,000 agents
- **WHEN** the pool starts with 2,000 agents
- **THEN** about 1,400 are noise, 400 momentum, and 200 mean reversion

### Requirement: Closed-loop rate control
Each worker SHALL measure its realized order rate every second and scale agent wake rates so the pool delivers the target TPS, backing off when its send queue builds.

#### Scenario: Strategies decline to act
- **WHEN** momentum and mean-reversion agents skip most wake-ups
- **THEN** the realized pool rate still converges to the target TPS

### Requirement: Config changes restart the pool
A CONFIG_UPDATE while running SHALL stop and restart the pool with the new settings instead of resharding live.

#### Scenario: Change agent count while running
- **WHEN** CONFIG_UPDATE arrives with a new agent count
- **THEN** the pool restarts with that count

### Requirement: Rate reporting
Workers SHALL publish their realized rates to `agents:stats`, and pool lifecycle to `agents:status`.

#### Scenario: Worker report
- **WHEN** one second elapses on a running worker
- **THEN** the worker publishes its realized rate
