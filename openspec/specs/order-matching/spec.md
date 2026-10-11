# order-matching Specification

## Purpose
Rust matching engine (`engine/`): a continuous double auction for one symbol. Receives orders over ZeroMQ IPC, matches with price-time priority, and publishes trades, L2 depth, and stats to Redis.

## Requirements

### Requirement: Price-time priority matching
The engine SHALL match an incoming order against the best opposite price first, and within a price level against the earliest resting order first. Trades SHALL execute at the resting order's price.

#### Scenario: FIFO within a level
- **WHEN** two bids rest at the same price and a sell arrives for less than their combined quantity
- **THEN** the earlier bid fills completely before the later bid fills at all

#### Scenario: Better price wins over earlier time
- **WHEN** a bid at a higher price arrives after a bid at a lower price
- **THEN** an incoming sell fills against the higher bid first

#### Scenario: Book never stays crossed
- **WHEN** any order finishes processing
- **THEN** the best bid is strictly below the best ask

### Requirement: Order types
Limit orders SHALL rest any unfilled remainder. Market orders SHALL be immediate-or-cancel and never rest.

#### Scenario: Partial limit fill
- **WHEN** a limit order fills partially
- **THEN** the remainder rests at its limit price

#### Scenario: Market order remainder
- **WHEN** a market order exhausts the opposite side
- **THEN** the unfilled quantity is dropped

### Requirement: Integer prices and quantities
Prices SHALL be `u64` ticks and quantities `u64` shares. No floating-point value SHALL represent a price or quantity in the engine or on the wire.

#### Scenario: Wire decode
- **WHEN** a MessagePack order arrives
- **THEN** price and quantity decode into integer fields

### Requirement: Bounded book size
The engine SHALL cap resting orders at `MAX_RESTING` (0 = unlimited) and expire the oldest resting quotes once the cap is exceeded. Filled or cancelled orders SHALL NOT count against the cap.

#### Scenario: Cap reached
- **WHEN** a new order rests and the book exceeds `MAX_RESTING`
- **THEN** the oldest resting order is removed and the expired counter increments

### Requirement: Coalesced market data
The engine SHALL publish a top-`BOOK_DEPTH` L2 snapshot at `SNAPSHOT_HZ` and SHALL batch every trade printed since the previous tick. Every message SHALL carry a monotonic `seq`. Building a snapshot SHALL cost O(depth), not O(orders).

#### Scenario: Snapshot tick
- **WHEN** a snapshot tick fires
- **THEN** one book message and one batch of all trades since the last tick are published

### Requirement: Backpressure without stalling or losing trades
The engine SHALL NOT block the ZeroMQ intake. When the inbound channel is full, orders SHALL be dropped and counted. When the publisher channel is full, the stale snapshot SHALL be dropped and the trades SHALL be kept.

#### Scenario: Publisher saturated
- **WHEN** the publisher channel is full at a snapshot tick
- **THEN** trades are still delivered and only the snapshot is skipped

### Requirement: Restart safety
The engine SHALL remove a stale IPC socket file on boot so it can restart alone and agents reconnect.

#### Scenario: Engine restarted alone
- **WHEN** `docker compose restart engine` runs while agents are up
- **THEN** agents reconnect and orders decode without errors
