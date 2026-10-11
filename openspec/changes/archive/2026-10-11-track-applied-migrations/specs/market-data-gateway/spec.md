## MODIFIED Requirements

### Requirement: Schema migrations on boot
The gateway SHALL apply SQL files from `db/migrations/` in lexical order at startup before serving traffic. It SHALL record each applied file in a `schema_migrations` table and SHALL NOT re-apply a recorded file. Each file and its record row SHALL commit in one transaction.

#### Scenario: Fresh database
- **WHEN** the gateway boots against an empty database
- **THEN** all migrations are applied, recorded in `schema_migrations`, and the tables exist

#### Scenario: Restart
- **WHEN** the gateway boots and every migration is already recorded
- **THEN** no migration SQL runs

#### Scenario: Failed migration
- **WHEN** a migration file fails partway
- **THEN** its changes roll back, it is not recorded, and the gateway exits with an error
