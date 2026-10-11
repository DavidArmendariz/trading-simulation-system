## 1. Implementation
- [x] 1.1 Create `schema_migrations` table in `Migrate` before applying files
- [x] 1.2 Skip files already recorded; apply each new file and insert its row in one transaction
- [x] 1.3 Update CLAUDE.md gotcha and decision log

## 2. Verification
- [x] 2.1 Gateway builds
- [x] 2.2 Boot against existing volume: 001-003 recorded once, second boot applies nothing
