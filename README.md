# app-sync

Two-way sync server for ThingCue, built on [go-oversync](https://github.com/mobiletoly/go-oversync).

## Architecture

```
Android App (KMP) → oversqlite → app-sync (Go) → Supabase PostgreSQL
```

## Quick Start

```bash
# Build
go build -o app-sync .

# Configure
export DATABASE_URL="postgres://..."
export JWT_SECRET="your-secret"

# Run
./app-sync
```

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `JWT_SECRET` | required | HMAC-SHA256 key for JWT tokens |
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `DB_MAX_CONNS` | `25` | Max database connections |
| `DB_MIN_CONNS` | `5` | Min database connections |

## Endpoints

### Health (no auth)
- `GET /health` — Server health
- `GET /syncx/health` — Sync service health
- `GET /syncx/status` — Sync service status

### Sync (JWT required)
- `POST /sync/connect`
- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`
- `DELETE /sync/push-sessions/{push_id}`
- `GET /sync/committed-bundles/{bundle_seq}/rows`
- `GET /sync/pull`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/capabilities`

## Deployment

See deployment guide at `docs/deploy.md`.
