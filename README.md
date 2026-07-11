# Provio Dark-Social Ingestion Engine (DSIE)

Provio DSIE collects job posts from dark-social communities, filters them for India-only fresher/entry-level technology roles, and moves validated jobs into PostgreSQL through a queue-based ingestion pipeline.

The goal is to scale job ingestion beyond traditional scrapers by listening to Telegram, WhatsApp, and Discord communities where HRs, founders, and community managers post unlisted opportunities.

## Current Status

DSIE now has the full MVP pipeline shape:

```txt
Telegram / WhatsApp / Discord
-> Redis raw queue
-> rules-first Stage 3 extraction worker
-> OpenRouter free-model fallback for uncertain messages
-> deterministic policy validation
-> Redis clean staging queue
-> Go ingestion worker
-> PostgreSQL social_jobs
```

The Stage 3 worker is no longer a placeholder. It now performs deterministic extraction first and uses free AI only as a fallback for uncertain posts.

## Architecture

### Stage 1: Social Listeners

Listener workers capture raw messages and push them into Redis. They do not perform heavy processing.

| Source | Worker | Runtime | Queue output |
| --- | --- | --- | --- |
| Telegram | `externaltracker-python/workers/tg_worker.py` | Python / Telethon | `provio_raw_messages_queue` |
| WhatsApp | `externaltracker-python/workers/wa_worker.js` | Node.js / Baileys | `provio_raw_messages_queue` |
| Discord | `externaltracker-python/workers/discord_worker.js` + `discord_shard.js` | Node.js / discord.js | `provio_raw_messages_queue` |

### Stage 2: Redis Buffer

Redis decouples noisy chat traffic from slower AI/database processing.

| Queue | Purpose |
| --- | --- |
| `provio_raw_messages_queue` | Raw messages from Telegram, WhatsApp, and Discord |
| `provio_clean_jobs_staging` | Validated jobs ready for Go ingestion |
| `provio_jobs_review` | Uncertain or AI-failed messages for manual review |
| `provio_jobs_dead_letter` | Malformed messages or unrecoverable parsing failures |

### Stage 3: Rules-First Extraction Worker

Implemented in:

```txt
externaltracker-python/workers/openclaw_worker.py
```

This worker replaces the earlier OpenClaw placeholder with a bounded extraction engine:

- Consumes raw messages from `provio_raw_messages_queue`.
- Applies regex/rule checks first.
- Extracts URLs, emails, phones, experience ranges, location signals, seniority signals, and tech-track signals.
- Accepts clear valid jobs without AI.
- Rejects clear invalid jobs without AI.
- Calls OpenRouter only for uncertain messages.
- Allows only free OpenRouter models: `openrouter/free` or model IDs ending in `:free`.
- Applies Redis-backed OpenRouter daily and per-minute limits.
- Validates final policy in Python, not in the model.
- Signs clean job envelopes with HMAC when `PROVIO_INGEST_HMAC_SECRET` is configured.
- Routes uncertain/failing cases to `provio_jobs_review`.

Final acceptance policy:

```txt
country_code == IN
seniority == fresher_intern
tech_track in software, ai, data_science
application URL/email/phone exists
no blocking risk flags
```

### Stage 4: Go Ingestion Funnel

Implemented in:

```txt
externaltracker-python/ingest/go_ingest.go
```

The Go worker consumes clean jobs from `provio_clean_jobs_staging`, verifies signed envelopes when required, and inserts into PostgreSQL using `ON CONFLICT (job_source_url) DO NOTHING`.

## Repository Layout

```txt
.
|-- README.md
`-- externaltracker-python/
    |-- ingest/
    |   |-- go.mod
    |   |-- go.sum
    |   `-- go_ingest.go
    |-- workers/
    |   |-- tg_worker.py
    |   |-- wa_worker.js
    |   |-- discord_worker.js
    |   |-- discord_shard.js
    |   `-- openclaw_worker.py
    |-- package.json
    |-- package-lock.json
    `-- requirements.txt
```

Ignored local files include `.env`, `node_modules/`, `wa_session/`, Telegram session files, Python caches, virtual environments, and local Codex dependency folders.

## Prerequisites

- Python 3.11+
- Node.js 18+
- Go 1.22+
- Redis
- PostgreSQL
- Optional: Docker for local Redis
- OpenRouter account and API key if AI fallback is enabled

## Local Setup

Run commands from:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
```

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Install Node dependencies:

```powershell
npm install
```

Prepare Go dependencies:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python\ingest"
go mod tidy
go test -buildvcs=false .
```

## Environment Variables

Create `externaltracker-python/.env`. Do not commit it.

```env
# Redis
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379/0

PROVIO_RAW_MESSAGES_QUEUE=provio_raw_messages_queue
PROVIO_CLEAN_JOBS_QUEUE=provio_clean_jobs_staging
PROVIO_REVIEW_QUEUE=provio_jobs_review
PROVIO_DEAD_LETTER_QUEUE=provio_jobs_dead_letter

# Telegram
TG_API_ID=
TG_API_HASH=
TG_SESSION_NAME=provio_tg_session
TG_PHONE_NUMBER=
TG_TARGET_CHANNELS=@india_tech_jobs,@fresher_openings_india,@dev_jobs_hub
TG_PUBLIC_PREVIEW_CHANNELS=india_tech_jobs,fresher_openings_india,dev_jobs_hub
TG_WORKER_MODE=telethon
PUBLIC_PREVIEW_POLL_SECONDS=60

# Discord
DISCORD_BOT_TOKEN=
DISCORD_TARGET_CHANNEL_IDS=

# WhatsApp
WHATSAPP_SESSION_PATH=./wa_session

# Stage 3 extraction
AI_PROVIDER=openrouter
OPENROUTER_API_KEY=
OPENROUTER_MODEL=openrouter/free
OPENROUTER_DAILY_LIMIT=300
OPENROUTER_PER_MINUTE_LIMIT=10
AI_ONLY_FOR_UNCERTAIN=true
AI_MAX_TOKENS=500
OPENROUTER_TIMEOUT_SECONDS=20

# Go ingestion / PostgreSQL
DATABASE_URL=postgres://postgres:<password>@127.0.0.1:5432/tracker_db
PROVIO_INGEST_HMAC_SECRET=change_this_secret
SIGN_CLEAN_JOBS=true
ALLOW_UNSIGNED_LOCAL_JOBS=true
GO_INGEST_BATCH_SIZE=1
```

Recommended production changes:

```env
ALLOW_UNSIGNED_LOCAL_JOBS=false
GO_INGEST_BATCH_SIZE=50
SIGN_CLEAN_JOBS=true
```

## Running the Pipeline

Start Redis:

```powershell
docker start provio-redis
docker exec provio-redis redis-cli ping
```

Expected:

```txt
PONG
```

Start the Stage 3 extraction worker:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
python workers\openclaw_worker.py
```

Run listeners as needed:

```powershell
python workers\tg_worker.py
node workers\discord_worker.js
node workers\wa_worker.js
```

Run Go ingestion:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python\ingest"
go run -buildvcs=false go_ingest.go
```

## Manual Stage 3 Test

Push a raw message into Redis:

```powershell
docker exec provio-redis redis-cli RPUSH provio_raw_messages_queue "{`"source`":`"manual_test`",`"channel`":`"local`",`"message_id`":`"test-001`",`"raw_text`":`"Hiring Software Engineer Intern at Provio. Freshers 0-1 years. Location: Remote India. Apply: https://example.com/apply`",`"timestamp`":`"2026-07-10T10:00:00Z`"}"
```

Expected worker output:

```txt
[OPENCLAW-MATCH] Software Engineer Intern @ Provio track=software method=rules
```

Check clean queue:

```powershell
docker exec provio-redis redis-cli LLEN provio_clean_jobs_staging
docker exec provio-redis redis-cli LINDEX provio_clean_jobs_staging 0
```

Check review queue:

```powershell
docker exec provio-redis redis-cli LLEN provio_jobs_review
```

Check dead-letter queue:

```powershell
docker exec provio-redis redis-cli LLEN provio_jobs_dead_letter
```

## PostgreSQL Table

The current local target table is:

```sql
social_jobs (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    company_slug VARCHAR(255),
    job_source_url VARCHAR(1000) NOT NULL UNIQUE,
    raw_description TEXT,
    raw_location_text VARCHAR(500),
    posted_timestamp TIMESTAMPTZ,
    tech_track VARCHAR(100),
    source VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

Useful verification query:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d tracker_db -c "SELECT id, title, company_slug, job_source_url, tech_track, source FROM social_jobs ORDER BY id DESC LIMIT 10;"
```

## Verified So Far

- Python syntax check passes for `openclaw_worker.py`.
- Rules path accepts a clear India fresher software job.
- Rules path rejects a senior non-India job.
- In-memory queue test pushes a signed envelope to `provio_clean_jobs_staging`.
- OpenRouter API is reachable with the configured key.
- `openrouter/free` can return inconsistent JSON or loose enum labels, so the worker includes fallback parsing, alias normalization, and review routing.
- Go ingestion has previously inserted clean manual jobs into local PostgreSQL.

## Production Readiness

Production-friendly pieces already present:

- Queue-based isolation between listeners, extraction, and database ingestion.
- Rules-first extraction to reduce AI usage and cost.
- Free-model enforcement for OpenRouter.
- Redis-backed OpenRouter request limits.
- Deterministic final policy gate in Python.
- HMAC signing between extraction and Go ingestion.
- Review and dead-letter queues.
- Duplicate URL protection through PostgreSQL conflict handling.

Remaining work before production:

- Add SQL migration files for `social_jobs`.
- Add unit tests for extraction rules, OpenRouter validation, HMAC envelopes, and Go parsing.
- Run full live listener-to-DB tests for Telegram, WhatsApp, and Discord.
- Add dedupe beyond exact `job_source_url` conflicts.
- Add scam/domain reputation checks.
- Add structured logs, metrics, queue-depth monitoring, and alerts.
- Add retry strategy for transient AI/database failures.
- Add graceful shutdown handling.
- Build a reviewer UI or dashboard for `provio_jobs_review`.
- Choose a specific reliable OpenRouter `:free` model if `openrouter/free` proves too variable.
- Move production secrets to a secret manager and rotate any exposed local tokens.

## Security Notes

- Never commit `.env`, session files, QR/auth state, database passwords, or API keys.
- Treat every social message as hostile input.
- Do not give AI models database, shell, browser, or file-system tools.
- Keep the model as a parser only; final policy decisions must remain in code.
- Use `ALLOW_UNSIGNED_LOCAL_JOBS=false` in production.
- Rotate credentials if they were pasted into chat, screenshots, logs, or commits.

## Useful Commands

Show queue lengths:

```powershell
docker exec provio-redis redis-cli LLEN provio_raw_messages_queue
docker exec provio-redis redis-cli LLEN provio_clean_jobs_staging
docker exec provio-redis redis-cli LLEN provio_jobs_review
docker exec provio-redis redis-cli LLEN provio_jobs_dead_letter
```

Clear local test queues only:

```powershell
docker exec provio-redis redis-cli DEL provio_raw_messages_queue
docker exec provio-redis redis-cli DEL provio_clean_jobs_staging
docker exec provio-redis redis-cli DEL provio_jobs_review
docker exec provio-redis redis-cli DEL provio_jobs_dead_letter
```

Run Python syntax check:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
python -m py_compile workers\openclaw_worker.py workers\tg_worker.py
```

Run Go build/test:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python\ingest"
go test -buildvcs=false .
```
