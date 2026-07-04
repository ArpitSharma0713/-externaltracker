# Provio Dark-Social Ingestion Engine (DSIE)

This repository contains the current implementation of Provio's Dark-Social Ingestion Engine, a pipeline for collecting job posts from community channels and moving validated jobs into PostgreSQL.

The product goal from the DOC is to scale job ingestion from a few hundred jobs to 20,000+ jobs by listening to Telegram, WhatsApp, and Discord communities, buffering raw messages safely in Redis, filtering them through an OpenClaw-style AI decision layer, and inserting only clean India fresher tech jobs into the Provio database.

## Architecture

The intended system is split into four stages:

1. Stage 1: Social listeners
   Telegram, WhatsApp, and Discord workers capture raw chat messages and push them into Redis.

2. Stage 2: Redis buffer
   Redis decouples noisy chat traffic from slower downstream AI and database processing.

3. Stage 3: OpenClaw AI agent layer
   The AI layer should classify raw messages, enforce business filters, normalize job fields, and push validated jobs into a clean staging queue.

4. Stage 4: Go ingestion funnel
   The Go worker consumes validated jobs from Redis and inserts them into PostgreSQL in controlled batches.

Current queue contract:

```txt
provio_raw_messages_queue       # raw Telegram/WhatsApp/Discord messages
provio_clean_jobs_staging       # AI-approved normalized jobs
```

## Repository Layout

```txt
externaltracker-python/
|-- app/
|-- ingest/
|   |-- go.mod
|   |-- go.sum
|   `-- go_ingest.go
|-- workers/
|   |-- tg_worker.py
|   |-- openclaw_worker.py
|   |-- discord_worker.js
|   |-- discord_shard.js
|   `-- wa_worker.js
|-- package.json
|-- package-lock.json
`-- requirements.txt
```

Sensitive local files are intentionally ignored:

```txt
.env
node_modules/
wa_session/
*.session
__pycache__/
```

## What Is Done

### Project Foundation

The project structure has been created and pushed to GitHub. Python, Node.js, Go, Redis, and PostgreSQL pieces are now represented in the repo.

Completed:

- Base `externaltracker-python` folder structure
- Worker entrypoints for Telegram, Discord, WhatsApp, and OpenClaw
- Go ingestion module under `ingest`
- `.gitignore` covering secrets, local sessions, dependency folders, caches, and build artifacts
- GitHub remote push completed for the initial working code

### Stage 1: Telegram Listener

Implemented in:

```txt
externaltracker-python/workers/tg_worker.py
```

Current capabilities:

- Uses Telethon for Telegram MTProto listening
- Loads settings from `.env`
- Connects to Redis
- Pushes raw payloads into `provio_raw_messages_queue`
- Captures source, channel, message ID, raw text, timestamp, and media URL placeholder
- Supports `TG_TARGET_CHANNELS`
- Includes a public preview fallback mode for public Telegram channels
- Handles first-time Telegram login and saved Telethon sessions

Verified:

- Dependencies installed in the active Python virtual environment
- Telegram login succeeded
- Worker reached `Connected to Telegram`
- Redis connectivity worked

Still pending:

- Confirming at least one live Telegram message enters `provio_raw_messages_queue`
- Production-grade reconnect/backoff handling
- Cleaner operational logs and metrics
- Media extraction beyond URL placeholders

### Stage 1: Discord Listener

Implemented in:

```txt
externaltracker-python/workers/discord_worker.js
externaltracker-python/workers/discord_shard.js
```

Current capabilities:

- Uses `discord.js`
- Uses `ShardingManager`
- Uses `GatewayIntentBits`
- Connects to Redis
- Pushes raw Discord messages into `provio_raw_messages_queue`
- Supports optional `DISCORD_TARGET_CHANNEL_IDS`
- Captures message text, channel, guild, message ID, timestamp, and attachment URLs
- Ignores bot messages

Verified:

- Node dependencies installed
- JavaScript syntax checks passed
- Redis connection failure handling was improved
- Worker now exits cleanly if Redis is unavailable instead of repeating errors endlessly

Still pending:

- Discord bot token must be valid in local `.env`
- Message Content Intent must be enabled in Discord Developer Portal
- Bot must be invited to a test server
- Live message capture into Redis still needs end-to-end verification

### Stage 1: WhatsApp Listener

Implemented in:

```txt
externaltracker-python/workers/wa_worker.js
```

Current capabilities:

- Uses `@whiskeysockets/baileys`
- Shows QR code through `qrcode-terminal`
- Uses multi-file WhatsApp auth state in `wa_session`
- Connects to Redis
- Pushes raw WhatsApp messages into `provio_raw_messages_queue`
- Captures chat JID, message ID, raw text, timestamp, and media URL placeholder
- Ignores messages sent by the connected account
- Filters non-live history sync batches
- Silences noisy Baileys internal logs

Verified:

- WhatsApp QR pairing succeeded
- Worker reached `Connected and listening`
- Redis connectivity worked

Still pending:

- Confirming at least one fresh live WhatsApp message enters `provio_raw_messages_queue`
- More complete extraction for group names, sender IDs, and media
- Session reset documentation for `wa_session`
- Production reconnect/backoff strategy

### Stage 2: Redis Buffer

Redis queue contract is implemented across workers.

Completed:

- Raw listener queue: `provio_raw_messages_queue`
- Clean job queue: `provio_clean_jobs_staging`
- Docker Redis container `provio-redis` was used during development
- `PING` returned `PONG`
- `LLEN` checks worked for both queues

Still pending:

- Redis persistence/backup policy
- Dead-letter queue for malformed messages
- Retry queue for transient AI/database failures
- Monitoring for queue depth and worker lag

### Stage 3: OpenClaw AI Agent Layer

Placeholder file:

```txt
externaltracker-python/workers/openclaw_worker.py
```

Current state:

- The file exists and records the intended Stage 3 boundary.
- The actual AI extraction/filtering loop is not implemented yet.

What the DOC expects here:

- Consume raw messages from `provio_raw_messages_queue`
- Detect whether a post is a real job
- Enforce India-only jobs
- Enforce fresher, intern, trainee, entry-level, or 0-2 years seniority
- Enforce tech tracks: `software`, `ai`, `data_science`
- Normalize jobs into a clean JSON schema
- Optionally sign jobs with HMAC
- Push approved jobs to `provio_clean_jobs_staging`
- Drop everything else

Still pending:

- Full OpenClaw worker implementation
- LLM provider integration
- Structured output validation
- HMAC signing for production mode
- Scam/ad/duplicate filtering
- Unit tests for decision rules

### Stage 4: Go Ingestion Funnel

Implemented in:

```txt
externaltracker-python/ingest/go_ingest.go
```

Current capabilities:

- Uses `github.com/redis/go-redis/v9`
- Uses `github.com/jackc/pgx/v5`
- Loads `../.env` when run from the `ingest` folder
- Connects to Redis
- Connects to PostgreSQL through `DATABASE_URL`
- Blocks on `provio_clean_jobs_staging`
- Parses direct clean job JSON or signed envelopes
- Supports local unsigned jobs with `ALLOW_UNSIGNED_LOCAL_JOBS=true`
- Verifies HMAC signatures when unsigned local jobs are disabled
- Inserts into PostgreSQL table `social_jobs`
- Uses `ON CONFLICT (job_source_url) DO NOTHING`
- Rebuffers failed batches on database failure

Verified:

- `go mod tidy`
- `go test -buildvcs=false .`
- Redis connection
- PostgreSQL connection
- Manual clean job consumed from Redis
- Real row inserted into `social_jobs`
- PostgreSQL query showed inserted rows

Example verified rows:

```txt
Software Engineer Intern | Provio Test Company | software     | manual_test
Data Science Intern      | Provio Social Test  | data_science | manual_clean_test
```

Still pending:

- Production batch size should move from `1` to `50`
- Better transaction logging around inserted vs ignored row count
- Migration file for `social_jobs`
- Integration with the real production jobs schema
- Tests around HMAC, malformed JSON, duplicate URLs, and rebuffering
- Graceful shutdown and signal handling

### PostgreSQL

Completed:

- Local PostgreSQL connectivity verified
- `tracker_db` database used
- `social_jobs` table created or confirmed
- `job_source_url` unique constraint verified
- Manual insert tested
- Go ingestion insert tested

Current table shape:

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

Still pending:

- Add SQL migration files to the repo
- Avoid relying on manually created local DB state
- Add indexes for common query paths
- Decide whether `company_slug` should be a slug or display company name
- Map this staging table into the main production job listings model

## How Much Is Complete

High-level status:

```txt
Project scaffold:                 Done
Redis queue contract:             Done
Telegram worker implementation:   Mostly done, live capture verification pending
Discord worker implementation:    Code done, bot/live capture verification pending
WhatsApp worker implementation:   Mostly done, live capture verification pending
OpenClaw AI worker:               Not implemented yet
Go ingestion skeleton:            Done
PostgreSQL insert path:           Done for local social_jobs table
End-to-end raw-to-clean pipeline: Not done until OpenClaw exists
Production readiness:             Not yet
```

Practical completion estimate:

```txt
Infrastructure and queue plumbing: 70-80%
Listener code:                     60-70%
AI validation layer:               0-10%
Database ingestion:                70-80%
Overall DSIE MVP:                  45-55%
Production DSIE:                   20-30%
```

The project now has working foundations from source listeners through Redis and into PostgreSQL, but the critical missing bridge is Stage 3: the OpenClaw decision engine. Without Stage 3, raw Telegram/WhatsApp/Discord messages do not automatically become validated clean jobs.

## Local Setup

### Python

From the project folder:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
python -m pip install -r requirements.txt
```

Python dependencies:

```txt
redis
telethon
python-dotenv
requests
beautifulsoup4
```

### Node.js

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
npm install
```

Node dependencies include:

```txt
discord.js
redis
dotenv
@whiskeysockets/baileys
qrcode-terminal
```

### Go

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python\ingest"
go mod tidy
go test -buildvcs=false .
```

### Redis

Expected local container name:

```txt
provio-redis
```

Useful checks:

```powershell
docker ps
docker start provio-redis
docker exec provio-redis redis-cli ping
docker exec provio-redis redis-cli LLEN provio_raw_messages_queue
docker exec provio-redis redis-cli LLEN provio_clean_jobs_staging
```

### PostgreSQL

Expected local database:

```txt
tracker_db
```

Useful check:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d tracker_db -c "SELECT id, title, company_slug, job_source_url, tech_track, source FROM social_jobs;"
```

## Required Environment Variables

Create a local `.env` file inside `externaltracker-python`. Do not commit it.

Use placeholders like this:

```env
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379/0

PROVIO_RAW_MESSAGES_QUEUE=provio_raw_messages_queue
PROVIO_CLEAN_JOBS_QUEUE=provio_clean_jobs_staging

TG_API_ID=
TG_API_HASH=
TG_SESSION_NAME=provio_tg_session
TG_PHONE_NUMBER=
TG_TARGET_CHANNELS=@india_tech_jobs,@fresher_openings_india,@dev_jobs_hub,@offcampusjobupdateslive
TG_PUBLIC_PREVIEW_CHANNELS=india_tech_jobs,fresher_openings_india,dev_jobs_hub,offcampusjobupdateslive
TG_WORKER_MODE=telethon
PUBLIC_PREVIEW_POLL_SECONDS=60

DISCORD_BOT_TOKEN=
DISCORD_TARGET_CHANNEL_IDS=

WHATSAPP_SESSION_PATH=./wa_session

DATABASE_URL=postgres://postgres:<password>@127.0.0.1:5432/tracker_db
PROVIO_INGEST_HMAC_SECRET=provio_local_secret_123
ALLOW_UNSIGNED_LOCAL_JOBS=true
GO_INGEST_BATCH_SIZE=1
```

Before production:

```env
ALLOW_UNSIGNED_LOCAL_JOBS=false
GO_INGEST_BATCH_SIZE=50
```

## Running Workers

Run each worker from:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python"
```

Telegram:

```powershell
python workers\tg_worker.py
```

Discord:

```powershell
node workers\discord_worker.js
```

WhatsApp:

```powershell
node workers\wa_worker.js
```

Go ingestion:

```powershell
cd "C:\Users\avihs\Desktop\Work 5\DSIE\externaltracker-python\ingest"
go run -buildvcs=false go_ingest.go
```

## Manual End-to-End Test

Until OpenClaw is implemented, manually push a clean job into Redis:

```powershell
docker exec provio-redis redis-cli RPUSH provio_clean_jobs_staging "{\"title\":\"Data Science Intern\",\"company_slug\":\"Provio Social Test\",\"job_source_url\":\"https://example.com/apply-data-science\",\"raw_description\":\"Hiring fresher data science intern in India\",\"raw_location_text\":\"Remote, India\",\"posted_timestamp\":\"2026-07-03T12:30:00Z\",\"tech_track\":\"data_science\",\"source\":\"manual_clean_test\"}"
```

Then run the Go ingestion worker. Expected output:

```txt
[GO-CORE] Connected to Redis.
[GO-CORE] Connected to PostgreSQL.
[GO-CORE] Ingestion Funnel active. BATCH_SIZE=1
[GO-CORE] ALLOW_UNSIGNED_LOCAL_JOBS=true
[GO-CORE] Queued clean job: Data Science Intern @ Provio Social Test
[DB-SUCCESS] Inserted/ignored batch of 1 jobs into social_jobs.
```

Verify in PostgreSQL:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d tracker_db -c "SELECT id, title, company_slug, job_source_url, tech_track, source FROM social_jobs;"
```

## Main Remaining Work

### 1. Build OpenClaw Worker

This is the most important missing component.

Needed:

- `BLPOP provio_raw_messages_queue`
- Parse raw social messages
- Classify job vs non-job
- Filter India-only roles
- Filter fresher/entry-level roles
- Filter tech tracks
- Normalize into `ValidatedJobPayload`
- Sign with HMAC
- `RPUSH provio_clean_jobs_staging`
- Log accepted and rejected messages with reasons

### 2. Verify Live Listener Capture

Each listener has code, but live capture should be proven with fresh messages:

- Telegram: fresh channel post increases `provio_raw_messages_queue`
- Discord: bot receives a channel message and queue increases
- WhatsApp: fresh incoming message increases queue

### 3. Add Migrations

The PostgreSQL table was created manually. Add versioned SQL migrations such as:

```txt
migrations/001_create_social_jobs.sql
```

### 4. Add Tests

Useful test targets:

- Telegram payload shape
- Discord payload shape
- WhatsApp payload shape
- OpenClaw decision rules
- HMAC verification
- Go parser direct payload and envelope payload
- Go duplicate URL behavior
- DB transaction rollback/rebuffering

### 5. Improve Operations

Needed before production:

- Structured logs
- Worker health checks
- Queue depth monitoring
- Dead-letter queues
- Retry strategy
- Graceful shutdown for all workers
- Docker Compose for Redis, workers, and local Postgres
- Deployment notes

### 6. Security Cleanup

Before sharing or deploying:

- Rotate any credentials that were ever pasted into local `.env` or terminal logs
- Keep `.env`, session files, and QR/auth data out of Git
- Move production secrets into a secret manager
- Disable `ALLOW_UNSIGNED_LOCAL_JOBS`
- Require HMAC signatures from OpenClaw to Go ingestion

## Current Bottom Line

We have built the DSIE foundation: listeners, Redis queue contracts, WhatsApp/Telegram login flows, Discord worker structure, PostgreSQL table, and a real Go-to-Postgres insertion path.

The remaining major gap is the OpenClaw AI decision layer. Once that worker exists, the project can run the real pipeline:

```txt
Telegram/WhatsApp/Discord
-> provio_raw_messages_queue
-> OpenClaw filtering and normalization
-> provio_clean_jobs_staging
-> Go ingestion
-> PostgreSQL social_jobs
```

That is the next milestone.
