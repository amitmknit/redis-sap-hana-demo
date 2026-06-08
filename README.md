# Redis + SAP HANA Integration Demo

A fully self-contained demo showing four production-grade Redis integration patterns for SAP HANA environments — **no Redis Enterprise license or SAP license required**. Runs entirely on your laptop using Docker.

> Built to accompany the *Redis Enterprise for SAP HANA* solution deck.  
> Designed for pre-sales and architecture conversations with SAP customers.

---

## What This Demo Shows

| # | Use Case | Pattern | Customer Value |
|---|----------|---------|----------------|
| 1 | **HANA Read Offload** | Cache-aside | ~30–40% HANA read load reduction; sub-ms response |
| 2 | **Fiori Session Management** | Session store | ~2.4M daily HANA SQL queries eliminated |
| 3 | **RDI / CDC Event Streaming** | Redis Streams | Real-time SAP change propagation, no bespoke ETL |
| 4 | **Reporting Acceleration** | Pre-materialised views | HANA protected from analytical workload spikes |

The demo substitutes **Redis OSS** (free) for Redis Enterprise and **PostgreSQL** (free) for SAP HANA. The integration patterns, latency comparisons, and data flows are identical to a production deployment.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Your Laptop                          │
│                                                          │
│  ┌──────────────┐     ┌──────────────┐                  │
│  │  Flask App   │────▶│  Redis OSS   │  ← cache, sessions│
│  │  (port 5000) │     │  (port 6379) │    streams, reports│
│  └──────┬───────┘     └──────────────┘                  │
│         │                                                │
│         │ cache miss / CDC / report refresh              │
│         ▼                                                │
│  ┌──────────────┐                                        │
│  │  PostgreSQL  │  ← simulates SAP HANA                  │
│  │  (port 5432) │    (same SQL dialect for demo)          │
│  └──────────────┘                                        │
└─────────────────────────────────────────────────────────┘
```

---

## Prerequisites

Install these free tools before starting:

| Tool | Purpose | Download |
|------|---------|----------|
| Docker Desktop | Runs all services | https://www.docker.com/products/docker-desktop |
| Git | Version control | https://git-scm.com |
| Python 3.10+ | CLI demo script | https://www.python.org |

---

## Quick Start — Step by Step

### Step 1 — Clone the repository

```bash
# After you have pushed to GitHub (see GitHub Setup below):
git clone https://github.com/YOUR_USERNAME/redis-sap-hana-demo.git
cd redis-sap-hana-demo
```

Or if running locally from this folder:

```bash
cd redis-sap-hana-demo
```

---

### Step 2 — Start all services

```bash
docker compose up -d
```

This pulls Redis OSS and PostgreSQL images (free, ~200 MB total) and starts:
- Redis on `localhost:6379`
- PostgreSQL (mock HANA) on `localhost:5432`
- Flask demo app on `localhost:5000`

Wait ~20 seconds for services to become healthy:

```bash
docker compose ps          # all three services should show "healthy"
```

---

### Step 3 — Open the web dashboard

Open your browser to: **http://localhost:5000**

You will see a dark dashboard with four panels — one per use case.

---

### Step 4 — Run the demo (web dashboard)

#### Use Case 1 — HANA Read Offload

1. Click **"Read (bypass cache)"** → response comes from HANA (~40–50 ms)
2. Click **"Read (use cache)"** → response comes from Redis (<1 ms)
3. Point out the `latency_ms` difference in the JSON output
4. Click **"Flush cache"** to reset, then repeat to show the pattern

**Talking point:** _"The first request primes the cache. Every subsequent request — from any user, any Fiori session — is served from Redis at sub-millisecond speed. HANA is never hit again until the TTL expires or a CDC event invalidates the entry."_

---

#### Use Case 2 — Fiori Session Management

1. Enter a username and click **"Create Session"**
2. Note the response: `"stored_in": "Redis (HANA NOT written to)"`
3. Click **"Read Session"** — observe the sub-millisecond latency and TTL countdown

**Talking point:** _"SAP SuccessFactors published a study showing 2.4 million daily SQL queries against HANA were purely from session persistence. Redirecting sessions to Redis eliminates that load entirely — no code changes needed for standards-based session APIs."_

---

#### Use Case 3 — RDI / CDC Event Streaming

1. Set a Material ID and new price, click **"Publish CDC Event"**
2. Note the response: stream entry ID + `"cache_invalidated": true`
3. Click **"Read Stream"** to see the last 5 change events
4. Go back to Use Case 1 and read MAT-001 — it re-fetches from HANA (cache was invalidated)

**Talking point:** _"This simulates what Redis Data Integration does in production — the RDI connector tails the HANA transaction log and publishes change events to Redis Streams. Downstream consumers (cache invalidation, analytics, AI pipelines) react in real time. No bespoke ETL pipelines required."_

---

#### Use Case 4 — Reporting Acceleration

1. Click **"Materialise from HANA"** — note the ~80 ms HANA query time
2. Click **"Serve from Redis"** multiple times — observe sub-millisecond latency every time
3. Explain that this job runs on a schedule (or triggered by RDI on data change)

**Talking point:** _"Heavy CDS views and aggregated reports are the biggest source of HANA CPU spikes. Pre-materialising them into Redis means every dashboard hit — across hundreds of concurrent Fiori users — is served in under 1ms. HANA stays available for transactional workloads."_

---

### Step 5 — (Optional) CLI walkthrough

For a narrated, step-by-step CLI demo that prints each result with talking points:

```bash
pip install requests
python scripts/demo_script.py
```

Press ENTER at each step to advance. Ideal for screen-sharing with a customer.

---

### Step 6 — View Redis stats

In the web dashboard, click **"Redis Stats"** to show:
- Total keys in Redis
- Cache hit / miss counts
- Hit rate %

This is a live view of how Redis is serving the workload.

---

## GitHub Setup

### First time — create repository and push

```bash
# 1. Initialise git (already done if you cloned)
cd redis-sap-hana-demo
git init

# 2. Stage all files
git add .

# 3. Commit
git commit -m "feat: Redis + SAP HANA integration demo — 4 use cases"

# 4. Create a new repo on GitHub:
#    Go to https://github.com/new
#    Name it: redis-sap-hana-demo
#    Keep it private or public — your choice
#    Do NOT initialise with README (you already have one)

# 5. Add the remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/redis-sap-hana-demo.git

# 6. Push
git branch -M main
git push -u origin main
```

### Subsequent updates

```bash
git add .
git commit -m "fix: update talking points in demo script"
git push
```

---

## Saving to Your Local Computer

The project is already on your local machine when you run `docker compose up`. To make a clean local copy for offline use:

```bash
# Copy to your Documents folder (macOS/Linux)
cp -r redis-sap-hana-demo ~/Documents/

# Windows (PowerShell)
Copy-Item -Recurse redis-sap-hana-demo $env:USERPROFILE\Documents\
```

To run offline (no internet after first pull):

```bash
# Save Docker images for offline use
docker save redis:7.2-alpine postgres:16-alpine | gzip > sap-demo-images.tar.gz

# Restore on another machine
docker load < sap-demo-images.tar.gz
```

---

## Stopping the Demo

```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop + remove volumes (full reset)
```

---

## API Reference

All endpoints are available via the web dashboard and directly:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Web dashboard |
| GET | `/material/<id>` | Read material (cached) |
| GET | `/material/<id>?bypass_cache=true` | Force HANA read |
| POST | `/flush-cache` | Clear material cache |
| POST | `/session/create` | Create Fiori session |
| GET | `/session/<id>` | Read session |
| POST | `/cdc/publish` | Publish CDC event to stream |
| GET | `/cdc/stream` | Read last N stream entries |
| POST | `/report/refresh` | Materialise report from HANA |
| GET | `/report` | Serve report from Redis |
| GET | `/redis-info` | Cache stats |
| GET | `/health` | Service health check |

---

## Mapping Demo → Production

| Demo Component | Production Equivalent |
|---------------|----------------------|
| Redis OSS (Docker) | Redis Enterprise (cluster, HA, TLS, ACLs) |
| PostgreSQL | SAP HANA S/4 or ECC |
| Flask integration layer | Java service, SAP BTP extension, or SAP-native connector |
| Simulated CDC via `/cdc/publish` | Redis Data Integration (RDI) with log-based CDC connector |
| `time.sleep(0.04)` HANA delay | Real HANA network + OData/RFC parse overhead |
| TTL-based invalidation | TTL + RDI-driven event invalidation |

---

## License

MIT — free to use, modify, and share.

---

*Built for Redis pre-sales demos. All data is synthetic. No SAP or Redis licenses required.*
