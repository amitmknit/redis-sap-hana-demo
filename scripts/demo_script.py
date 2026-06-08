#!/usr/bin/env python3
"""
demo_script.py — CLI walkthrough for the Redis + SAP HANA customer demo.
Run this after `docker compose up -d` to narrate each use case step-by-step.
"""

import requests, json, time, sys

BASE = "http://localhost:5000"

def banner(title):
    print("\n" + "═" * 60)
    print(f"  {title}")
    print("═" * 60)

def show(resp):
    print(json.dumps(resp, indent=2))

def pause(msg="Press ENTER to continue..."):
    input(f"\n  ▶  {msg} ")

# ─────────────────────────────────────────────────────────────
banner("HEALTH CHECK — confirm all services are up")
r = requests.get(f"{BASE}/health").json()
show(r)
if r.get("redis") != "ok" or r.get("mock_hana") != "ok":
    print("\n❌  Services not ready. Run: docker compose up -d")
    sys.exit(1)
print("\n✅  All services healthy — ready to demo!")

pause()

# ─────────────────────────────────────────────────────────────
banner("USE CASE 1 — HANA Read Offload")
print("""
TALKING POINT:
  Every Fiori read for master data, pricing, OData metadata hits HANA today.
  With Redis as a cache, the first call primes the cache — every subsequent
  call is served in under 1ms without touching HANA.
""")

pause("STEP 1a: Cold read — fetches from 'HANA', primes Redis cache")
r1 = requests.get(f"{BASE}/material/MAT-001?bypass_cache=true").json()
show(r1)
print(f"\n  ⏱  HANA latency: {r1['latency_ms']} ms")

pause("STEP 1b: Warm read — served from Redis cache")
r2 = requests.get(f"{BASE}/material/MAT-001").json()
show(r2)
print(f"\n  ⚡  Redis latency: {r2['latency_ms']} ms  ({round(r1['latency_ms']/max(r2['latency_ms'],0.001))}× faster)")

# ─────────────────────────────────────────────────────────────
banner("USE CASE 2 — Fiori Session Management")
print("""
TALKING POINT:
  SAP SuccessFactors found 2.4M SQL queries per day against HANA were
  driven purely by HTTP session persistence. Redirect sessions to Redis
  — HANA never sees them.
""")

pause("STEP 2a: Create Fiori session (stored in Redis only)")
sr = requests.post(f"{BASE}/session/create", json={"user": "amit.malik"}).json()
show(sr)
sess_id = sr["session_id"]

pause("STEP 2b: Read session back (sub-millisecond)")
sr2 = requests.get(f"{BASE}/session/{sess_id}").json()
show(sr2)
print(f"\n  ⚡  Session read: {sr2['latency_ms']} ms  (TTL remaining: {sr2['ttl_remaining_seconds']}s)")

# ─────────────────────────────────────────────────────────────
banner("USE CASE 3 — RDI / CDC Event Streaming")
print("""
TALKING POINT:
  When SAP HANA data changes, Redis Data Integration (RDI) captures the
  CDC event and publishes it to Redis Streams. Downstream consumers
  (cache refresh, analytics, AI pipelines) react in real time.
""")

pause("STEP 3a: Publish a price-change CDC event for MAT-001")
cdc = requests.post(f"{BASE}/cdc/publish", json={"material_id": "MAT-001", "new_price": 4750}).json()
show(cdc)

pause("STEP 3b: Cache is now stale — next read will re-fetch from HANA")
r3 = requests.get(f"{BASE}/material/MAT-001").json()
show(r3)

pause("STEP 3c: Read the Redis Stream (last 5 events)")
stream = requests.get(f"{BASE}/cdc/stream?count=5").json()
show(stream)

# ─────────────────────────────────────────────────────────────
banner("USE CASE 4 — Reporting Acceleration")
print("""
TALKING POINT:
  Heavy analytical CDS views and aggregated reports block HANA for
  transactional users. Pre-materialise them into Redis once, serve
  all dashboard reads from Redis at sub-millisecond speed.
""")

pause("STEP 4a: Materialise department-spend report from HANA into Redis")
rr = requests.post(f"{BASE}/report/refresh").json()
show(rr)
print(f"\n  ⏱  HANA materialisation: {rr['hana_query_ms']} ms (runs once per refresh cadence)")

pause("STEP 4b: Serve report from Redis — HANA NOT queried")
rep = requests.get(f"{BASE}/report").json()
show(rep)
print(f"\n  ⚡  Redis report latency: {rep.get('redis_latency_ms')} ms")

# ─────────────────────────────────────────────────────────────
banner("REDIS STATS — cache hits / misses / keys")
info = requests.get(f"{BASE}/redis-info").json()
show(info)
print(f"\n  📊  Cache hit rate: {info['hit_rate_pct']}%")

banner("DEMO COMPLETE ✅")
print("""
  Key takeaways for the customer:
  ─────────────────────────────────
  • Use case 1: Sub-ms reads vs ~40ms HANA — 30-40% load reduction
  • Use case 2: Sessions in Redis — 2.4M daily HANA queries eliminated
  • Use case 3: CDC events via Redis Streams — real-time data pipeline
  • Use case 4: Pre-materialised reports — HANA protected from analytics
  • All four patterns on one Redis platform — not just a cache
""")
