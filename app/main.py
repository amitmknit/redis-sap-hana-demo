"""
Redis + SAP HANA Integration Demo — NSW Police Edition
=======================================================
Four use cases mapped to real NSW Police SAP scenarios:

  1. HANA Read Offload    — Equipment & asset master data cache
  2. Fiori Session Mgmt  — Officer portal session management
  3. CDC / Event Streams  — Real-time equipment assignment propagation (OData polling sim)
  4. Reporting + Search  — Operational dashboards + Redis Search on officer/asset data
"""

import os, json, time, secrets
from decimal import Decimal
from datetime import datetime, date
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

# ── Custom JSON encoder ────────────────────────────────────────────────────────
class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return str(obj)
        return super().default(obj)

def safe_dumps(data):
    return json.dumps(data, cls=SafeEncoder)

# ── Connections ────────────────────────────────────────────────────────────────
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

def get_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "sapdb"),
        user=os.getenv("PG_USER", "sapuser"),
        password=os.getenv("PG_PASSWORD", "sappass"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# ── Bootstrap Redis Search index on startup ───────────────────────────────────
def bootstrap_search_index():
    """Load NSW Police officer & asset data into Redis and create search index."""
    try:
        r.ft("idx:officers").info()
        return  # index already exists
    except Exception:
        pass

    # Officer records (simulates SAP HR / SuccessFactors data)
    officers = [
        {"id": "EMP-1001", "name": "Sarah Mitchell",   "rank": "Senior Constable", "station": "Sydney CBD",    "division": "Uniform", "status": "Active",    "equipment_count": 4},
        {"id": "EMP-1002", "name": "James Okafor",     "rank": "Detective",        "station": "Parramatta",    "division": "CID",     "status": "Active",    "equipment_count": 3},
        {"id": "EMP-1003", "name": "Priya Nair",       "rank": "Sergeant",         "station": "Chatswood",     "division": "Uniform", "status": "Active",    "equipment_count": 5},
        {"id": "EMP-1004", "name": "Tom Berglund",     "rank": "Constable",        "station": "Liverpool",     "division": "Traffic", "status": "On Leave",  "equipment_count": 2},
        {"id": "EMP-1005", "name": "Ana Reyes",        "rank": "Inspector",        "station": "Sydney CBD",    "division": "Command", "status": "Active",    "equipment_count": 6},
        {"id": "EMP-1006", "name": "David Nguyen",     "rank": "Senior Constable", "station": "Bankstown",     "division": "Uniform", "status": "Active",    "equipment_count": 4},
        {"id": "EMP-1007", "name": "Emma Walsh",       "rank": "Detective",        "station": "Parramatta",    "division": "CID",     "status": "Active",    "equipment_count": 3},
        {"id": "EMP-1008", "name": "Raj Patel",        "rank": "Constable",        "station": "Newcastle",     "division": "Uniform", "status": "Active",    "equipment_count": 3},
        {"id": "EMP-1009", "name": "Fatima Hassan",    "rank": "Sergeant",         "station": "Wollongong",    "division": "Traffic", "status": "Active",    "equipment_count": 5},
        {"id": "EMP-1010", "name": "Chris Lawson",     "rank": "Superintendent",   "station": "HQ Parramatta", "division": "Command", "status": "Active",    "equipment_count": 2},
    ]

    # Asset/equipment records (simulates SAP Plant Maintenance / MM data)
    assets = [
        {"id": "ASSET-001", "description": "Glock 17 Service Pistol",     "category": "Firearm",   "station": "Sydney CBD",    "assigned_to": "EMP-1001", "status": "Issued",    "value": 950.00},
        {"id": "ASSET-002", "description": "Axon Body Camera 3",          "category": "Equipment", "station": "Sydney CBD",    "assigned_to": "EMP-1001", "status": "Issued",    "value": 800.00},
        {"id": "ASSET-003", "description": "Ford Ranger Patrol Vehicle",  "category": "Vehicle",   "station": "Parramatta",    "assigned_to": "EMP-1002", "status": "Issued",    "value": 52000.00},
        {"id": "ASSET-004", "description": "Taser X26P",                  "category": "Equipment", "station": "Chatswood",     "assigned_to": "EMP-1003", "status": "Issued",    "value": 1200.00},
        {"id": "ASSET-005", "description": "Motorola APX Radio",          "category": "Radio",     "station": "Liverpool",     "assigned_to": "EMP-1004", "status": "In Service","value": 3500.00},
        {"id": "ASSET-006", "description": "Toyota Hilux Pursuit Vehicle","category": "Vehicle",   "station": "Bankstown",     "assigned_to": "EMP-1006", "status": "Issued",    "value": 58000.00},
        {"id": "ASSET-007", "description": "Glock 17 Service Pistol",     "category": "Firearm",   "station": "Newcastle",     "assigned_to": "EMP-1008", "status": "Issued",    "value": 950.00},
        {"id": "ASSET-008", "description": "Axon Body Camera 3",          "category": "Equipment", "station": "Wollongong",    "assigned_to": "EMP-1009", "status": "Maintenance","value": 800.00},
        {"id": "ASSET-009", "description": "Holden Commodore Highway",    "category": "Vehicle",   "station": "Wollongong",    "assigned_to": "EMP-1009", "status": "Issued",    "value": 48000.00},
        {"id": "ASSET-010", "description": "Forensic Evidence Kit",       "category": "Equipment", "station": "Parramatta",    "assigned_to": "EMP-1002", "status": "Issued",    "value": 2200.00},
    ]

    # Write to Redis as JSON hashes
    pipe = r.pipeline()
    for o in officers:
        pipe.hset(f"officer:{o['id']}", mapping={k: str(v) for k, v in o.items()})
    for a in assets:
        pipe.hset(f"asset:{a['id']}", mapping={k: str(v) for k, v in a.items()})
    pipe.execute()

    # Create Redis Search indexes
    try:
        r.ft("idx:officers").create_index(
            [
                TextField("name"),
                TagField("rank"),
                TagField("station"),
                TagField("division"),
                TagField("status"),
                NumericField("equipment_count"),
            ],
            definition=IndexDefinition(prefix=["officer:"], index_type=IndexType.HASH)
        )
    except Exception:
        pass

    try:
        r.ft("idx:assets").create_index(
            [
                TextField("description"),
                TagField("category"),
                TagField("station"),
                TagField("assigned_to"),
                TagField("status"),
                NumericField("value"),
            ],
            definition=IndexDefinition(prefix=["asset:"], index_type=IndexType.HASH)
        )
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# HOME — demo dashboard
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
def home():
    return render_template_string(HTML_DASHBOARD)

# ══════════════════════════════════════════════════════════════════════════════
# USE CASE 1 — EQUIPMENT & ASSET MASTER DATA CACHE
# NSW Police: Officers and dispatchers look up equipment/asset data constantly.
# Every lookup today hits SAP HANA. Redis caches these reads.
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/asset/<asset_id>")
def get_asset(asset_id):
    bypass = request.args.get("bypass_cache", "false").lower() == "true"
    cache_key = f"cache:asset:{asset_id}"

    if not bypass:
        t0 = time.perf_counter()
        cached = r.get(cache_key)
        if cached:
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
            return jsonify({
                "source": "REDIS cache hit ✅",
                "latency_ms": elapsed_ms,
                "scenario": "Dispatcher checks officer equipment — SAP HANA NOT queried",
                "data": json.loads(cached)
            })

    # Cache miss — fetch from SAP HANA
    t0 = time.perf_counter()
    time.sleep(0.04)  # realistic HANA latency ~40ms
    conn = get_pg()
    cur = conn.cursor()
    cur.execute("""
        SELECT e.employee_id, e.full_name, e.department as division, e.location as station,
               mm.material_id as asset_id, mm.description as asset_description,
               mm.unit_price as asset_value
        FROM employees e
        JOIN material_master mm ON mm.plant = LEFT(e.location, 3)
        WHERE mm.material_id = %s
        LIMIT 1
    """, (asset_id,))
    row = cur.fetchone()
    conn.close()
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

    if not row:
        # Return mock asset data if join returns nothing
        data = {
            "asset_id": asset_id,
            "description": "Glock 17 Service Pistol" if "001" in asset_id else "Axon Body Camera 3",
            "station": "Sydney CBD",
            "status": "Issued",
            "last_checked": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
    else:
        data = dict(row)

    r.setex(cache_key, 600, safe_dumps(data))

    return jsonify({
        "source": "SAP HANA — cache miss, now cached ⏳",
        "latency_ms": elapsed_ms,
        "scenario": "First lookup hits HANA. Every subsequent lookup served from Redis in <1ms.",
        "data": json.loads(safe_dumps(data))
    })

@app.route("/flush-cache", methods=["POST"])
def flush_cache():
    keys = r.keys("cache:asset:*") + r.keys("cache:officer:*")
    if keys:
        r.delete(*keys)
    return jsonify({"flushed": len(keys), "message": "Asset/officer cache cleared — next read will hit HANA"})

# ══════════════════════════════════════════════════════════════════════════════
# USE CASE 2 — OFFICER PORTAL SESSION MANAGEMENT
# NSW Police: Officers log into SAP Fiori (HR self-service, leave, rosters).
# Every session read/write hits HANA today. Redis eliminates this entirely.
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/session/create", methods=["POST"])
def create_session():
    body = request.get_json(silent=True) or {}
    officer_id = body.get("officer_id", "EMP-1001")
    name = body.get("name", "Sarah Mitchell")
    rank = body.get("rank", "Senior Constable")
    station = body.get("station", "Sydney CBD")

    session_id = f"sess:{secrets.token_hex(12)}"
    session_data = {
        "officer_id": officer_id,
        "name": name,
        "rank": rank,
        "station": station,
        "roles": ["FIORI_HR", "LEAVE_APPLY", "ROSTER_VIEW", "EQUIPMENT_VIEW"],
        "fiori_launchpad": "https://nswpolice.fiori.sap/sap/bc/ui2/flp",
        "sap_client": "100",
        "login_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ip_address": request.remote_addr or "10.0.1.45"
    }
    # Written to Redis ONLY — SAP HANA never touched
    r.setex(session_id, 1800, json.dumps(session_data))

    return jsonify({
        "session_id": session_id,
        "ttl_seconds": 1800,
        "stored_in": "Redis ONLY — SAP HANA NOT written to ✅",
        "impact": "Based on the SAP SuccessFactors published study: ~2.4M daily HANA SQL queries eliminated through session offload. NSW Police impact requires PoC sizing.",
        "session": session_data
    })

@app.route("/session/<session_id>")
def read_session(session_id):
    t0 = time.perf_counter()
    raw = r.get(session_id)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
    if not raw:
        return jsonify({"error": "Session expired or not found"}), 404
    return jsonify({
        "source": "Redis ✅",
        "latency_ms": elapsed_ms,
        "ttl_remaining_seconds": r.ttl(session_id),
        "scenario": "Officer navigates Fiori — session read from Redis, HANA untouched",
        "session": json.loads(raw)
    })

# ══════════════════════════════════════════════════════════════════════════════
# USE CASE 3 — SAP HANA → REDIS: CDC / CHANGE EVENT STREAMING
#
# NSW Police: Sergeant reassigns equipment in SAP → change detected →
# Redis Streams notifies COPS, CAD, Asset Register in <100ms.
#
# INTEGRATION NOTE (PoC required):
# In a real SAP HANA environment, the change event would be captured by one of:
#   Option A — SAP SLT (trigger-based CDC, near real-time, ~5-10% write overhead)
#   Option B — OData polling (app-layer polling, seconds latency, zero SAP impact)
#   Option C — SAP BTP Integration Suite (event-driven, requires BTP licence)
#   Option D — Custom ABAP exit (most control, highest effort)
# This demo simulates Option B (OData polling) — the safest PoC starting point.
# RDI does NOT support SAP HANA as a source (supports Oracle/SQL Server/PostgreSQL).
# ══════════════════════════════════════════════════════════════════════════════
STREAM_KEY = "nsw.police:sap:equipment_changes"

@app.route("/cdc/publish", methods=["POST"])
def publish_cdc_event():
    body = request.get_json(silent=True) or {}
    officer_id   = body.get("officer_id", "EMP-1001")
    asset_id     = body.get("asset_id", "ASSET-001")
    change_type  = body.get("change_type", "EQUIPMENT_ASSIGNED")
    old_value    = body.get("old_value", "EMP-1004")
    new_value    = body.get("new_value", officer_id)

    # ── Simulate OData polling detecting a change in SAP HANA ────────────────
    # In production this would be triggered by:
    #   - SAP SLT detecting a row change in EQUI table via trigger-based CDC, OR
    #   - A polling service calling GET /sap/opu/odata/sap/API_EQUIPMENT/... 
    #     and detecting a changed ETag / LastChangeDateTime field
    # The integration layer then writes the event to Redis Streams.
    # ─────────────────────────────────────────────────────────────────────────
    event_id = r.xadd(STREAM_KEY, {
        "operation":         "UPDATE",
        "sap_table":         "EQUI",           # SAP equipment master table
        "asset_id":          asset_id,
        "change_type":       change_type,
        "officer_id":        officer_id,
        "old_assigned_to":   old_value,
        "new_assigned_to":   new_value,
        "integration_path":  "OData-polling",  # PoC: swap to SLT-CDC in production
        "poc_note":          "SAP HANA CDC path requires PoC validation. RDI not used (HANA not a supported RDI source).",
        "source_system":     "SAP_ECC_NSWPOL",
        "timestamp":         time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "downstream_consumers": "COPS,CAD,AssetRegister,AccessControl"
    })

    # Invalidate Redis cache for this asset (forces fresh read next time)
    invalidated = r.delete(f"cache:asset:{asset_id}")

    # Update the asset hash in Redis Search index
    r.hset(f"asset:{asset_id}", mapping={
        "assigned_to": officer_id,
        "status": "Issued",
    })

    return jsonify({
        "stream": STREAM_KEY,
        "event_id": event_id,
        "cache_invalidated": bool(invalidated),
        "scenario": f"Sergeant reassigned {asset_id} to {officer_id} in SAP Fiori",
        "integration_simulated": "OData polling (Option B — safest PoC path)",
        "poc_required": "All SAP HANA CDC integration paths require validation in NSW Police environment",
        "integration_options": {
            "Option_A_SLT":  "Trigger-based CDC, near real-time, ~5-10% HANA write overhead. Licence included in HANA Enterprise / RISE / S4HANA.",
            "Option_B_OData": "App-layer polling, seconds latency, zero HANA impact. Recommended for PoC.",
            "Option_C_BTP":  "SAP BTP Integration Suite event-driven. Requires BTP licence check.",
            "Option_D_ABAP": "Custom ABAP exit. Highest effort, most control.",
            "RDI_note":      "Redis Data Integration (RDI) does NOT support SAP HANA as a CDC source."
        },
        "what_happens_next": {
            "COPS":           "Officer profile updated in <100ms",
            "CAD":            "Dispatch system reflects new equipment in <100ms",
            "Asset_Register": "Chain of custody updated automatically",
            "Access_Control": "Building/vehicle access rights updated"
        },
        "without_redis": "All systems would remain out of sync until tonight batch job"
    })

@app.route("/cdc/stream")
def read_stream():
    count = int(request.args.get("count", 5))
    entries = r.xrevrange(STREAM_KEY, count=count)
    return jsonify({
        "stream": STREAM_KEY,
        "description": "SAP HANA equipment changes — simulating OData polling integration path (PoC required for production)",
        "entry_count": len(entries),
        "entries": [{"event_id": eid, "data": edata} for eid, edata in entries]
    })

# ══════════════════════════════════════════════════════════════════════════════
# USE CASE 4 — OPERATIONAL REPORTING + REDIS SEARCH
# NSW Police: Commanders need live dashboards on fleet, equipment, staffing.
# Pre-materialised reports + Redis Search lets them query across the entire
# officer/asset dataset in milliseconds — without touching HANA at all.
# ══════════════════════════════════════════════════════════════════════════════
REPORT_KEY = "report:nsw_police:operational_summary"

@app.route("/report/refresh", methods=["POST"])
def refresh_report():
    """Materialise operational summary from SAP HANA into Redis."""
    t0 = time.perf_counter()
    time.sleep(0.09)  # simulate heavy HANA analytical query

    conn = get_pg()
    cur = conn.cursor()

    # Simulate department staffing summary (from SAP HR)
    cur.execute("""
        SELECT department as division,
               COUNT(*) as officer_count,
               COUNT(DISTINCT location) as stations_covered
        FROM employees
        GROUP BY department
        ORDER BY officer_count DESC
    """)
    staffing = [dict(r) for r in cur.fetchall()]

    # Simulate asset value by location (from SAP Plant Maintenance)
    cur.execute("""
        SELECT plant as station_code,
               COUNT(*) as asset_count,
               SUM(unit_price) as total_asset_value,
               AVG(unit_price) as avg_asset_value
        FROM material_master
        GROUP BY plant
        ORDER BY total_asset_value DESC
    """)
    assets = [dict(r) for r in cur.fetchall()]
    conn.close()

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "report_name": "NSW Police — SAP Operational Summary",
        "staffing_by_division": staffing,
        "assets_by_station": assets,
        "total_officers": sum(s["officer_count"] for s in staffing),
        "total_asset_value": sum(float(a["total_asset_value"]) for a in assets)
    }
    r.setex(REPORT_KEY, 600, safe_dumps(report))
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

    return jsonify({
        "message": "Operational report materialised from SAP HANA → cached in Redis",
        "hana_query_ms": elapsed_ms,
        "cached_key": REPORT_KEY,
        "ttl_seconds": 600,
        "scenario": "This job runs on schedule (or triggered by SAP SLT / OData polling on data change). All dashboard reads served from Redis."
    })

@app.route("/report")
def get_report():
    t0 = time.perf_counter()
    cached = r.get(REPORT_KEY)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
    if cached:
        report = json.loads(cached)
        report["served_from"] = "Redis ✅ — SAP HANA NOT queried"
        report["redis_latency_ms"] = elapsed_ms
        return jsonify(report)
    return jsonify({
        "error": "Report not materialised yet",
        "action": "POST /report/refresh first"
    }), 404

# ── Redis Search endpoints ─────────────────────────────────────────────────────

@app.route("/search/officers")
def search_officers():
    """
    Search officer records using Redis Search.
    Examples:
      /search/officers?q=Mitchell
      /search/officers?station=Sydney+CBD
      /search/officers?rank=Detective
      /search/officers?division=CID
      /search/officers?status=Active
    """
    t0 = time.perf_counter()

    q           = request.args.get("q", "")
    station     = request.args.get("station", "")
    rank        = request.args.get("rank", "")
    division    = request.args.get("division", "")
    status      = request.args.get("status", "")

    # Build Redis Search query
    parts = []
    if q:
        parts.append(q)                                    # full-text on name
    if station:
        parts.append(f"@station:{{{station.replace(' ', '\\ ')}}}")
    if rank:
        parts.append(f"@rank:{{{rank.replace(' ', '\\ ')}}}")
    if division:
        parts.append(f"@division:{{{division}}}")
    if status:
        parts.append(f"@status:{{{status}}}")

    query_str = " ".join(parts) if parts else "*"

    try:
        results = r.ft("idx:officers").search(Query(query_str).paging(0, 20))
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        return jsonify({
            "source": "Redis Search ✅ — SAP HANA NOT queried",
            "latency_ms": elapsed_ms,
            "query": query_str,
            "total_results": results.total,
            "scenario": "Commander searches officer roster — instant results from Redis index",
            "officers": [doc.__dict__ for doc in results.docs]
        })
    except Exception as e:
        return jsonify({"error": str(e), "hint": "Try GET /search/seed first"}), 500

@app.route("/search/assets")
def search_assets():
    """
    Search asset/equipment records using Redis Search.
    Examples:
      /search/assets?category=Vehicle
      /search/assets?station=Parramatta
      /search/assets?status=Maintenance
      /search/assets?q=Camera
    """
    t0 = time.perf_counter()

    q        = request.args.get("q", "")
    category = request.args.get("category", "")
    station  = request.args.get("station", "")
    status   = request.args.get("status", "")

    parts = []
    if q:
        parts.append(q)
    if category:
        parts.append(f"@category:{{{category}}}")
    if station:
        parts.append(f"@station:{{{station.replace(' ', '\\ ')}}}")
    if status:
        parts.append(f"@status:{{{status}}}")

    query_str = " ".join(parts) if parts else "*"

    try:
        results = r.ft("idx:assets").search(Query(query_str).paging(0, 20))
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        return jsonify({
            "source": "Redis Search ✅ — SAP HANA NOT queried",
            "latency_ms": elapsed_ms,
            "query": query_str,
            "total_results": results.total,
            "scenario": "Asset manager searches equipment across all stations",
            "assets": [doc.__dict__ for doc in results.docs]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/search/assets/by-station")
def assets_by_station():
    """Aggregate: how many assets per station — served entirely from Redis."""
    t0 = time.perf_counter()
    try:
        results = r.ft("idx:assets").search(Query("*").paging(0, 100))
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        # Aggregate in Python (Redis FT.AGGREGATE also available)
        by_station = {}
        for doc in results.docs:
            station = getattr(doc, "station", "Unknown")
            by_station[station] = by_station.get(station, 0) + 1

        return jsonify({
            "source": "Redis Search ✅",
            "latency_ms": elapsed_ms,
            "scenario": "Fleet dashboard — asset count per station, no HANA query",
            "assets_by_station": by_station
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════════
# HEALTH + REDIS INFO
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/health")
def health():
    redis_ok = r.ping()
    try:
        conn = get_pg(); conn.close(); hana_ok = True
    except Exception:
        hana_ok = False
    search_ok = False
    try:
        r.ft("idx:officers").info(); search_ok = True
    except Exception:
        pass
    return jsonify({
        "redis": "ok" if redis_ok else "error",
        "mock_hana": "ok" if hana_ok else "error",
        "redis_search_index": "ok" if search_ok else "not loaded"
    })

@app.route("/redis-info")
def redis_info():
    info = r.info("stats")
    keys = r.dbsize()
    hits   = info.get("keyspace_hits", 0)
    misses = info.get("keyspace_misses", 0)
    return jsonify({
        "total_keys": keys,
        "total_commands_processed": info.get("total_commands_processed"),
        "keyspace_hits": hits,
        "keyspace_misses": misses,
        "hit_rate_pct": round(hits / max(hits + misses, 1) * 100, 1)
    })

# ══════════════════════════════════════════════════════════════════════════════
# HTML DASHBOARD — NSW Police Edition
# ══════════════════════════════════════════════════════════════════════════════
HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Redis + SAP HANA — NSW Police Demo</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0a0e1a; color: #e2e8f0; margin: 0; padding: 24px; }
  .header { display:flex; align-items:center; gap:16px; margin-bottom:6px; }
  h1   { color: #ff4438; margin: 0; font-size: 1.6rem; }
  .subtitle { color: #64748b; font-size: .9rem; margin-bottom: 24px; }
  .badge-police { background:#1e3a5f; color:#60a5fa; padding:3px 10px; border-radius:4px; font-size:.75rem; font-weight:600; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 20px; }
  .card { background: #111827; border-radius: 12px; padding: 22px; border: 1px solid #1f2937; }
  .card-title { color: #ff4438; margin: 0 0 4px 0; font-size: .9rem; text-transform: uppercase; letter-spacing: .07em; font-weight: 700; }
  .card-scenario { color: #60a5fa; font-size: .78rem; margin: 0 0 12px 0; font-style: italic; }
  .tag  { display:inline-block; background:#1f2937; border-radius:4px; padding:2px 8px; font-size:.72rem; color:#94a3b8; margin:0 4px 8px 0; }
  .tag-green { background:#064e3b; color:#34d399; }
  .tag-blue  { background:#1e3a5f; color:#60a5fa; }
  button { background:#ff4438; color:white; border:none; border-radius:6px; padding:8px 14px; cursor:pointer; font-size:.82rem; margin:3px 2px; font-weight:500; }
  button:hover { background:#e03020; }
  button.sec { background:#1f2937; color:#e2e8f0; }
  button.sec:hover { background:#374151; }
  button.blue { background:#1d4ed8; }
  button.blue:hover { background:#1e40af; }
  pre   { background:#060a12; border-radius:6px; padding:12px; font-size:.75rem; max-height:260px; overflow-y:auto; color:#4ade80; border:1px solid #1f2937; white-space:pre-wrap; margin-top:10px; }
  input, select { background:#060a12; border:1px solid #1f2937; border-radius:6px; color:#e2e8f0; padding:6px 10px; font-size:.82rem; }
  input { width:160px; }
  select { width:180px; }
  label { font-size:.78rem; color:#94a3b8; display:block; margin-bottom:3px; }
  .row  { display:flex; flex-wrap:wrap; gap:10px; align-items:flex-end; margin-bottom:8px; }
  .field { display:flex; flex-direction:column; }
  .impact { background:#0c1a0c; border:1px solid #14532d; border-radius:6px; padding:8px 12px; font-size:.78rem; color:#86efac; margin-bottom:10px; }
  .divider { border:none; border-top:1px solid #1f2937; margin:20px 0; }
  .section-label { color:#94a3b8; font-size:.75rem; text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }
</style>
</head>
<body>

<div class="header">
  <h1>🔴 Redis + SAP HANA</h1>
  <span class="badge-police">NSW POLICE DEMO</span>
</div>
<div class="subtitle">Four real-world use cases — running locally, zero licenses required</div>

<div class="grid">

  <!-- ── USE CASE 1 ── -->
  <div class="card">
    <p class="card-title">① Equipment & Asset Read Offload</p>
    <p class="card-scenario">🚔 Dispatcher checks officer equipment before dispatch — today every lookup hits SAP HANA</p>
    <span class="tag">Cache-aside</span><span class="tag tag-green">~30–40% HANA load reduction</span>
    <div class="impact">💡 With 10,000+ officers making multiple equipment lookups per shift, HANA is queried millions of times daily for data that rarely changes.</div>
    <div class="row">
      <div class="field">
        <label>Asset ID</label>
        <select id="assetId">
          <option>ASSET-001</option><option>ASSET-002</option><option>ASSET-003</option>
          <option>ASSET-004</option><option>ASSET-005</option>
        </select>
      </div>
    </div>
    <button onclick="fetchAsset(true)">🔴 Read from HANA (cold)</button>
    <button class="sec" onclick="fetchAsset(false)">⚡ Read from Redis (cached)</button>
    <button class="sec" onclick="flushCache()">🗑 Flush cache</button>
    <pre id="asset-out">← Start with "Read from HANA" then "Read from Redis" — watch the latency drop</pre>
  </div>

  <!-- ── USE CASE 2 ── -->
  <div class="card">
    <p class="card-title">② Officer Portal Session Management</p>
    <p class="card-scenario">👮 Officers log into SAP Fiori for HR self-service, leave, rosters — each session hits HANA today</p>
    <span class="tag">Session store</span><span class="tag tag-green">2.4M SQL queries/day eliminated</span>
    <div class="impact">💡 SAP SuccessFactors study: session persistence alone generated 2.4M daily HANA SQL queries. Redis eliminates all of them.</div>
    <div class="row">
      <div class="field">
        <label>Officer</label>
        <select id="sessOfficer">
          <option value='{"officer_id":"EMP-1001","name":"Sarah Mitchell","rank":"Senior Constable","station":"Sydney CBD"}'>Sarah Mitchell — Sr. Constable</option>
          <option value='{"officer_id":"EMP-1002","name":"James Okafor","rank":"Detective","station":"Parramatta"}'>James Okafor — Detective</option>
          <option value='{"officer_id":"EMP-1003","name":"Priya Nair","rank":"Sergeant","station":"Chatswood"}'>Priya Nair — Sergeant</option>
          <option value='{"officer_id":"EMP-1010","name":"Chris Lawson","rank":"Superintendent","station":"HQ Parramatta"}'>Chris Lawson — Superintendent</option>
        </select>
      </div>
    </div>
    <button onclick="createSession()">🔐 Create Fiori Session</button>
    <button class="sec" onclick="readSession()">⚡ Read Session from Redis</button>
    <pre id="sess-out">← Create a session — note "HANA NOT written to ✅"</pre>
  </div>

  <!-- ── USE CASE 3 ── -->
  <div class="card">
    <p class="card-title">③ SAP HANA → Redis CDC / Event Streaming</p>
    <p class="card-scenario">🔫 Sergeant reassigns equipment in SAP Fiori → change event flows to Redis Streams → COPS, CAD, Asset Register notified in &lt;100ms</p>
    <span class="tag">Redis Streams</span><span class="tag tag-green">No overnight batch jobs</span>
    <span class="tag" style="background:#3b1f00;color:#fb923c">⚠ PoC required</span>
    <div class="impact">💡 Demo simulates <strong>OData polling</strong> (zero SAP impact). Production options: SAP SLT trigger-based CDC (~5-10% write overhead, licence included in HANA Enterprise/RISE), SAP BTP Integration Suite, or custom ABAP. <strong>RDI does NOT support SAP HANA as a CDC source</strong> — RDI supports Oracle, SQL Server, MySQL, PostgreSQL.</div>
    <div class="row">
      <div class="field">
        <label>Asset</label>
        <select id="cdcAsset">
          <option value="ASSET-001">ASSET-001 — Glock 17</option>
          <option value="ASSET-002">ASSET-002 — Body Camera</option>
          <option value="ASSET-003">ASSET-003 — Ford Ranger</option>
          <option value="ASSET-005">ASSET-005 — Motorola Radio</option>
        </select>
      </div>
      <div class="field">
        <label>Assign to Officer</label>
        <select id="cdcOfficer">
          <option value="EMP-1001">EMP-1001 — S. Mitchell</option>
          <option value="EMP-1003">EMP-1003 — P. Nair</option>
          <option value="EMP-1006">EMP-1006 — D. Nguyen</option>
          <option value="EMP-1008">EMP-1008 — R. Patel</option>
        </select>
      </div>
    </div>
    <button onclick="publishCDC()">📡 Publish Assignment Event</button>
    <button class="sec" onclick="readStream()">📋 View Event Stream</button>
    <pre id="cdc-out">← Reassign equipment — see downstream systems notified instantly</pre>
  </div>

  <!-- ── USE CASE 4 ── -->
  <div class="card">
    <p class="card-title">④ Operational Reporting + Redis Search</p>
    <p class="card-scenario">📊 Commander needs live fleet/staffing dashboards + instant search across officer & asset records</p>
    <span class="tag">Pre-materialised reports</span><span class="tag tag-blue">Redis Search</span><span class="tag tag-green">HANA protected</span>
    <div class="impact">💡 Two capabilities: (1) Pre-materialised dashboards protect HANA from analytical spikes. (2) Redis Search lets commanders query 10,000+ officer & asset records in &lt;5ms.</div>

    <div class="section-label">Operational Dashboard</div>
    <button onclick="refreshReport()">🔄 Materialise from HANA</button>
    <button class="sec" onclick="getReport()">⚡ Serve from Redis</button>
    <pre id="report-out">← Click Materialise from HANA first, then Serve from Redis</pre>

    <hr class="divider">

    <div class="section-label">Redis Search — Officer Records</div>
    <div class="row">
      <div class="field"><label>Search name</label><input id="srchName" placeholder="e.g. Mitchell" style="width:130px"></div>
      <div class="field">
        <label>Station</label>
        <select id="srchStation"><option value="">Any</option><option>Sydney CBD</option><option>Parramatta</option><option>Chatswood</option><option>Bankstown</option><option>Newcastle</option></select>
      </div>
      <div class="field">
        <label>Division</label>
        <select id="srchDiv"><option value="">Any</option><option>Uniform</option><option>CID</option><option>Traffic</option><option>Command</option></select>
      </div>
    </div>
    <button class="blue" onclick="searchOfficers()">🔍 Search Officers</button>
    <pre id="officer-out">← Select filters and click Search Officers</pre>

    <hr class="divider">

    <div class="section-label">Redis Search — Asset / Equipment Records</div>
    <div class="row">
      <div class="field"><label>Search description</label><input id="srchAssetQ" placeholder="e.g. Camera" style="width:130px"></div>
      <div class="field">
        <label>Category</label>
        <select id="srchCat"><option value="">Any</option><option>Firearm</option><option>Vehicle</option><option>Equipment</option><option>Radio</option></select>
      </div>
      <div class="field">
        <label>Status</label>
        <select id="srchAssetStatus"><option value="">Any</option><option>Issued</option><option>Maintenance</option><option>In Service</option></select>
      </div>
    </div>
    <button class="blue" onclick="searchAssets()">🔍 Search Assets</button>
    <button class="sec" onclick="assetsByStation()">📍 Assets by Station</button>
    <pre id="asset-search-out">← Select filters and click Search Assets or Assets by Station</pre>
  </div>

</div>

<div style="margin-top:20px">
  <button onclick="getRedisInfo()" style="background:#1f2937">📊 Live Redis Stats</button>
  <pre id="info-out" style="display:none;margin-top:8px"></pre>
</div>

<script>
let lastSessionId = null;

async function api(method, url, body) {
  const opts = { method, headers: {'Content-Type':'application/json'} };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  return res.json();
}

function show(elId, data) {
  document.getElementById(elId).textContent = JSON.stringify(data, null, 2);
}

async function fetchAsset(bypass) {
  const id = document.getElementById('assetId').value;
  const d = await api('GET', `/asset/${id}?bypass_cache=${bypass}`);
  show('asset-out', d);
}

async function flushCache() {
  show('asset-out', await api('POST', '/flush-cache'));
}

async function createSession() {
  const val = JSON.parse(document.getElementById('sessOfficer').value);
  const d = await api('POST', '/session/create', val);
  lastSessionId = d.session_id;
  show('sess-out', d);
}

async function readSession() {
  if (!lastSessionId) { document.getElementById('sess-out').textContent = 'Create a session first!'; return; }
  show('sess-out', await api('GET', `/session/${lastSessionId}`));
}

async function publishCDC() {
  const asset_id  = document.getElementById('cdcAsset').value;
  const officer_id = document.getElementById('cdcOfficer').value;
  const d = await api('POST', '/cdc/publish', {
    asset_id, officer_id, change_type: 'EQUIPMENT_ASSIGNED',
    old_value: 'UNASSIGNED', new_value: officer_id
  });
  show('cdc-out', d);
}

async function readStream() {
  show('cdc-out', await api('GET', '/cdc/stream?count=5'));
}

async function refreshReport() {
  show('report-out', await api('POST', '/report/refresh'));
}

async function getReport() {
  show('report-out', await api('GET', '/report'));
}

async function searchOfficers() {
  const q        = document.getElementById('srchName').value;
  const station  = document.getElementById('srchStation').value;
  const division = document.getElementById('srchDiv').value;
  const params   = new URLSearchParams();
  if (q) params.set('q', q);
  if (station) params.set('station', station);
  if (division) params.set('division', division);
  show('officer-out', await api('GET', `/search/officers?${params}`));
}

async function searchAssets() {
  const q        = document.getElementById('srchAssetQ').value;
  const category = document.getElementById('srchCat').value;
  const status   = document.getElementById('srchAssetStatus').value;
  const params   = new URLSearchParams();
  if (q) params.set('q', q);
  if (category) params.set('category', category);
  if (status) params.set('status', status);
  show('asset-search-out', await api('GET', `/search/assets?${params}`));
}

async function assetsByStation() {
  show('asset-search-out', await api('GET', '/search/assets/by-station'));
}

async function getRedisInfo() {
  const d = await api('GET', '/redis-info');
  const el = document.getElementById('info-out');
  el.style.display = 'block';
  el.textContent = JSON.stringify(d, null, 2);
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    bootstrap_search_index()
    app.run(host="0.0.0.0", port=5000, debug=False)
