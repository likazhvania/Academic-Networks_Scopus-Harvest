#!/usr/bin/env python3
"""
harvest_scopus.py  –  Incremental Scopus downloader
"""

import os, time, json, gzip, datetime, requests
from pathlib import Path
from collections import deque
from dotenv import load_dotenv          # pip install python-dotenv
# from ratelimit import limits, sleep_and_retry  # alt. 3rd‑party limiter

# ─────────────────── Load secrets ────────────────────────────────────
load_dotenv()                             # reads .env
API_KEY = os.getenv("SCOPUS_API_KEY")
if not API_KEY:
    raise RuntimeError("SCOPUS_API_KEY not found. Create a .env file.")

# ─────────────────── Config ──────────────────────────────────────────
MAX_REQS_PER_RUN = 40_000        # Mon‑40k, Tue‑30k, Thu‑30k
CHUNK_SIZE_REQS  = 2_000         # 2 000 requests → 50k docs
DATE_RANGE       = "2000-2024"
BASE_URL         = "https://api.elsevier.com/content/search/scopus"

OUT_RAW_DIR      = Path("Data/raw")
OUT_RAW_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE       = Path("cursor_state.json")   # persists cursor + counter
HEADERS          = {"X-ELS-APIKey": API_KEY, "Accept": "application/json"}

# ─────────────────── Resume state ────────────────────────────────────
if STATE_FILE.exists():
    state         = json.loads(STATE_FILE.read_text())
    cursor_value  = state["cursor"]
    chunk_counter = state["chunk_counter"]
else:
    cursor_value  = "*"
    chunk_counter = 0

# ─────────────────── Helper: write one raw .jsonl.gz chunk ───────────
def flush_chunk(records, counter):
    today = datetime.datetime.now().strftime("%Y%m%d")
    path  = OUT_RAW_DIR / f"scopus_raw_{counter:06d}_{today}.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as gz:
        for rec in records:
            gz.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"✓ wrote {len(records):,} docs → {path.name}")

# ─────────────────── Simple 9‑req/s token‑bucket limiter ─────────────
MAX_RPS   = 9
WINDOW    = 1.0  # seconds
recent_ts = deque()

def rate_limited_get(session, params):
    while True:
        now = time.perf_counter()
        while recent_ts and now - recent_ts[0] >= WINDOW:
            recent_ts.popleft()
        if len(recent_ts) < MAX_RPS:
            break
        time.sleep(WINDOW - (now - recent_ts[0]) + 0.001)

    resp = session.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
    recent_ts.append(time.perf_counter())
    return resp

# ─────────────────── Query skeleton ──────────────────────────────────
query = {
    "query" : "DOCTYPE(ar)",
    "date"  : DATE_RANGE,
    "sort"  : "-coverDate",
    "count" : 25,
    "cursor": cursor_value,
    "view"  : "COMPLETE",
}

# ─────────────────── Main loop ───────────────────────────────────────
session        = requests.Session()
records_buffer = []
requests_done  = 0
t_start        = time.perf_counter()

while True:
    # retry up to 5 times on network errors
    for attempt in range(5):
        try:
            resp = rate_limited_get(session, query)
            break
        except requests.exceptions.RequestException as e:
            print(f"⚠ network error {attempt+1}/5 – {e}")
            time.sleep(2 + attempt)
    else:
        print("🚫 network failed 5 times; aborting.")
        break

    if resp.status_code != 200:
        print(f"🚫 HTTP {resp.status_code} – {resp.text[:200]}")
        break

    data    = resp.json()
    entries = data.get("search-results", {}).get("entry", [])
    if not entries:
        print("• no more entries – dataset complete.")
        break

    records_buffer.extend(entries)
    requests_done += 1

    # pagination
    next_cursor = data["search-results"]["cursor"].get("@next")
    if not next_cursor:
        print("• reached end – no @next cursor.")
        break
    query["cursor"] = next_cursor

    # chunk flush
    if requests_done % CHUNK_SIZE_REQS == 0:
        chunk_counter += 1
        flush_chunk(records_buffer, chunk_counter)
        records_buffer = []

    # per‑run quota
    if requests_done >= MAX_REQS_PER_RUN:
        last_date = entries[-1].get("prism:coverDate")
        print(f"• hit {MAX_REQS_PER_RUN} requests; pausing. Last coverDate: {last_date}")
        break

# flush leftovers
if records_buffer:
    chunk_counter += 1
    flush_chunk(records_buffer, chunk_counter)

# persist state
STATE_FILE.write_text(json.dumps({"cursor": query["cursor"],
                                  "chunk_counter": chunk_counter}))
elapsed = time.perf_counter() - t_start
print(f"Done. Requests this run: {requests_done:,}  –  elapsed {elapsed:0.1f}s")
