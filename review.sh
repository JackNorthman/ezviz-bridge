#!/usr/bin/env bash
# Summarise a day's doorbell events. Usage: ./review.sh [YYYY-MM-DD]
# Needs the container running as "ezviz-bridge" and the image "ezviz-bridge:local" (see compose.yaml).
cd "$(dirname "$0")"
DAY="${1:-$(date -u +%F)}"
echo "=== ezviz-bridge events for $DAY (UTC) ==="
if [[ -f data/events.jsonl ]]; then
  python3 - "$DAY" <<'PY'
import json, sys, collections, pathlib
day = sys.argv[1]; rows = []
for line in pathlib.Path("data/events.jsonl").read_text().splitlines():
    try: r = json.loads(line)
    except Exception: continue
    if str(r.get("time","")).startswith(day): rows.append(r)
if not rows:
    print("  no events recorded for that day"); raise SystemExit
c = collections.Counter(l for r in rows for l in r["labels"])
src = collections.Counter(r.get("source", "push") for r in rows)   # rows before 2026-09-12 have no source = push
print(f"  {len(rows)} events · " + ", ".join(f"{k}×{v}" for k, v in c.most_common())
      + " · via " + ", ".join(f"{k}×{v}" for k, v in src.most_common()))
for r in rows:
    conf = max((d["conf"] for d in r["detections"]), default=0)
    print(f'  {r["time"][11:19]}  {r.get("source", "push"):4s}  {",".join(r["labels"]):14s} {("%.2f"%conf) if conf else "    "}  {r.get("image") or "(no image)"}')
PY
else
  echo "  no events.jsonl yet"
fi
LOGS="$(docker logs ezviz-bridge --since 24h 2>&1)"
echo "=== battery / mode ==="
grep -oE 'battery=[0-9]+% work_mode=[a-z_0-9]+ detect=[0-9]+' <<<"$LOGS" | tail -1
echo "=== push channel health (last 24h) ==="
echo "  $(grep -c 'push channel' <<<"$LOGS") push-channel up/drop lines, $(grep -c 'push registration failed' <<<"$LOGS") failed registrations, $(grep -c 'push missed alarm' <<<"$LOGS") missed-alarm warnings"
grep -E 'event source: ' <<<"$LOGS" | tail -3 | sed 's/^/  /'
echo "=== cloud cross-check (every cloud alarm should have been handled: by push, or by the bridge's poll) ==="
docker run --rm --env-file .env -v "$PWD/data:/data:ro" \
  ezviz-bridge:local python /app/cloud_events.py "$DAY" 2>&1 | grep -viE 'password' || echo "  (cloud check failed)"
