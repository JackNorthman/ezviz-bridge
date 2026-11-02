#!/usr/bin/env python3
"""List today's (or a given day's) alarm messages straight from the EZVIZ cloud.

Used by review.sh as a cross-check: every cloud message should have a matching row in
data/events.jsonl — by push, or (since 2026-09-12) by the bridge's own cloud poll when push
missed it or was down. Anything unmatched was never handled by the bridge.
Usage inside the container: python /app/cloud_events.py [YYYY-MM-DD]   (⚠️ one EZVIZ login per run)
"""
import datetime as dt, json, os, sys
from pyezvizapi.client import EzvizClient

day = sys.argv[1] if len(sys.argv) > 1 else dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
cl = EzvizClient(os.environ["EZVIZ_ACCOUNT"], os.environ["EZVIZ_PASSWORD"], os.environ.get("EZVIZ_REGION", "apiieu.ezvizlife.com"))
cl.login()
serial = os.environ["EZVIZ_SERIAL"]
msgs, end_time = [], ""
for _ in range(10):
    r = cl.get_device_messages_list(serial, limit=20, end_time=end_time)
    batch = r.get("message") or []
    msgs += batch
    if not r.get("hasNext") or not batch:
        break
    end_time = batch[-1].get("time") or ""
    if not end_time:
        break

handled = {}   # msg_id -> "push" | "poll"
try:
    for line in open("/data/events.jsonl"):
        try:
            r = json.loads(line)
            if r.get("msg_id"):
                handled[r["msg_id"]] = r.get("source", "push")   # rows before 2026-09-12 have no source = push
        except Exception:
            pass
except FileNotFoundError:
    pass

# NOTE: the cloud's epoch `time` is LOCAL time encoded as if it were UTC (verified 2026-09-05:
# a 19:32:32 UTC push carried epoch 20:32:26 during BST). So we never compare times across systems —
# we match on msgId, which the push and the cloud message share. `timeStr` is the device-local string.
rows = []
for m in msgs:
    ms = m.get("time")
    if not ms:
        continue
    local = dt.datetime.fromtimestamp(int(ms) / 1000, dt.UTC)          # local-as-UTC → read as local
    if local.strftime("%Y-%m-%d") != day:
        continue
    mid = m.get("msgId", "")
    rows.append((local, m.get("title", "?"), mid, handled.get(mid)))
rows.sort()
by_push = sum(1 for r in rows if r[3] == "push")
by_poll = sum(1 for r in rows if r[3] == "poll")
print(f"  cloud: {len(rows)} alarm messages on {day} (device-local date); {by_push} arrived by push, "
      f"{by_poll} caught by the bridge's cloud poll, {len(rows) - by_push - by_poll} never handled")
print("  (alarms from before the bridge was first started, or while it was stopped, show as never handled)")
labels = {"push": "push ok", "poll": "via poll (push missed/down)"}
for local, title, mid, src in rows:
    print(f"    {local:%H:%M:%S} local  {title[:32]:32s} {mid[-16:]:16s} {labels.get(src, '-- never handled --')}")
