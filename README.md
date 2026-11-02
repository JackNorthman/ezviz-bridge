# ezviz-bridge

**EZVIZ battery doorbell → local MQTT / Home Assistant, with on-device-snapshot object detection.**

> Unofficial. This project is not affiliated with, endorsed by or supported by EZVIZ or Hikvision.
> It talks to the EZVIZ cloud through the community [`pyezvizapi`](https://pypi.org/project/pyezvizapi/)
> library, and EZVIZ can change or break that API at any time. Use it at your own risk.

## Why

EZVIZ battery doorbells such as the **HP3** have no local stream at all: no RTSP, no ONVIF, and they
sleep between PIR wakes. Even when wired to AC power the RTSP ports stay closed. So they can never be
an NVR/Frigate camera. What they *do* have is a cloud **push channel**, the same one that rings your
phone, plus a cloud alarm list with a snapshot for every alarm.

This bridge listens on that push channel, downloads (and decrypts) each alarm snapshot, runs
**YOLO11n** on it locally on the CPU, and publishes the result to your MQTT broker with
**Home Assistant MQTT discovery**. You get motion / person / cat / dog binary sensors, an annotated
snapshot image, battery and online state in HA without editing any HA configuration.

Because the EZVIZ push service is not always reliable, a **cloud cross-check** of the alarm list backs
push up, and fully replaces it while push is down ("poll-only mode").

It has been built and run against an HP3 only. Other EZVIZ battery cameras that use the same push
channel and alarm list will probably work, but are untested.

## Architecture

```
EZVIZ cloud push ──► bridge (pyezvizapi push client, keepalive 30, clean session)
EZVIZ alarm list ──►   │  cross-check: every 60 s in poll-only mode, every 5 min with push up; dedupe by msgId
                       │  download + decrypt the alarm snapshot
                       │  (no detections? optionally ask the device for a fresh live frame)
                       │  YOLO11n ONNX on CPU (~40-100 ms)
                       ▼
              your MQTT broker  ──► Home Assistant (MQTT discovery)
                       │
                       └─► ./data: events.jsonl + annotated JPEG per event (kept EVENT_RETENTION_DAYS)
```

- `app/bridge.py`: the service (push supervision, cross-check, event pipeline, HA discovery, health).
- `app/detector.py`: a small YOLO11 ONNX detector (letterbox, NMS, box drawing), numpy + onnxruntime only.
- `app/cloud_events.py`: one-off listing of a day's cloud alarms, matched against `events.jsonl`.
- `app/clip_frames.py`: runs the detector over frames extracted from a video clip.
- `app/tests/`: offline tests for the resilience logic (fakes only, no network).

## Quick start

Requirements: Docker with the compose plugin, an MQTT broker (e.g. Mosquitto) that Home Assistant
uses, your EZVIZ account login, and the doorbell's serial number.

```sh
git clone https://github.com/JackNorthman/ezviz-bridge.git
cd ezviz-bridge
cp .env.example .env && chmod 600 .env      # then edit it
# export models/yolo11n.onnx (see "The model" below)
docker compose up -d --build
docker logs -f ezviz-bridge
```

With `MQTT_PASS` empty the bridge runs in **DRY RUN**: it logs everything it would publish and
publishes nothing. That is a good first run to check the EZVIZ side works.

### MQTT user

Give the bridge its own broker user. With Mosquitto in Docker, for example:

```sh
docker exec mosquitto mosquitto_passwd -b /mosquitto/config/passwordfile ezviz-bridge '<password>'
docker kill -s HUP mosquitto
```

If your broker uses an ACL file, allow the user to write `ezviz-bridge/#` (or your `TOPIC_BASE`) and
`homeassistant/#` (or your `DISCOVERY_PREFIX`). Put the password in `.env` as `MQTT_PASS`.

### Snapshot decryption

Alarm pictures are usually encrypted. `EZVIZ_VERIFY_CODE` must be the **device encryption password**:
that is the 6-letter verification code on the device label *only if you have never changed it* in the
app. If you set a custom encryption password in the EZVIZ app, use that instead. Without the right
code you still get motion and `last_alert`, but encrypted snapshots cannot be classified (the log says
`snapshot fetch/decrypt failed`).

## Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `EZVIZ_ACCOUNT` | *(required)* | EZVIZ account e-mail / user name |
| `EZVIZ_PASSWORD` | *(required)* | EZVIZ account password |
| `EZVIZ_SERIAL` | *(required)* | Device serial (EZVIZ app → device information, or the label) |
| `EZVIZ_REGION` | `apiieu.ezvizlife.com` | EZVIZ API host for your account's region |
| `EZVIZ_VERIFY_CODE` | *(empty)* | Device encryption password, for snapshot decryption (see above) |
| `MQTT_HOST` | `localhost` | Broker host. Inside Docker, `localhost` is the container: use the broker's LAN address or service name |
| `MQTT_PORT` | `1883` | Broker port |
| `MQTT_USER` | *(empty)* | Broker user |
| `MQTT_PASS` | *(empty)* | Broker password. **Empty = DRY RUN** |
| `DISCOVERY_PREFIX` | `homeassistant` | HA MQTT discovery prefix |
| `TOPIC_BASE` | `ezviz-bridge/<serial>` | Base for all state topics |
| `DEVICE_NAME` | `EZVIZ doorbell` | HA device name; also the entity ID prefix (`ezviz_doorbell_motion`, …) |
| `DEVICE_MODEL` | `HP3` | Model text on the HA device card |
| `DETECTION_MODE` | `leave` | Doorbell-side detection mode to set at startup: `human`, `image_change`, `pir` or `leave` (see gotchas) |
| `DETECT_CONF` | `0.40` | Minimum YOLO confidence |
| `LABELS` | `person,cat,dog` | COCO labels that get their own binary sensor |
| `HOLD_SECONDS` | `90` | How long motion / label sensors stay ON after a hit |
| `POLL_SECONDS` | `600` | Battery / online / work-mode status poll interval |
| `KEEPALIVE` | `30` | Push-channel MQTT keepalive (see gotchas before raising it) |
| `LIVE_CAPTURE_FALLBACK` | `1` | `0` disables the live re-capture when the alarm frame has no detections |
| `LIVE_CAPTURE_MAX_PER_HOUR` | `8` | Cap on device wakes caused by live re-captures |
| `EVENT_RETENTION_DAYS` | `7` | Days of per-event JPEGs to keep under `data/events/` |
| `CLOUD_CHECK_POLL_ONLY_SECONDS` | `60` | Cross-check interval while push is down |
| `CLOUD_CHECK_PUSH_SECONDS` | `300` | Cross-check interval while push is up |
| `PUSH_MISS_GRACE_SECONDS` | `120` | An alarm listed in the cloud but not pushed within this is a "missed" alarm |
| `KUMA_PUSH_URL` | *(empty)* | Optional Uptime Kuma push-monitor URL (see below) |
| `MODEL_PATH` | `/models/yolo11n.onnx` | Path to the ONNX model inside the container |
| `DATA_DIR` | `/data` | Event archive directory inside the container |
| `LOG_LEVEL` | `INFO` | Python log level |

## Home Assistant entities

All entities belong to one device named `DEVICE_NAME`, and the bridge publishes an availability
topic, so they go unavailable when it stops.

| Entity | Type | Notes |
|---|---|---|
| Motion | binary_sensor (motion) | ON for `HOLD_SECONDS` after any alarm |
| Person / Cat / Dog (one per `LABELS` entry) | binary_sensor | ON for `HOLD_SECONDS` after YOLO finds that label |
| Online | binary_sensor (connectivity, diagnostic) | Device online according to the cloud |
| Push channel | binary_sensor (connectivity, diagnostic) | Is the EZVIZ push channel up? |
| Battery | sensor (%) | |
| Work mode | sensor | `power_save`, `high_performance`, `plugged_in`, `super_power_save`, `custom`, `standard`, `always_on_video` |
| Detection mode | sensor | `human`, `image_change`, `pir`, or the raw value |
| Last alert | sensor + attributes | Alert name; attributes `code`, `time`, `msg_id`, `source` (`push`/`poll`), `encrypted` |
| Last detection | sensor + attributes | Top label (or `none` / `unknown`); attributes hold every box and confidence |
| Last event | sensor (timestamp) | When the bridge handled the last alarm (UTC) |
| Event source | sensor + attributes | `push`, `poll-only` or `offline`, with push health attributes |
| Last cloud check | sensor (timestamp) | Last successful cloud cross-check |
| Snapshot | image | The alarm frame with boxes drawn on it |

### MQTT topics

Discovery configs go to `<DISCOVERY_PREFIX>/<component>/ezviz_<serial>/<key>/config` (retained).
State topics, under `TOPIC_BASE` (default `ezviz-bridge/<serial>`):

```
status                      online / offline (LWT, retained)
motion, person, cat, dog    ON / OFF
online, push_channel        ON / OFF (retained)
battery, work_mode, detection_mode        (retained)
last_alert, last_alert/attributes         (retained, attributes = JSON)
detection, detection/attributes           (retained)
last_event_time, last_cloud_check         ISO 8601 UTC (retained)
event_source, event_source/attributes     (retained)
snapshot                    JPEG bytes (retained)
```

## Event archive

`./data` (mounted at `/data`) gets:

- `events.jsonl`: one JSON row per alarm (`time`, `msg_id`, `source`, `alert`, `code`, `labels`,
  `detections`, `image`),
- `events/YYYY-MM-DD/HHMMSS_<labels>.jpg`: the annotated frame for each event, pruned after
  `EVENT_RETENTION_DAYS`,
- `last.jpg` / `last.json`: the most recent event.

These are pictures of whatever your doorbell sees. Treat `data/` as private (it is in `.gitignore`).

## The model

`models/yolo11n.onnx` is not in this repository (about 10 MB, and it comes under the Ultralytics
licence). Export it once from the official `yolo11n.pt` with Ultralytics, in a throwaway container so
nothing lands on the host:

```sh
mkdir -p models
docker run --rm -v "$PWD/models:/out" python:3.12-slim sh -c '
  apt-get update -qq && apt-get install -y -qq --no-install-recommends libgl1 libglib2.0-0 libxcb1 >/dev/null &&
  pip install -q --extra-index-url https://download.pytorch.org/whl/cpu ultralytics opencv-python-headless onnx onnxslim &&
  cd /tmp && yolo export model=yolo11n.pt format=onnx imgsz=640 opset=12 simplify=True &&
  cp yolo11n.onnx /out/'
```

The `libgl1 libglib2.0-0 libxcb1` packages and `opencv-python-headless` matter: without them the export
dies importing OpenCV with `libxcb.so.1: cannot open shared object file`. The detector expects the
default static export: input 1×3×640×640, output `(1, 84, 8400)` (4 box values + 80 COCO class scores,
no separate objectness).

## Uptime Kuma heartbeat (optional)

Set `KUMA_PUSH_URL` to a Kuma **push** monitor URL (copying it with the `?status=up&msg=OK&ping=`
suffix is fine; those parameters are replaced). Every 60 s the bridge sends a heartbeat from its main
loop, so a wedged bridge also stops heartbeating. Suggested monitor heartbeat interval: 120 s or more.

The status means "can the bridge do its job", not "is push up". Poll-only is a working mode, and a
monitor stuck DOWN through a multi-day EZVIZ push outage would mean nobody notices when the bridge
really breaks.

- `status=up`: logged in and the cloud answers. `msg=push ok`, or
  `msg=poll-only: push down since <UTC>, retry in Nm` (`…, connecting` while a registration waits for
  the broker; `… (N days)` once push has been down for more than 7 days, still `up`). "Since" counts
  from the bridge's own start, not from when the EZVIZ outage began.
- `status=down`: `EZVIZ login failing since <UTC>, retry in Nm`, or
  `EZVIZ cloud failing: <problem>, last OK <UTC>; <mode>`, where the problem is 3 or more cross-checks
  or status polls failing in a row, or no successful login/poll/cross-check for more than 10 minutes
  (11 with push up, where cross-checks are 5 minutes apart). Isolated `RemoteDisconnected` blips on
  the cross-check are routine and never flip it.

The container must be able to reach Kuma. If Kuma runs in Docker on the same host and is published on
`127.0.0.1` only, join its network (commented example in `compose.yaml`) and use
`http://uptime-kuma:3001/api/push/<token>`.

## Tools

- `./review.sh [YYYY-MM-DD]`: a day's events (with push/poll source), the last battery line, push health
  from the container log, and a cloud cross-check via `app/cloud_events.py`. That last part does one
  EZVIZ login per run.
- `./classify_clip.sh <clip> [fps] [conf]`: YOLO over every frame of an exported video clip; writes the
  best annotated frame per label to `data/clips/<name>/out`. Needs `ffmpeg` on the host.
- Tests (offline, fakes only, no network needed), using the built image:

  ```sh
  docker compose build
  docker run --rm --network none -v "$PWD/app:/app:ro" --entrypoint python ezviz-bridge:local \
      -m unittest discover -s /app/tests -v
  ```

## Gotchas (learned the hard way)

**Push channel**

- `pyezvizapi`'s default `clean_session=False` silently never subscribes, so the bridge forces
  `clean_session=True`.
- The EZVIZ push broker drops `keepalive=60` clients at exactly 60 s, hence `KEEPALIVE=30`.
- A paho auto-reconnect of the push client does **not** restore delivery on the EZVIZ side: pushes can
  silently stop for days while the MQTT connection looks fine. On any drop (or when the cross-check finds
  an alarm push never delivered) the old client is torn down completely (paho stopped, EZVIZ `push/stop`)
  and a **fresh registration** with a new client id is made: immediately if the channel had been stable
  for 10 minutes, otherwise on the backoff schedule.
- The push registration endpoint (`pusheu.ezvizlife.com/v1/getClientId`) can return
  **503 "Back-end server is at capacity"** for days. Exiting on that and letting Docker restart the
  container meant roughly 60 restarts an hour, each one a full EZVIZ account login. So a registration
  failure never exits: the bridge stays up in poll-only mode and retries after 5 → 10 → 20 → 30 minutes
  (reset once push has been connected for 10 minutes). MQTT and HA availability stay up throughout.
- A second push client on the account *might* affect the phone app's own notifications. After setting
  this up, press the doorbell and check your phone still rings.

**Cloud cross-check**

- `get_device_messages_list` is checked every 60 s in poll-only mode and every 5 minutes with push up.
  New msgIds go through exactly the same snapshot/classify/publish/archive pipeline as a push (log line
  `POLL <msgId> …`, `source=poll`).
- Startup seeds the seen-set from the current list, so nothing old is replayed. An alarm that reappears
  but is older than ones already seen (say, after newer ones were deleted in the app) is skipped too.
- With push up, an alarm not pushed within `PUSH_MISS_GRACE_SECONDS` logs **`push missed alarm <msgId>`**,
  is handled from the list without a live re-capture (it is minutes old, so a live frame would show
  something unrelated), and push is re-registered.
- EZVIZ cloud epoch times are the **device's local time encoded as if it were UTC** (during BST they read
  an hour ahead). Never compare them with push or UTC times: matching is by **msgId only**. Cloud times
  are only compared with other cloud times.
- Cloud list picture URLs are re-signed on every call, so never compare URLs across calls.

**Snapshots**

- The HP3's pushes carry no media URL at all. After a push, `pic_from_cloud()` polls the device record's
  `last_alarm_pic` for a few seconds until a new URL appears.
- Encrypted pictures start with `hikencodepicture`; they are decrypted with `pyezvizapi.utils.decrypt_image`
  and `EZVIZ_VERIFY_CODE`. The 32 hex characters after that header are `md5(md5(password))`, so a single
  md5 of your code will never match them (handy if you are checking which password is right).
- `get_cam_key()` (fetching the encryption code from the cloud) needs an MFA code, so the bridge never
  calls it. Supply the code yourself.
- The stored alarm frame is taken at wake time and often misses a subject who kept moving. When it has
  no detections the bridge asks the device for a live frame (`capture_picture`, about 7 s) and tries that
  too. Each one wakes the doorbell, hence `LIVE_CAPTURE_MAX_PER_HOUR`.

**Device settings**

- `DETECTION_MODE=leave` is the safe default: the bridge never changes the setting you chose in the app.
  On the HP3, **never use `pir`** (mode 5): it stopped alarms reaching the cloud entirely. `human` and
  `image_change` set through the API also proved unreliable; change detection settings in the EZVIZ app
  instead.
- Work mode value 5 is "Standard" in the EZVIZ app on the HP3, not "hibernate" as `pyezvizapi`'s enum
  says. The bridge maps it to `standard`.
- Alert code 10120 is "AI Human Detection". Other codes (a doorbell press shows up in the cloud list as
  "Your doorbell is ringing") are logged in full as `non-motion … ext:` so they can be wired up later.

**Logins**

- EZVIZ login failures never exit either: retry after 1 → 2 → 5 → 10 → 30 minutes; wrong password, account
  lock or MFA errors retry every 6 hours, because fast retries only make those worse (and `pyezvizapi` makes
  EZVIZ e-mail an MFA code on every login that hits MFA).
- The only deliberate exit is code 2: required environment variables missing.
- `docker compose up --force-recreate` discards the old container's logs; save them first if you need them.

## Licence

MIT, see [LICENSE](LICENSE). The YOLO11 weights you export are covered by the Ultralytics licence
(AGPL-3.0 or an Ultralytics enterprise licence), not by this one.
