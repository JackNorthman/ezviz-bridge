#!/usr/bin/env python3
"""EZVIZ HP3 doorbell → local MQTT bridge with on-snapshot object detection.

Why: the HP3 is a battery, cloud-only doorbell (no RTSP/ONVIF, sleeps between PIR
wakes) so it can never be a Frigate camera. What it *does* have is a push channel —
the same one that rings the phone. This process:

  1. logs in to EZVIZ, registers as a push client and listens for alerts,
  2. on each alert for our serial: downloads + decrypts the alarm snapshot,
     runs YOLO11n on it (person / cat / dog / ...),
  3. publishes the result to mosquitto with Home Assistant MQTT discovery, so HA
     gets motion / person / cat / dog binary sensors, a snapshot image entity,
     battery, online state and last-alert info — no HA config changes needed,
  4. polls the cloud every POLL_SECONDS for battery/online/work-mode,
  5. cross-checks the cloud alarm list (every CLOUD_CHECK_POLL_ONLY_SECONDS while push is
     down, every CLOUD_CHECK_PUSH_SECONDS while it is up). Alarms that push did not deliver
     go through the same pipeline with source=poll, so the bridge keeps working without push.

Push resilience (2026-09-12, after the 09-06..09-11 outage):
  * a paho auto-reconnect does NOT restore EZVIZ-side delivery → on any drop, or when the
    cross-check finds an alarm push never delivered, the push client is torn down and
    registered from scratch (fresh getClientId);
  * a failed registration (e.g. pusheu 503 "Back-end server is at capacity") never exits:
    the bridge stays up in poll-only mode and retries after 5 → 10 → 20 → 30 min;
  * EZVIZ login failures never exit either (exit + docker restart = one account login per loop;
    that did ~1,600 logins on 09-10/11).

Secrets come from the environment only (compose env_file). Never log them.
Exit code: 2 = config error (missing env). Nothing else exits on purpose.
"""
from __future__ import annotations

import io
import json
import logging
import math
import os
import queue
import signal
import sys
import threading
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import paho.mqtt.client as mqtt
from PIL import Image
from pyezvizapi.client import EzvizClient
from pyezvizapi.exceptions import EzvizAuthVerificationCode, PyEzvizError
from pyezvizapi.mqtt import MQTTClient
from pyezvizapi.utils import decrypt_image

from detector import Detector, annotate

# ----------------------------------------------------------------------------- config
def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()

LOG = logging.getLogger("ezviz-bridge")
logging.basicConfig(level=getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO),
                    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")
logging.getLogger("pyezvizapi").setLevel(logging.WARNING)

ACCOUNT, PASSWORD = env("EZVIZ_ACCOUNT"), env("EZVIZ_PASSWORD")
REGION = env("EZVIZ_REGION", "apiieu.ezvizlife.com")
SERIAL = env("EZVIZ_SERIAL")
VERIFY_CODE = env("EZVIZ_VERIFY_CODE")          # 6-char code on the doorbell label (snapshot decryption)
DEVICE_NAME = env("DEVICE_NAME", "EZVIZ doorbell")
DEVICE_MODEL = env("DEVICE_MODEL", "HP3")         # shown in the HA device card only

MQTT_HOST, MQTT_PORT = env("MQTT_HOST", "localhost"), int(env("MQTT_PORT", "1883") or 1883)
MQTT_USER, MQTT_PASS = env("MQTT_USER"), env("MQTT_PASS")
DISCOVERY_PREFIX = env("DISCOVERY_PREFIX", "homeassistant")
BASE = env("TOPIC_BASE") or f"ezviz-bridge/{SERIAL}"

DETECTION_MODE = env("DETECTION_MODE", "leave").lower()   # human | pir | image_change | leave
DETECT_CONF = float(env("DETECT_CONF", "0.40") or 0.4)
LABELS = [l for l in env("LABELS", "person,cat,dog").split(",") if l]
HOLD_SECONDS = int(env("HOLD_SECONDS", "90") or 90)
POLL_SECONDS = int(env("POLL_SECONDS", "600") or 600)
KEEPALIVE = int(env("KEEPALIVE", "30") or 30)
# The stored alarm snapshot is grabbed at wake time and often misses a subject who kept
# moving (proven 2026-09-05: doorbell fired, saved frame showed an empty scene). When the
# alarm frame yields no detections, ask the device for a fresh live frame (~7 s) and try
# that too. Costs a device wake, which is fine on a solar panel or wired power. Set 0 to disable.
LIVE_CAPTURE_FALLBACK = env("LIVE_CAPTURE_FALLBACK", "1") != "0"
# Keep every event (image + a jsonl line) so a night's activity can be reviewed later.
# last.jpg/last.json are still overwritten for the "most recent" view.
EVENT_RETENTION_DAYS = int(env("EVENT_RETENTION_DAYS", "7") or 7)
# Each live re-capture wakes the doorbell. On battery/solar there is no charging overnight, and a windy night
# with high wake sensitivity could trigger constantly — so cap the wakes we cause.
LIVE_CAPTURE_MAX_PER_HOUR = int(env("LIVE_CAPTURE_MAX_PER_HOUR", "8") or 8)
MODEL_PATH = env("MODEL_PATH", "/models/yolo11n.onnx")
DATA_DIR = Path(env("DATA_DIR", "/data"))

# Cloud cross-check. In poll-only mode it is the only event source, so it runs every minute; with
# push up it is a safety net that catches a silently deaf push channel (the 09-06 failure mode).
CLOUD_CHECK_POLL_ONLY_SECONDS = int(env("CLOUD_CHECK_POLL_ONLY_SECONDS", "60") or 60)
CLOUD_CHECK_PUSH_SECONDS = int(env("CLOUD_CHECK_PUSH_SECONDS", "300") or 300)
PUSH_MISS_GRACE_SECONDS = int(env("PUSH_MISS_GRACE_SECONDS", "120") or 120)
# Uptime Kuma push-monitor URL; empty = disabled. Any status/msg/ping query params are replaced.
KUMA_PUSH_URL = env("KUMA_PUSH_URL")
KUMA_INTERVAL_SECONDS = 60
# Kuma status (2026-09-13): `up` whenever the bridge can do its job — logged in and talking to the EZVIZ cloud — even
# with push down: poll-only is a working mode, and a monitor stuck DOWN through a multi-day push outage pages nobody
# when the bridge really breaks. `down` = login failing, or cloud contact failing: KUMA_CLOUD_FAIL_COUNT consecutive
# failed cross-checks or status polls (isolated RemoteDisconnected blips are normal), or no successful login / poll /
# cross-check for longer than KUMA_CLOUD_STALE_SECONDS.
KUMA_CLOUD_FAIL_COUNT = 3
KUMA_CLOUD_STALE_SECONDS = 10 * 60
KUMA_PUSH_LONG_DOWN_SECONDS = 7 * 86400    # push down longer than this is said in the msg (status unaffected)
PUSH_BACKOFF_SECONDS = (300, 600, 1200, 1800)
PUSH_STABLE_SECONDS = 600                  # connected this long counts as success → backoff reset
PUSH_CONNECT_TIMEOUT = 60                  # registered but no CONNACK within this = failed attempt
LOGIN_BACKOFF_SECONDS = (60, 120, 300, 600, 1800)
LOGIN_AUTH_RETRY_SECONDS = 6 * 3600        # wrong password / locked / MFA: fast retries only make it worse
CLOUD_LIST_LIMIT = 20
CLOUD_REORDER_SLACK_MS = 10 * 60 * 1000    # cloud-vs-cloud ordering slack; never compared with UTC/push times

DETECT_MODE_VALUES = {"human": 1, "image_change": 3, "pir": 5}
ALERT_NAMES = {10120: "AI Human Detection"}   # learned from the device; others fall back to "Alert <code>"

DRY_RUN = not MQTT_PASS
if not (ACCOUNT and PASSWORD and SERIAL):
    LOG.error("EZVIZ_ACCOUNT, EZVIZ_PASSWORD and EZVIZ_SERIAL are required"); sys.exit(2)


# ----------------------------------------------------------------------------- small helpers
def utc_iso(ts: float | None) -> str | None:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds") if ts else None


def short_exc(exc: BaseException) -> str:
    """pyezvizapi raises HTTPError *from* the requests error with no text of its own."""
    msg = str(exc) or str(exc.__cause__ or "")
    return f"{type(exc).__name__}: {msg[:200]}" if msg else type(exc).__name__


class Backoff:
    """Retry delays: steps[0], steps[1], ... then the last step forever, until reset()."""

    def __init__(self, steps: tuple[int, ...]) -> None:
        self.steps, self.failures = tuple(steps), 0

    def fail(self) -> int:
        self.failures += 1
        return self.steps[min(self.failures, len(self.steps)) - 1]

    def reset(self) -> None:
        self.failures = 0


class SeenIds:
    """msgIds already handled (or deliberately skipped). Bounded, insertion-ordered.

    Deliberately NOT pruned by age: an id that is still on the cloud's newest-N page must
    stay here, or the next cross-check would replay it as a new alarm.
    """

    def __init__(self, cap: int = 5000) -> None:
        self._d: OrderedDict[str, str] = OrderedDict()
        self.cap = cap

    def add(self, msg_id: str, source: str) -> None:
        if msg_id and msg_id not in self._d:
            self._d[msg_id] = source
            while len(self._d) > self.cap:
                self._d.popitem(last=False)

    def source(self, msg_id: str) -> str | None:
        return self._d.get(msg_id)

    def __contains__(self, msg_id: object) -> bool:
        return msg_id in self._d

    def __len__(self) -> int:
        return len(self._d)


def msg_time(m: dict) -> int:
    try:
        return int(m.get("time") or 0)
    except (TypeError, ValueError):
        return 0


def message_pic(m: dict) -> str:
    """Snapshot URL of a unified cloud alarm message (pyezvizapi camera.py precedence, minus the
    generic `defaultPic` placeholder). Verified 2026-09-12: the URL is re-signed on every list call."""
    ext = m.get("ext") if isinstance(m.get("ext"), dict) else {}
    pics = ext.get("pics")
    multi = next((p for p in pics.split(";") if p), None) if isinstance(pics, str) else None
    for cand in (m.get("pic"), multi):
        if isinstance(cand, str) and cand.startswith("http"):
            return cand
    return ""


def cloud_to_push(m: dict) -> dict:
    """Shape a cloud alarm message like a decoded push so handle() treats both the same.

    Verified 2026-09-12 against the live list: cloud `msgId` == push `ext.msgId`, cloud
    `ext.alarmType` == push `alert_type_code` (10120), `timeStr` is device-LOCAL time.
    """
    ext = m.get("ext") if isinstance(m.get("ext"), dict) else {}
    return {"ext": {"msgId": str(m.get("msgId") or ""), "device_serial": m.get("deviceSerial"),
                    "alert_type_code": ext.get("alarmType"), "time": m.get("timeStr"),
                    "default_pic_url": message_pic(m), "device_name": m.get("from"),
                    "is_encrypted": m.get("picCrypt")},
            "title": m.get("title")}


def kuma_url(base: str, status: str, msg: str) -> str:
    parts = urlsplit(base)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in ("status", "msg", "ping")]
    query += [("status", status), ("msg", msg), ("ping", "")]
    return urlunsplit(parts._replace(query=urlencode(query)))


class PushClient(MQTTClient):
    """pyezvizapi's push client plus connection bookkeeping for Bridge.supervise_push().

    paho calls these on its network thread, so they only record facts; every decision
    (tear down, re-register, back off) is taken on the main thread.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connected_at: float | None = None
        self.dropped_at: float | None = None
        self.drop_rc = None
        self.closing = False

    def _on_connect(self, client, userdata, flags, rc, *rest) -> None:
        super()._on_connect(client, userdata, flags, rc)
        if rc == 0 and not self.closing and self.connected_at is None:
            self.connected_at = time.time()

    def _on_disconnect(self, client, userdata, rc, *rest) -> None:
        super()._on_disconnect(client, userdata, rc)
        if not self.closing and self.dropped_at is None:
            self.dropped_at, self.drop_rc = time.time(), rc


# ----------------------------------------------------------------------------- MQTT (local)
class LocalMqtt:
    """Publisher to mosquitto with HA discovery. In DRY_RUN it only logs."""

    def __init__(self, on_ready=None) -> None:
        self.ok = False
        self.on_ready = on_ready
        if DRY_RUN:
            LOG.warning("MQTT_PASS not set → DRY RUN: nothing will be published to the broker")
            return
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"ezviz-bridge-{SERIAL}")
        self.c.username_pw_set(MQTT_USER, MQTT_PASS)
        self.c.will_set(f"{BASE}/status", "offline", qos=1, retain=True)
        self.c.on_connect = self._on_connect
        self.c.on_disconnect = lambda *_: LOG.warning("local MQTT disconnected")
        self.c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.c.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if getattr(reason_code, "is_failure", False) or (isinstance(reason_code, int) and reason_code != 0):
            self.ok = False
            LOG.error("local MQTT connection REFUSED by %s:%s: %s — check MQTT_USER/MQTT_PASS in .env",
                      MQTT_HOST, MQTT_PORT, reason_code)
            return
        LOG.info("local MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
        self.ok = True
        self.discovery()
        client.publish(f"{BASE}/status", "online", qos=1, retain=True)
        if self.on_ready:
            self.on_ready()

    def pub(self, topic: str, payload, retain: bool = False) -> None:
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, default=str)
        if DRY_RUN:
            shown = f"<{len(payload)} bytes>" if isinstance(payload, (bytes, bytearray)) else str(payload)[:200]
            LOG.info("DRY-RUN publish %s = %s", topic, shown)
            return
        self.c.publish(topic, payload, qos=1, retain=retain)

    # --- Home Assistant discovery -------------------------------------------------
    def discovery(self) -> None:
        dev = {"identifiers": [f"ezviz_{SERIAL}"], "name": DEVICE_NAME, "manufacturer": "EZVIZ",
               "model": f"{DEVICE_MODEL} (via ezviz-bridge)", "serial_number": SERIAL}
        avail = {"availability_topic": f"{BASE}/status", "payload_available": "online",
                 "payload_not_available": "offline"}
        slug = DEVICE_NAME.lower().replace(" ", "_")

        def cfg(component: str, key: str, name: str, extra: dict) -> None:
            body = {"name": name, "unique_id": f"ezviz_{SERIAL}_{key}", "object_id": f"{slug}_{key}",
                    "device": dev, **avail, **extra}
            self.pub(f"{DISCOVERY_PREFIX}/{component}/ezviz_{SERIAL}/{key}/config", body, retain=True)

        cfg("binary_sensor", "motion", "Motion", {"state_topic": f"{BASE}/motion", "device_class": "motion"})
        icons = {"person": "mdi:walk", "cat": "mdi:cat", "dog": "mdi:dog", "bird": "mdi:bird"}
        for label in LABELS:
            extra = {"state_topic": f"{BASE}/{label}", "icon": icons.get(label, "mdi:eye")}
            if label == "person":
                extra["device_class"] = "occupancy"
            cfg("binary_sensor", label, label.capitalize(), extra)
        cfg("binary_sensor", "online", "Online", {"state_topic": f"{BASE}/online", "device_class": "connectivity",
                                                  "entity_category": "diagnostic"})
        cfg("sensor", "battery", "Battery", {"state_topic": f"{BASE}/battery", "device_class": "battery",
                                             "unit_of_measurement": "%", "state_class": "measurement",
                                             "entity_category": "diagnostic"})
        cfg("sensor", "work_mode", "Work mode", {"state_topic": f"{BASE}/work_mode", "icon": "mdi:battery-clock",
                                                 "entity_category": "diagnostic"})
        cfg("sensor", "detection_mode", "Detection mode", {"state_topic": f"{BASE}/detection_mode",
                                                           "icon": "mdi:motion-sensor", "entity_category": "diagnostic"})
        cfg("sensor", "last_alert", "Last alert", {"state_topic": f"{BASE}/last_alert",
                                                   "json_attributes_topic": f"{BASE}/last_alert/attributes",
                                                   "icon": "mdi:bell-alert"})
        cfg("sensor", "last_detection", "Last detection", {"state_topic": f"{BASE}/detection",
                                                           "json_attributes_topic": f"{BASE}/detection/attributes",
                                                           "icon": "mdi:image-search"})
        cfg("sensor", "last_event_time", "Last event", {"state_topic": f"{BASE}/last_event_time",
                                                        "device_class": "timestamp"})
        cfg("image", "snapshot", "Snapshot", {"image_topic": f"{BASE}/snapshot", "content_type": "image/jpeg"})
        # bridge health (2026-09-12): is the EZVIZ push channel up, or are events coming from cloud polling?
        cfg("binary_sensor", "push_channel", "Push channel", {"state_topic": f"{BASE}/push_channel",
                                                              "device_class": "connectivity",
                                                              "entity_category": "diagnostic"})
        cfg("sensor", "event_source", "Event source", {"state_topic": f"{BASE}/event_source",
                                                       "json_attributes_topic": f"{BASE}/event_source/attributes",
                                                       "icon": "mdi:swap-horizontal", "entity_category": "diagnostic"})
        cfg("sensor", "last_cloud_check", "Last cloud check", {"state_topic": f"{BASE}/last_cloud_check",
                                                               "device_class": "timestamp",
                                                               "entity_category": "diagnostic"})
        LOG.info("HA discovery published under %s/*/ezviz_%s/", DISCOVERY_PREFIX, SERIAL)


# ----------------------------------------------------------------------------- bridge
class Bridge:
    def __init__(self, client=None, mq=None, det=None, push_factory=None, clock=time.time, kuma_get=None) -> None:
        """State only — network work starts in start(). Tests inject fakes through the arguments."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.q: queue.Queue[tuple[str, dict]] = queue.Queue()   # (source, decoded)
        self.holds: dict[str, float] = {}          # topic -> expiry epoch
        self.seen = SeenIds()                      # msgId -> how we first met it (push/poll/seed/stale)
        self.lock = threading.Lock()               # guards pushed + last_push_at (written on paho's thread)
        self.pushed: OrderedDict[str, float] = OrderedDict()   # msgIds that arrived by push, even if still queued
        self.last_push_at: float | None = None
        self.running = True
        self.last_pic_url: str = ""
        self.live_caps: list[float] = []
        self.last_poll = 0.0
        self.alert_names = dict(ALERT_NAMES)
        self.client, self.mq, self.det = client, mq, det
        self.session = None
        self.push_factory = push_factory or (lambda token, session, cb: PushClient(token, session, on_message_callback=cb))
        self.kuma_get = kuma_get or urllib.request.urlopen
        now = clock()
        self.started_at = now
        # EZVIZ login
        self.logged_in = False
        self.login_backoff = Backoff(LOGIN_BACKOFF_SECONDS)
        self.next_login_attempt = 0.0
        self.login_down_since: float | None = None
        # push channel
        self.push = None
        self.push_backoff = Backoff(PUSH_BACKOFF_SECONDS)
        self.next_push_attempt = 0.0
        self.push_registered_at = 0.0
        self.push_up_since: float | None = None
        self.push_down_since: float | None = now
        self.push_attempts = 0
        self.missed_total = 0
        self.last_missed: str | None = None
        # cloud cross-check
        self.cloud_seeded = False
        self.cloud_high_water = 0
        self.next_cloud_check = 0.0
        self.last_cloud_check_ok: float | None = None
        self.cloud_failures = 0
        self.poll_failures = 0                     # consecutive failed status polls
        self.last_cloud_contact: float | None = None   # newest successful login / status poll / cross-check
        self.pending: dict[str, float] = {}        # msgId -> first seen in the cloud while push was up
        # misc
        self.next_kuma = 0.0
        self._health_published: str | None = None
        self._last_source: str | None = None
        self._guard_logged: dict[str, float] = {}

    def start(self) -> None:
        if self.mq is None:
            self.mq = LocalMqtt(on_ready=self.republish_health)
        if self.det is None:
            LOG.info("loading model %s (conf>=%.2f, labels=%s)", MODEL_PATH, DETECT_CONF, LABELS)
            self.det = Detector(MODEL_PATH, conf=DETECT_CONF)
        if self.client is None:
            self.client = EzvizClient(ACCOUNT, PASSWORD, REGION)
        if KUMA_PUSH_URL:
            LOG.info("Uptime Kuma heartbeat every %ss to %s", KUMA_INTERVAL_SECONDS, urlsplit(KUMA_PUSH_URL).netloc or "?")
        self.login_and_prime()
        self.publish_health()

    # --- EZVIZ session --------------------------------------------------------------
    def try_login(self) -> bool:
        now = self.clock()
        try:
            self.client.login()
        except Exception as exc:  # noqa: BLE001
            auth = isinstance(exc, EzvizAuthVerificationCode) or any(s in str(exc) for s in ("Incorrect", "locked", "MFA"))
            delay = LOGIN_AUTH_RETRY_SECONDS if auth else self.login_backoff.fail()
            self.login_down_since = self.login_down_since or now
            self.next_login_attempt = now + delay
            LOG.error("EZVIZ login failed (%s) — staying up, next login attempt in %d min%s", short_exc(exc),
                      delay // 60, " (credentials/lock/MFA: fix, then restart the container)" if auth else "")
            return False
        self.logged_in = True
        self.login_backoff.reset()
        self.login_down_since = None
        self.last_cloud_contact = now
        LOG.info("EZVIZ login OK (%s)", REGION)
        return True

    def login_and_prime(self) -> None:
        """Log in, then everything that needs a session: settings check, status poll, seed, push."""
        if not self.try_login():
            return
        self.session = getattr(self.client, "session", None) or self.client._session  # noqa: SLF001
        try:
            self.apply_detection_mode()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("detection mode check failed: %s", short_exc(exc))
        self.poll(force=True)
        self.cross_check()          # the first successful check only seeds the seen-set: nothing old is replayed
        self.supervise_push()       # first push registration attempt

    # --- device settings ----------------------------------------------------------
    def apply_detection_mode(self) -> None:
        want = DETECT_MODE_VALUES.get(DETECTION_MODE)
        if want is None:
            return
        cur = self.device().get("Alarm_DetectHumanCar")
        if cur == want:
            LOG.info("doorbell detection mode already %s (%s)", DETECTION_MODE, want)
            return
        ok = self.client.set_alarm_detect_human_car(SERIAL, want)
        LOG.warning("doorbell detection mode %s -> %s (%s): %s", cur, want, DETECTION_MODE, "OK" if ok else "FAILED")

    def device(self) -> dict:
        cams = self.client.load_cameras()
        if SERIAL not in cams:
            raise PyEzvizError(f"serial {SERIAL} not in account (have {list(cams)})")
        return cams[SERIAL]

    # --- polling ------------------------------------------------------------------
    def poll(self, force: bool = False) -> None:
        if not force and time.time() - self.last_poll < POLL_SECONDS:
            return
        self.last_poll = time.time()
        self.poll_failures += 1         # counts as failed until the device read succeeds (unexpected exceptions too)
        try:
            d = self.device()
        except PyEzvizError as exc:
            LOG.warning("poll failed (%s) — re-login", exc)
            try:
                self.client.login()     # refreshes the session token when it can; password login only if that fails
                d = self.device()
            except Exception as exc2:  # noqa: BLE001
                LOG.error("re-login/poll failed (%d in a row): %s", self.poll_failures, short_exc(exc2))
                return
        self.poll_failures, self.last_cloud_contact = 0, self.clock()
        code, name = d.get("last_alarm_type_code"), d.get("last_alarm_type_name")
        if isinstance(code, int) and name:
            self.alert_names[code] = name
        # ⚠️ 5 is NOT hibernate on the HP3 — the EZVIZ app shows "Standard Mode" for this
        # exact value (confirmed against the app 2026-09-05). pyezvizapi's enum is wrong here.
        mode_names = {0: "power_save", 1: "high_performance", 2: "plugged_in", 3: "super_power_save",
                      4: "custom", 5: "standard", 7: "always_on_video"}
        det_names = {v: k for k, v in DETECT_MODE_VALUES.items()}
        self.mq.pub(f"{BASE}/battery", d.get("battery_level"), retain=True)
        self.mq.pub(f"{BASE}/online", "ON" if d.get("status") == 1 else "OFF", retain=True)
        self.mq.pub(f"{BASE}/work_mode", mode_names.get(d.get("battery_camera_work_mode"), str(d.get("battery_camera_work_mode"))), retain=True)
        self.mq.pub(f"{BASE}/detection_mode", det_names.get(d.get("Alarm_DetectHumanCar"), str(d.get("Alarm_DetectHumanCar"))), retain=True)
        LOG.info("poll: online=%s battery=%s%% work_mode=%s detect=%s last_alarm=%s %s",
                 d.get("status") == 1, d.get("battery_level"), d.get("battery_camera_work_mode"),
                 d.get("Alarm_DetectHumanCar"), d.get("last_alarm_time"), name)

    # --- push channel ---------------------------------------------------------------
    def on_push(self, decoded: dict) -> None:
        """Runs on the push client's thread — just record the msgId and enqueue."""
        ext = decoded.get("ext") if isinstance(decoded.get("ext"), dict) else {}
        serial = ext.get("device_serial")
        if serial and serial != SERIAL:
            LOG.debug("push for other device %s ignored", serial)
            return
        msg_id = str(ext.get("msgId") or decoded.get("msgId") or "")
        with self.lock:
            self.last_push_at = time.time()
            if msg_id:
                self.pushed[msg_id] = self.last_push_at
                while len(self.pushed) > 1000:
                    self.pushed.popitem(last=False)
        self.q.put(("push", decoded))

    def push_healthy(self) -> bool:
        p = self.push
        if p is None or p.connected_at is None or p.dropped_at is not None or self.push_up_since is None:
            return False
        mc = getattr(p, "mqtt_client", None)
        return bool(mc and mc.is_connected())

    def supervise_push(self) -> None:
        """Main-thread state machine: register when due, notice CONNACK / drops, reset backoff when stable."""
        now = self.clock()
        p = self.push
        if p is None:
            if now >= self.next_push_attempt:
                self.register_push()
            return
        if p.dropped_at is not None:
            self.push_lost(f"push channel dropped (rc={p.drop_rc})")
            return
        if p.connected_at is None:
            if now - self.push_registered_at > PUSH_CONNECT_TIMEOUT:
                self.push_lost(f"push channel: broker did not accept the connection within {PUSH_CONNECT_TIMEOUT}s",
                               failed=True)
            return
        mc = getattr(p, "mqtt_client", None)
        if not (mc and mc.is_connected()):
            self.push_lost("push channel not connected", failed=self.push_up_since is None)
            return
        if self.push_up_since is None:
            down = now - self.push_down_since if self.push_down_since else 0.0
            self.push_up_since, self.push_down_since = now, None
            LOG.info("push channel up: broker=%s topic=%s keepalive=%s (registration attempt #%d%s)",
                     (getattr(p, "_mqtt_data", None) or {}).get("push_url"), getattr(p, "_topic", "?"), KEEPALIVE,
                     self.push_attempts, f"; was down/poll-only for {down / 60:.0f} min" if down >= 60 else "")
        elif self.push_backoff.failures and now - self.push_up_since >= PUSH_STABLE_SECONDS:
            LOG.info("push channel stable for %d min — retry backoff reset", PUSH_STABLE_SECONDS // 60)
            self.push_backoff.reset()

    def register_push(self) -> None:
        """Fresh, full registration: new client → getClientId → push/start → paho connect."""
        if self.push is not None:                  # never two live registrations
            self.teardown_push()
        now = self.clock()
        self.push_attempts += 1
        push = None
        try:
            push = self.push_factory(self.client.export_token(), self.session or self.client._session,  # noqa: SLF001
                                     self.on_push)
            # clean_session=True: a lingering session makes pyezvizapi skip the subscribe.
            # keepalive 30: the EZVIZ broker dropped keepalive=60 clients at exactly 60 s.
            push.connect(clean_session=True, keepalive=KEEPALIVE)
        except Exception as exc:  # noqa: BLE001
            if push is not None:
                self.teardown_push(push)
            self.push_down_since = self.push_down_since or now
            delay = self.push_backoff.fail()
            self.next_push_attempt = now + delay
            LOG.warning("push registration failed (%s) — poll-only, fresh registration in %d min",
                        short_exc(exc), delay // 60)
            return
        self.push, self.push_registered_at, self.push_up_since = push, now, None
        LOG.info("push client registered (attempt #%d, new client id) — waiting for the broker", self.push_attempts)

    def push_lost(self, reason: str, failed: bool = False) -> None:
        """Tear the push client down and schedule a from-scratch registration (paho resume is not enough)."""
        now = self.clock()
        stable = not failed and self.push_up_since is not None and now - self.push_up_since >= PUSH_STABLE_SECONDS
        self.teardown_push()
        self.push_down_since = self.push_down_since or now
        if stable:
            self.push_backoff.reset()
            self.next_push_attempt = now
            LOG.warning("%s — re-registering from scratch now", reason)
        else:
            delay = self.push_backoff.fail()
            self.next_push_attempt = now + delay
            LOG.warning("%s — poll-only, fresh registration in %d min", reason, delay // 60)
        self.next_cloud_check = min(self.next_cloud_check, now)   # look for anything missed right away

    def teardown_push(self, push=None) -> None:
        """Stop a push client completely so its paho thread cannot reconnect behind our back."""
        if push is None or push is self.push:
            push, self.push, self.push_up_since = self.push, None, None
        if push is None:
            return
        push.closing = True
        mc = getattr(push, "mqtt_client", None)
        if mc is not None:
            for step in (mc.disconnect, mc.loop_stop):     # disconnect first so paho's loop exits cleanly
                try:
                    step()
                except Exception as exc:  # noqa: BLE001
                    LOG.debug("push teardown %s: %s", getattr(step, "__name__", "step"), exc)
        if (getattr(push, "_mqtt_data", None) or {}).get("ticket"):
            try:
                push._stop_ezviz_push()  # noqa: SLF001 — ask EZVIZ to forget the old client id
            except Exception as exc:  # noqa: BLE001
                LOG.debug("push/stop for the old client failed (harmless): %s", short_exc(exc))

    # --- cloud cross-check ------------------------------------------------------------
    def cross_check(self) -> None:
        """Compare the cloud alarm list with what we have handled — by msgId ONLY.

        ⚠️ Cloud epoch times are device-LOCAL time encoded as UTC (e.g. +1 h during BST): never match them
        against push/UTC times. They are only compared with other cloud times (replay guard).
        """
        now = self.clock()
        covered_since = self.push_up_since if self.push_healthy() else None
        self.next_cloud_check = now + (CLOUD_CHECK_PUSH_SECONDS if covered_since is not None
                                       else CLOUD_CHECK_POLL_ONLY_SECONDS)
        try:
            r = self.client.get_device_messages_list(SERIAL, limit=CLOUD_LIST_LIMIT, date="", end_time="")
        except Exception as exc:  # noqa: BLE001
            self.cloud_failures += 1
            log = LOG.warning if self.cloud_failures in (1, 5) or self.cloud_failures % 60 == 0 else LOG.debug
            log("cloud cross-check failed (%d in a row): %s", self.cloud_failures, short_exc(exc))
            return
        if self.cloud_failures >= 5:
            LOG.info("cloud cross-check working again after %d failures", self.cloud_failures)
        msgs = [m for m in (r.get("message") or r.get("messages") or [])
                if isinstance(m, dict) and m.get("msgId") and m.get("deviceSerial", SERIAL) == SERIAL]
        prev_ok, self.last_cloud_check_ok, self.cloud_failures = self.last_cloud_check_ok, now, 0
        self.last_cloud_contact = now
        self.mq.pub(f"{BASE}/last_cloud_check", utc_iso(now), retain=True)
        newest = max((msg_time(m) for m in msgs), default=0)
        if not self.cloud_seeded:
            for m in msgs:
                self.seen.add(str(m["msgId"]), "seed")
            self.cloud_seeded, self.cloud_high_water = True, max(self.cloud_high_water, newest)
            LOG.info("cloud cross-check seeded with %d alarm(s) already in the cloud (newest %s device time) — not replayed",
                     len(msgs), msgs[0].get("timeStr") if msgs else "-")
            return
        with self.lock:
            pushed = set(self.pushed)
        for mid in [k for k in self.pending if k in self.seen or k in pushed]:
            self.pending.pop(mid, None)
        fresh = [m for m in reversed(msgs) if str(m["msgId"]) not in self.seen and str(m["msgId"]) not in pushed]
        if len(msgs) >= CLOUD_LIST_LIMIT and len(fresh) == len(msgs):
            LOG.warning("cloud cross-check: all %d listed alarms are new — any older ones are beyond this page", len(msgs))
        # push "covers" an alarm only if it was up for the whole window the alarm could have appeared in
        covered = covered_since is not None and prev_ok is not None and covered_since <= prev_ok
        missed: list[dict] = []
        for m in fresh:                              # oldest first
            mid, t = str(m["msgId"]), msg_time(m)
            if t and t < self.cloud_high_water - CLOUD_REORDER_SLACK_MS:
                self.seen.add(mid, "stale")
                LOG.info("cloud alarm %s (%s device time) is older than alarms already seen — skipped, not replayed",
                         mid, m.get("timeStr"))
                continue
            if not covered:
                self.pending.pop(mid, None)
                self.handle(cloud_to_push(m), "poll")
                continue
            first = self.pending.setdefault(mid, now)
            if now - first >= PUSH_MISS_GRACE_SECONDS:
                self.pending.pop(mid, None)
                missed.append(m)
        self.cloud_high_water = max(self.cloud_high_water, newest)
        for m in missed:
            self.missed_total, self.last_missed = self.missed_total + 1, str(m["msgId"])
            LOG.warning("push missed alarm %s (%s device time) — not pushed within %ds, handling it from the cloud list",
                        m["msgId"], m.get("timeStr"), PUSH_MISS_GRACE_SECONDS)
            event = cloud_to_push(m)
            event["late"] = True
            self.handle(event, "poll")
        if missed and self.push is not None:
            self.push_lost(f"push channel judged dead ({len(missed)} missed alarm(s))")
        if self.pending:
            self.next_cloud_check = min(self.next_cloud_check, min(self.pending.values()) + PUSH_MISS_GRACE_SECONDS + 1)

    # --- event pipeline (push and poll share it) ------------------------------------
    def handle(self, decoded: dict, source: str = "push") -> None:
        ext = decoded.get("ext") or {}
        msg_id = str(ext.get("msgId") or decoded.get("msgId") or "")
        now = time.time()
        if msg_id and msg_id in self.seen:
            if source == "push" and self.seen.source(msg_id) == "poll":
                LOG.info("late push %s — already handled from the cloud list", msg_id)
            else:
                LOG.debug("duplicate %s %s ignored", source, msg_id)
            return
        self.seen.add(msg_id, source)
        code = ext.get("alert_type_code")
        try:
            code = int(code)
        except (TypeError, ValueError):
            pass
        title = decoded.get("title") if source == "poll" else None
        if isinstance(code, int) and title:
            self.alert_names.setdefault(code, title)
        name = self.alert_names.get(code) or title or f"Alert {code}"
        when = ext.get("time") or datetime.now(timezone.utc).isoformat()
        pic = ext.get("default_pic_url") or ext.get("media_url_alt1") or ext.get("media_url_alt2") or ""
        late = bool(decoded.get("late"))
        if source == "push":
            LOG.info("PUSH %s code=%s (%s) pic=%s", msg_id or "-", code, name, "in-push" if pic else "none")
        else:
            LOG.info("POLL %s code=%s (%s) pic=%s device_time=%s%s", msg_id or "-", code, name,
                     "in-list" if pic else "none", when, " — late: push missed it" if late else "")
        if code != 10120:
            # Unknown/rare code (a doorbell PRESS shows up in the cloud list as "Your doorbell is
            # ringing") — log everything so the code can be wired to its own sensor next time.
            LOG.info("non-motion %s ext: %s", source, json.dumps(
                {k: v for k, v in ext.items() if v not in (None, "") and "pic" not in k and "url" not in k},
                default=str)[:600])
        if not pic and source == "push":
            # This device's pushes arrive with every media URL empty (verified 2026-09-05,
            # ext carried only routing fields). The cloud's alarm record does hold the
            # snapshot, so poll it briefly — the picture lands a few seconds after the push.
            LOG.debug("push ext fields present: %s", sorted(k for k, v in ext.items() if v not in (None, "")))
            pic = self.pic_from_cloud()
        elif not pic:
            LOG.warning("cloud alarm %s carries no snapshot URL — event published without an image", msg_id)
        ts = datetime.now(timezone.utc).isoformat()

        self.mq.pub(f"{BASE}/motion", "ON")
        self.holds[f"{BASE}/motion"] = now + HOLD_SECONDS
        self.mq.pub(f"{BASE}/last_alert", name, retain=True)
        self.mq.pub(f"{BASE}/last_alert/attributes", {"code": code, "time": when, "msg_id": msg_id, "source": source,
                                                      "device_name": ext.get("device_name"), "encrypted": ext.get("is_encrypted")}, retain=True)
        self.mq.pub(f"{BASE}/last_event_time", ts, retain=True)

        dets: list[dict] = []
        img: Image.Image | None = None
        if pic:
            try:
                img = self.fetch_image(pic, ext)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("snapshot fetch/decrypt failed: %s", exc)
        if img is not None:
            t0 = time.time()
            dets = self.det.detect(img)
            LOG.info("detect: %s in %.0f ms", [(d["label"], d["conf"]) for d in dets] or "nothing", (time.time() - t0) * 1000)
            found = {d["label"] for d in dets}
            for label in LABELS:
                if label in found:
                    self.mq.pub(f"{BASE}/{label}", "ON")
                    self.holds[f"{BASE}/{label}"] = now + HOLD_SECONDS
            # A late (push-missed) alarm is minutes old: a live frame now would show something unrelated.
            if not dets and LIVE_CAPTURE_FALLBACK and not late:
                live = self.live_capture()
                if live is not None:
                    live_dets = self.det.detect(live)
                    LOG.info("live re-capture: %s", [(d["label"], d["conf"]) for d in live_dets] or "nothing")
                    if live_dets:
                        img, dets = live, live_dets
                        found = {d["label"] for d in dets}
                        for label in LABELS:
                            if label in found:
                                self.mq.pub(f"{BASE}/{label}", "ON")
                                self.holds[f"{BASE}/{label}"] = time.time() + HOLD_SECONDS
            top = dets[0]["label"] if dets else "none"
            self.mq.pub(f"{BASE}/detection", top, retain=True)
            self.mq.pub(f"{BASE}/detection/attributes", {"detections": dets, "time": ts, "alert": name,
                                                         "labels": sorted(found)}, retain=True)
            out = annotate(img, dets)
            buf = io.BytesIO(); out.save(buf, "JPEG", quality=85)
            self.mq.pub(f"{BASE}/snapshot", buf.getvalue(), retain=True)
            try:
                out.save(DATA_DIR / "last.jpg", "JPEG", quality=85)
                (DATA_DIR / "last.json").write_text(json.dumps({"time": ts, "alert": name, "code": code,
                                                                 "detections": dets}, indent=1))
            except OSError as exc:
                LOG.debug("could not save last.jpg: %s", exc)
            self.archive(ts, name, code, dets, out, msg_id, source)
        else:
            self.mq.pub(f"{BASE}/detection", "unknown", retain=True)
            self.mq.pub(f"{BASE}/detection/attributes", {"detections": [], "time": ts, "alert": name,
                                                         "labels": [], "note": "no snapshot"}, retain=True)
            self.archive(ts, name, code, [], None, msg_id, source)

    def pic_from_cloud(self, tries: int = 5, delay: float = 4.0) -> str:
        """Fetch the alarm snapshot URL from the cloud record after a push.

        Returns "" if nothing new turns up, in which case the event is still
        published (motion + last_alert), just without a classified image.
        """
        for attempt in range(tries):
            time.sleep(2.0 if attempt == 0 else delay)
            try:
                d = self.device()
            except Exception as exc:  # noqa: BLE001
                LOG.debug("snapshot poll %d/%d failed: %s", attempt + 1, tries, exc)
                continue
            url = d.get("last_alarm_pic") or ""
            if url and url != self.last_pic_url:
                self.last_pic_url = url
                LOG.info("snapshot URL from cloud after ~%.0fs (alarm %s)",
                         2.0 + attempt * delay, d.get("last_alarm_time"))
                return url
        LOG.warning("no new snapshot in the cloud record after ~%.0fs — event published without an image",
                    2.0 + (tries - 1) * delay)
        return ""

    def archive(self, ts: str, alert: str, code, dets: list[dict], img: Image.Image | None, msg_id: str = "",
                source: str = "push") -> None:
        """Persist one event: a jsonl row always, plus the annotated frame if there is one."""
        try:
            stamp = datetime.now(timezone.utc)
            day = DATA_DIR / "events" / stamp.strftime("%Y-%m-%d")
            day.mkdir(parents=True, exist_ok=True)
            labels = sorted({d["label"] for d in dets}) or ["none"]
            fname = f'{stamp.strftime("%H%M%S")}_{"-".join(labels)}.jpg'
            if img is not None:
                if (day / fname).exists():       # two cloud alarms handled within the same second
                    tag = "".join(c for c in msg_id[-8:] if c.isalnum() or c in "-_") or "dup"
                    fname = f'{stamp.strftime("%H%M%S")}_{"-".join(labels)}_{tag}.jpg'
                img.save(day / fname, "JPEG", quality=85)
            with (DATA_DIR / "events.jsonl").open("a") as fh:
                fh.write(json.dumps({"time": ts, "msg_id": msg_id, "source": source, "alert": alert, "code": code,
                                     "labels": labels, "detections": dets,
                                     "image": fname if img is not None else None}) + "\n")
            cutoff = stamp.timestamp() - EVENT_RETENTION_DAYS * 86400
            for old in (DATA_DIR / "events").iterdir():
                if old.is_dir() and old.stat().st_mtime < cutoff:
                    for f in old.iterdir():
                        f.unlink()
                    old.rmdir()
                    LOG.debug("pruned old event dir %s", old.name)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("archiving event failed: %s", exc)

    def live_capture(self) -> Image.Image | None:
        """Ask the device for a fresh frame right now (wakes it; ~7 s).

        Rate-capped: waking the camera costs battery (and a solar panel does not charge overnight).
        """
        now = time.time()
        self.live_caps = [t for t in self.live_caps if now - t < 3600]
        if len(self.live_caps) >= LIVE_CAPTURE_MAX_PER_HOUR:
            LOG.info("live re-capture skipped — %d already this hour (cap %d, saving battery)",
                     len(self.live_caps), LIVE_CAPTURE_MAX_PER_HOUR)
            return None
        self.live_caps.append(now)
        try:
            r = self.client.capture_picture(SERIAL, 1)
            url = ((r or {}).get("captureInfo") or {}).get("picUrl")
            if not url:
                LOG.debug("live capture returned no picUrl: %s", str(r)[:160])
                return None
            return self.fetch_image(url, {})
        except Exception as exc:  # noqa: BLE001
            LOG.warning("live capture failed: %s", exc)
            return None

    def fetch_image(self, url: str, ext: dict) -> Image.Image:
        r = self.session.get(url, timeout=25)
        r.raise_for_status()
        data = r.content
        if b"hikencodepicture" in data[:64]:
            if not VERIFY_CODE:
                raise RuntimeError("snapshot is encrypted and EZVIZ_VERIFY_CODE is not set")
            data = decrypt_image(data, VERIFY_CODE)
        return Image.open(io.BytesIO(data))

    # --- health: HA entities + Uptime Kuma ----------------------------------------------
    def health(self) -> dict:
        up = self.logged_in and self.push_healthy()
        with self.lock:
            last_push = self.last_push_at
        return {"push_channel": "ON" if up else "OFF",
                "event_source": "push" if up else ("poll-only" if self.logged_in else "offline"),
                "attributes": {
                    "push_registered": self.push is not None,
                    "push_down_since": None if up else utc_iso(self.push_down_since),
                    "next_push_attempt": utc_iso(self.next_push_attempt) if self.logged_in and self.push is None else None,
                    "push_retry_failures": self.push_backoff.failures,
                    "push_registration_attempts": self.push_attempts,
                    "last_push_message": utc_iso(last_push),
                    "missed_alarms": self.missed_total,
                    "last_missed_alarm": self.last_missed,
                    "cloud_check_every_s": CLOUD_CHECK_PUSH_SECONDS if up else CLOUD_CHECK_POLL_ONLY_SECONDS,
                    "ezviz_login_ok": self.logged_in}}

    def publish_health(self) -> None:
        if self.mq is None:
            return
        h = self.health()
        if h["event_source"] != self._last_source:
            LOG.info("event source: %s%s", f"{self._last_source} → " if self._last_source else "", h["event_source"])
            self._last_source = h["event_source"]
        key = json.dumps(h, sort_keys=True, default=str)
        if key == self._health_published:
            return
        self._health_published = key
        self.mq.pub(f"{BASE}/push_channel", h["push_channel"], retain=True)
        self.mq.pub(f"{BASE}/event_source", h["event_source"], retain=True)
        self.mq.pub(f"{BASE}/event_source/attributes", h["attributes"], retain=True)

    def republish_health(self) -> None:
        """Local MQTT (re)connected. Runs on paho's thread, so only invalidate; the main loop re-sends."""
        self._health_published = None

    def kuma_status(self) -> tuple[str, str]:
        """(status, msg) for the Kuma push monitor: up = the bridge can do its job (see KUMA_CLOUD_FAIL_COUNT)."""
        now = self.clock()

        def hm(ts: float) -> str:
            return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")

        def mins(ts: float) -> int:
            return max(0, math.ceil((ts - now) / 60))

        if not self.logged_in:
            return "down", (f"EZVIZ login failing since {hm(self.login_down_since or self.started_at)} UTC, "
                            f"retry in {mins(self.next_login_attempt)}m")
        push_ok = self.push_healthy()
        if push_ok:
            mode = "push ok"
        else:
            since = self.push_down_since or now
            days = f" ({int((now - since) // 86400)} days)" if now - since > KUMA_PUSH_LONG_DOWN_SECONDS else ""
            retry = "connecting" if self.push is not None else f"retry in {mins(self.next_push_attempt)}m"
            mode = f"poll-only: push down since {hm(since)} UTC{days}, {retry}"
        last = self.last_cloud_contact or self.started_at
        # With push up the cross-check runs every 5 min, so one isolated failure leaves a 10-min gap by design.
        interval = CLOUD_CHECK_PUSH_SECONDS if push_ok else CLOUD_CHECK_POLL_ONLY_SECONDS
        stale_after = max(KUMA_CLOUD_STALE_SECONDS, 2 * interval + KUMA_INTERVAL_SECONDS)
        if self.cloud_failures >= KUMA_CLOUD_FAIL_COUNT:
            problem = f"{self.cloud_failures} cross-checks failed in a row"
        elif self.poll_failures >= KUMA_CLOUD_FAIL_COUNT:
            problem = f"{self.poll_failures} status polls failed in a row"
        elif now - last > stale_after:
            problem = f"no successful contact for {int((now - last) // 60)}m"
        else:
            return "up", mode
        return "down", f"EZVIZ cloud failing: {problem}, last OK {hm(last)} UTC; {mode}"

    def kuma_heartbeat(self) -> None:
        self.next_kuma = self.clock() + KUMA_INTERVAL_SECONDS
        status, text = self.kuma_status()
        try:
            with self.kuma_get(kuma_url(KUMA_PUSH_URL, status, text), timeout=5) as resp:
                resp.read(256)
            LOG.debug("kuma heartbeat sent: %s (%s)", status, text)
        except Exception as exc:  # noqa: BLE001 — Kuma trouble must never affect the bridge
            LOG.debug("kuma heartbeat failed: %s", type(exc).__name__)

    # --- main loop ----------------------------------------------------------------
    def expire_holds(self) -> None:
        now = time.time()
        for topic in [t for t, exp in self.holds.items() if exp <= now]:
            self.holds.pop(topic, None)
            self.mq.pub(topic, "OFF")

    def _guard(self, fn, *args) -> None:
        try:
            fn(*args)
        except Exception as exc:  # noqa: BLE001
            name, now = getattr(fn, "__name__", "task"), self.clock()
            if now - self._guard_logged.get(name, float("-inf")) >= 60:
                self._guard_logged[name] = now
                LOG.exception("%s failed (continuing): %s", name, exc)

    def tick(self) -> None:
        """Everything periodic. Each step is guarded so one failure cannot stop the others."""
        now = self.clock()
        self._guard(self.expire_holds)
        if not self.logged_in:
            if now >= self.next_login_attempt:
                self._guard(self.login_and_prime)
        else:
            self._guard(self.supervise_push)
            if now >= self.next_cloud_check:
                self._guard(self.cross_check)
            self._guard(self.poll)
        if KUMA_PUSH_URL and now >= self.next_kuma:
            self._guard(self.kuma_heartbeat)
        self._guard(self.publish_health)

    def request_stop(self, *_) -> None:
        self.running = False

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        LOG.info("ready — %s (%s), event source: %s", SERIAL, DEVICE_NAME, self.health()["event_source"])
        while self.running:
            try:
                source, item = self.q.get(timeout=1)
            except queue.Empty:
                source, item = "push", None
            if item is not None:
                try:
                    self.handle(item, source)
                except Exception as exc:  # noqa: BLE001
                    LOG.exception("handling %s event failed: %s", source, exc)
            self.tick()
        self.shutdown()

    def shutdown(self) -> None:
        LOG.info("stopping")
        live_mqtt = not DRY_RUN and isinstance(self.mq, LocalMqtt)
        if live_mqtt:       # first: tearing push down makes an HTTP call that may be slow
            try:
                self.mq.c.publish(f"{BASE}/status", "offline", qos=1, retain=True).wait_for_publish(timeout=3)
            except Exception:  # noqa: BLE001
                pass
        self.teardown_push()
        if live_mqtt:
            try:
                self.mq.c.disconnect()
                self.mq.c.loop_stop()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    bridge = Bridge()
    bridge.start()
    bridge.run()
