"""Offline tests for the push-resilience / cloud cross-check logic in bridge.py (no network).

Run inside the image:
  docker run --rm --network none -v "$PWD/app:/app:ro" --entrypoint python ezviz-bridge:local \
      -m unittest discover -s /app/tests -v
"""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

os.environ.update({"EZVIZ_ACCOUNT": "test", "EZVIZ_PASSWORD": "test", "EZVIZ_SERIAL": "BFTEST0001",
                   "MQTT_PASS": "", "DATA_DIR": tempfile.mkdtemp(prefix="ezviz-test-"),
                   "LIVE_CAPTURE_FALLBACK": "1", "LOG_LEVEL": "CRITICAL", "KUMA_PUSH_URL": ""})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bridge as B  # noqa: E402
import requests  # noqa: E402
from PIL import Image  # noqa: E402
from pyezvizapi.exceptions import HTTPError  # noqa: E402


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


class FakeMq:
    def __init__(self):
        self.pubs = []

    def pub(self, topic, payload, retain=False):
        self.pubs.append((topic, payload, retain))

    def last(self, suffix):
        vals = [p for t, p, _ in self.pubs if t.endswith("/" + suffix)]
        return vals[-1] if vals else None


class FakeDet:
    def __init__(self):
        self.result = [{"label": "person", "conf": 0.9, "box": [1, 1, 5, 5]}]

    def detect(self, img):
        return list(self.result)


class FakeClient:
    _session = object()

    def __init__(self):
        self.messages, self.list_calls, self.logins, self.captures = [], 0, 0, 0
        self.login_error = self.list_error = self.cameras_error = None

    def login(self):
        self.logins += 1
        if self.login_error:
            raise self.login_error
        return self.export_token()

    def export_token(self):
        return {"username": "u", "session_id": "s", "service_urls": {"pushAddr": "push.invalid"}}

    def get_device_messages_list(self, serials, limit=20, date="", end_time=""):
        self.list_calls += 1
        if self.list_error:
            raise self.list_error
        return {"message": list(self.messages[:limit]), "hasNext": False, "meta": {"code": 200}}

    def load_cameras(self):
        if self.cameras_error:
            raise self.cameras_error
        return {B.SERIAL: {"status": 1, "battery_level": 100, "battery_camera_work_mode": 5, "Alarm_DetectHumanCar": 1}}

    def capture_picture(self, serial, channel):
        self.captures += 1
        return {}

    def __getattr__(self, name):          # any EZVIZ setter / MFA call fails the test loudly
        if name.startswith("set_") or name in ("send_mfa_code", "get_cam_key"):
            raise AssertionError(f"forbidden EZVIZ call: {name}")
        raise AttributeError(name)


class FakePaho:
    def __init__(self):
        self.connected, self.calls = False, []

    def is_connected(self):
        return self.connected

    def disconnect(self):
        self.calls.append("disconnect")
        self.connected = False

    def loop_stop(self):
        self.calls.append("loop_stop")


class FakePush:
    def __init__(self, fail=None):
        self.fail, self.mqtt_client = fail, None
        self.connected_at = self.dropped_at = self.drop_rc = None
        self.closing, self.stopped = False, False
        self._mqtt_data, self._topic = {"ticket": None, "push_url": "push.invalid"}, "t/#"

    def connect(self, clean_session, keepalive):
        if self.fail:
            raise self.fail
        self.mqtt_client, self._mqtt_data["ticket"] = FakePaho(), "ticket"

    def _stop_ezviz_push(self):
        self.stopped = True


class PushFactory:
    def __init__(self):
        self.plan, self.made = [], []           # plan: exceptions to raise on successive attempts (None = OK)
        self.fail_all = None                    # callable → exception for every attempt once the plan is used up

    def __call__(self, token, session, cb):
        p = FakePush(self.plan.pop(0) if self.plan else (self.fail_all() if self.fail_all else None))
        self.made.append(p)
        return p


def msg(mid, t_ms, pic=True, alarm=10120, title="AI Human Detection"):
    return {"msgId": mid, "deviceSerial": B.SERIAL, "time": t_ms, "timeStr": "2026-09-12 10:00:00", "title": title,
            "subType": 2005, "pic": f"https://pic.invalid/{mid}?sig=x" if pic else "", "ext": {"alarmType": alarm}}


def connack(b, clock):
    b.push.mqtt_client.connected = True
    b.push.connected_at = clock()


def push_503():
    exc = HTTPError()
    exc.__cause__ = RuntimeError("503 Server Error: Back-end server is at capacity")
    return exc


def remote_disconnected():
    """The routine cross-check blip seen live: EZVIZ closes an idle keep-alive connection (not wrapped by pyezvizapi)."""
    return requests.exceptions.ConnectionError("('Connection aborted.', RemoteDisconnected('Remote end closed "
                                               "connection without response'))")


def hm(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


class Base(unittest.TestCase):
    def setUp(self):
        B.DATA_DIR = Path(tempfile.mkdtemp(prefix="ezviz-data-"))
        self.clock, self.client, self.mq, self.pf = Clock(), FakeClient(), FakeMq(), PushFactory()
        self.kuma_calls = []
        self.b = B.Bridge(client=self.client, mq=self.mq, det=FakeDet(), push_factory=self.pf, clock=self.clock,
                          kuma_get=self._kuma_get)
        self.b.fetch_image = lambda url, ext: Image.new("RGB", (64, 36))
        self.b.pic_from_cloud = lambda *a, **k: ""          # the real one sleeps; not under test here
        self.b.poll = lambda force=False: None               # status poll unchanged; keep tests focused
        self.b._guard = lambda fn, *a: fn(*a)                # surface bugs instead of logging and carrying on

    def _kuma_get(self, url, timeout):
        self.kuma_calls.append((url, timeout))
        raise OSError("kuma unreachable")

    def rows(self):
        f = B.DATA_DIR / "events.jsonl"
        return [json.loads(line) for line in f.read_text().splitlines()] if f.exists() else []

    def tick_after(self, seconds):
        self.clock.advance(seconds)
        self.b.tick()


class BackoffTest(unittest.TestCase):
    def test_steps_cap_and_reset(self):
        bo = B.Backoff(B.PUSH_BACKOFF_SECONDS)
        self.assertEqual([bo.fail() for _ in range(6)], [300, 600, 1200, 1800, 1800, 1800])
        bo.reset()
        self.assertEqual(bo.fail(), 300)


class PushSupervisorTest(Base):
    def test_startup_503_stays_up_poll_only_then_backs_off_and_recovers(self):
        self.pf.plan = [push_503(), push_503()]
        self.client.messages = [msg("A", 2_000_000), msg("B", 1_000_000)]
        self.b.login_and_prime()
        self.assertTrue(self.b.logged_in)
        self.assertIsNone(self.b.push)
        self.assertEqual(self.b.next_push_attempt, self.clock() + 300)
        self.assertEqual(self.b.health()["event_source"], "poll-only")
        self.assertEqual(self.b.health()["push_channel"], "OFF")
        self.assertEqual(self.b.kuma_status(),
                         ("up", f"poll-only: push down since {hm(self.b.started_at)} UTC, retry in 5m"))
        self.assertEqual(self.rows(), [])                     # seeding replayed nothing
        self.tick_after(299)
        self.assertEqual(len(self.pf.made), 1)                # not yet due
        self.tick_after(2)
        self.assertEqual(len(self.pf.made), 2)
        self.assertEqual(self.b.next_push_attempt, self.clock() + 600)
        self.tick_after(601)
        self.assertEqual(len(self.pf.made), 3)                # third attempt succeeds
        self.assertIsNotNone(self.b.push)
        self.assertFalse(self.b.push_healthy())               # no CONNACK yet
        connack(self.b, self.clock)
        self.tick_after(1)
        self.assertTrue(self.b.push_healthy())
        self.assertEqual(self.b.health()["event_source"], "push")
        self.assertEqual(self.b.kuma_status(), ("up", "push ok"))
        self.assertEqual(self.b.push_backoff.failures, 2)
        self.tick_after(B.PUSH_STABLE_SECONDS)
        self.assertEqual(self.b.push_backoff.failures, 0)     # stable → reset

    def test_drop_before_stable_backs_off_and_tears_down(self):
        self.b.login_and_prime()
        connack(self.b, self.clock)
        self.tick_after(1)
        old = self.b.push
        old.dropped_at = self.clock()
        self.tick_after(1)
        self.assertIsNone(self.b.push)
        self.assertTrue(old.closing and old.stopped)
        self.assertEqual(old.mqtt_client.calls, ["disconnect", "loop_stop"])
        self.assertEqual(self.b.next_push_attempt, self.clock() + 300)

    def test_drop_after_stable_reregisters_immediately_with_new_client(self):
        self.b.login_and_prime()
        connack(self.b, self.clock)
        self.tick_after(1)
        self.tick_after(B.PUSH_STABLE_SECONDS + 5)
        self.b.push.dropped_at = self.clock()
        self.tick_after(1)                                    # lost → next attempt = now
        self.tick_after(1)                                    # fresh registration
        self.assertEqual(len(self.pf.made), 2)
        self.assertIsNot(self.pf.made[0], self.pf.made[1])
        self.assertIs(self.b.push, self.pf.made[1])

    def test_no_connack_counts_as_failure(self):
        self.b.login_and_prime()
        self.tick_after(B.PUSH_CONNECT_TIMEOUT + 1)
        self.assertIsNone(self.b.push)
        self.assertEqual(self.b.push_backoff.failures, 1)


class CrossCheckTest(Base):
    def test_seed_then_new_alarm_goes_through_pipeline_once(self):
        self.pf.plan = [push_503()]
        self.client.messages = [msg("A", 2_000_000), msg("B", 1_000_000)]
        self.b.login_and_prime()
        self.assertEqual(self.rows(), [])
        self.client.messages.insert(0, msg("C", 3_000_000))
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        rows = self.rows()
        self.assertEqual([(r["msg_id"], r["source"]) for r in rows], [("C", "poll")])
        self.assertEqual(rows[0]["labels"], ["person"])
        self.assertEqual(self.mq.last("motion"), "ON")
        self.assertEqual(self.mq.last("last_alert"), "AI Human Detection")
        self.assertIsNotNone(self.mq.last("last_cloud_check"))
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)       # still listed → not replayed
        self.b.on_push({"ext": {"msgId": "C", "device_serial": B.SERIAL, "alert_type_code": 10120}})
        source, item = self.b.q.get_nowait()
        self.b.handle(item, source)                            # late push → dedupe by msgId
        self.assertEqual(len(self.rows()), 1)

    def test_queued_push_is_not_double_counted_by_poll(self):
        self.pf.plan = [push_503()]
        self.b.login_and_prime()
        self.b.on_push({"ext": {"msgId": "D", "device_serial": B.SERIAL, "alert_type_code": 10120}})
        self.client.messages = [msg("D", 5_000_000)]
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)       # push still sitting in the queue
        self.assertEqual(self.rows(), [])
        source, item = self.b.q.get_nowait()
        self.b.handle(item, source)
        self.assertEqual([(r["msg_id"], r["source"]) for r in self.rows()], [("D", "push")])

    def test_push_missed_alarm_warns_handles_late_and_reregisters(self):
        self.client.messages = [msg("A", 2_000_000)]
        self.b.login_and_prime()
        connack(self.b, self.clock)
        self.tick_after(1)
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)       # a check fully inside the push-up window
        self.tick_after(B.PUSH_STABLE_SECONDS)
        self.b.det.result = []                                # empty frame → normally triggers a live re-capture
        self.client.messages.insert(0, msg("E", 9_000_000))
        self.clock.advance(B.CLOUD_CHECK_PUSH_SECONDS)
        self.b.tick()
        self.assertEqual(self.rows(), [])                     # grace period: push may still arrive
        self.assertIn("E", self.b.pending)
        first_push = self.b.push
        with self.assertLogs("ezviz-bridge", level="WARNING") as logs:
            self.tick_after(B.PUSH_MISS_GRACE_SECONDS + 1)
        self.assertTrue(any("push missed alarm E" in line for line in logs.output))
        self.assertEqual([(r["msg_id"], r["source"]) for r in self.rows()], [("E", "poll")])
        self.assertEqual(self.client.captures, 0)             # late alarm: no pointless device wake
        self.assertTrue(first_push.closing)
        self.tick_after(1)
        self.assertEqual(len(self.pf.made), 2)                # fresh registration right away (was stable)

    def test_old_message_reappearing_is_not_replayed(self):
        self.pf.plan = [push_503()]
        self.client.messages = [msg("NEW", 90_000_000)]
        self.b.login_and_prime()
        self.client.messages.append(msg("OLD", 1_000))        # e.g. newer alarms deleted in the app
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        self.assertEqual(self.rows(), [])

    def test_message_without_pic_still_publishes(self):
        self.pf.plan = [push_503()]
        self.b.login_and_prime()
        self.client.messages = [msg("NOPIC", 5_000_000, pic=False)]
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        rows = self.rows()
        self.assertEqual((rows[0]["msg_id"], rows[0]["source"], rows[0]["image"]), ("NOPIC", "poll", None))

    def test_timely_poll_event_keeps_live_capture_fallback(self):
        self.pf.plan = [push_503()]
        self.b.login_and_prime()
        self.b.det.result = []
        self.client.messages = [msg("F", 5_000_000)]
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        self.assertEqual(self.client.captures, 1)
        self.assertEqual(self.rows()[0]["labels"], ["none"])


class LoginAndKumaTest(Base):
    def test_login_failure_stays_up_and_backs_off(self):
        self.client.login_error = push_503()
        self.b.login_and_prime()
        self.assertFalse(self.b.logged_in)
        self.assertEqual(self.b.health()["event_source"], "offline")
        self.tick_after(30)
        self.assertEqual(self.client.logins, 1)
        self.client.login_error = None
        self.tick_after(31)
        self.assertEqual(self.client.logins, 2)
        self.assertTrue(self.b.logged_in)

    def send_kuma(self):
        """One real heartbeat through tick() (kuma_get raises → must be swallowed); returns (status, msg) as sent."""
        before = len(self.kuma_calls)
        B.KUMA_PUSH_URL = "http://kuma.invalid:3001/api/push/TOKEN?status=up&msg=OK&ping="
        try:
            self.b.next_kuma = 0.0
            self.b.tick()
        finally:
            B.KUMA_PUSH_URL = ""
        self.assertEqual(len(self.kuma_calls), before + 1)
        url, timeout = self.kuma_calls[-1]
        q = parse_qs(urlsplit(url).query, keep_blank_values=True)
        self.assertEqual((urlsplit(url).path, q["ping"]), ("/api/push/TOKEN", [""]))
        self.assertLessEqual(timeout, 5)
        return q["status"][0], q["msg"][0]

    def test_kuma_up_while_poll_only_through_blips_and_long_outage(self):
        self.pf.fail_all = push_503                           # EZVIZ pusheu 503 for the whole test
        self.b.login_and_prime()
        start = self.b.started_at
        self.assertEqual(self.send_kuma(), ("up", f"poll-only: push down since {hm(start)} UTC, retry in 5m"))
        for minute in range(1, 41):                            # 40 min: well past the old 15-min "down"
            if minute in (7, 8, 23):                           # isolated / double RemoteDisconnected blips
                self.client.list_error = remote_disconnected()
            self.tick_after(60)
            self.client.list_error = None
            status, text = self.b.kuma_status()
            self.assertEqual(status, "up", f"minute {minute}: {text}")
            self.assertTrue(text.startswith(f"poll-only: push down since {hm(start)} UTC, retry in "), text)
        self.assertIsNone(self.b.push)
        self.assertEqual(len(self.pf.made), 4)                 # push backoff unchanged: attempts at 0, 5, 15, 35 min
        self.clock.advance(8 * 86400)                          # push outage lasts 8 days, the cloud keeps answering
        status, text = self.send_kuma()                        # tick runs the due cross-check before the heartbeat
        self.assertEqual((status, text), ("up", f"poll-only: push down since {hm(start)} UTC (8 days), retry in 30m"))

    def test_kuma_down_on_login_failure_and_up_after_recovery(self):
        self.client.login_error = push_503()
        self.b.login_and_prime()
        self.assertEqual(self.send_kuma(),
                         ("down", f"EZVIZ login failing since {hm(self.b.started_at)} UTC, retry in 1m"))
        self.client.login_error = None
        self.tick_after(61)
        self.assertTrue(self.b.logged_in)
        status, text = self.send_kuma()
        self.assertEqual(status, "up")
        self.assertTrue(text.startswith("poll-only: push down since ") and text.endswith(", connecting"), text)

    def test_kuma_down_after_three_consecutive_cross_check_failures(self):
        self.pf.fail_all = push_503
        self.b.login_and_prime()
        last_ok = self.b.last_cloud_contact
        self.client.list_error = remote_disconnected()
        for n in (1, 2):
            self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
            self.assertEqual((self.b.cloud_failures, self.b.kuma_status()[0]), (n, "up"))
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        status, text = self.send_kuma()
        self.assertEqual(status, "down")
        self.assertTrue(text.startswith(f"EZVIZ cloud failing: 3 cross-checks failed in a row, last OK {hm(last_ok)} "
                                        f"UTC; poll-only: push down since "), text)
        self.client.list_error = None
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS)
        self.assertEqual((self.b.cloud_failures, self.b.kuma_status()[0]), (0, "up"))

    def test_kuma_down_after_three_consecutive_status_poll_failures(self):
        self.pf.fail_all = push_503
        del self.b.poll                                        # use the real status poll here
        self.b.login_and_prime()
        self.assertEqual(self.b.poll_failures, 0)
        self.client.cameras_error = B.PyEzvizError("HTTP 500")  # re-login works, the device read keeps failing
        self.b.poll(force=True)
        self.b.poll(force=True)
        self.assertEqual((self.b.poll_failures, self.b.kuma_status()[0]), (2, "up"))
        self.client.cameras_error = RuntimeError("unexpected")  # not a PyEzvizError: propagates, still counted
        with self.assertRaises(RuntimeError):
            self.b.poll(force=True)
        status, text = self.b.kuma_status()
        self.assertEqual(status, "down")
        self.assertTrue(text.startswith("EZVIZ cloud failing: 3 status polls failed in a row, last OK "), text)
        self.client.cameras_error = None
        self.b.poll(force=True)
        self.assertEqual((self.b.poll_failures, self.b.kuma_status()[0]), (0, "up"))

    def test_kuma_stale_contact_is_down_but_one_failed_push_mode_check_is_not(self):
        self.b.login_and_prime()                               # push registers fine this time
        connack(self.b, self.clock)
        self.tick_after(1)
        self.tick_after(B.CLOUD_CHECK_POLL_ONLY_SECONDS - 1)   # cross-check with push up → next one in 5 min
        self.assertTrue(self.b.push_healthy())
        last_ok = self.b.last_cloud_contact
        self.client.list_error = remote_disconnected()
        self.tick_after(B.CLOUD_CHECK_PUSH_SECONDS)            # one isolated failure
        self.clock.advance(B.CLOUD_CHECK_PUSH_SECONDS + 1)     # loop a second late: next check due, not yet run
        self.assertGreater(self.clock() - last_ok, B.KUMA_CLOUD_STALE_SECONDS)
        self.assertEqual(self.b.kuma_status(), ("up", "push ok"))
        self.client.list_error = None
        self.b.tick()
        last_ok = self.b.last_cloud_contact
        self.assertEqual(last_ok, self.clock())
        self.clock.advance(2 * B.CLOUD_CHECK_PUSH_SECONDS + B.KUMA_INTERVAL_SECONDS + 1)   # nothing ran at all
        self.assertEqual(self.b.kuma_status(),
                         ("down", f"EZVIZ cloud failing: no successful contact for 11m, last OK {hm(last_ok)} UTC; "
                                  f"push ok"))


if __name__ == "__main__":
    unittest.main()
