"""Protocol and failure-boundary tests for Wikimedia SSE; no public network required."""
import contextlib
import io
import json
from pathlib import Path
import sys
import threading
import unittest
import requests
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import wikimedia_stream as stream


def event(identifier="e1", server="he.wikipedia.org"):
    return {"meta": {"id": identifier, "dt": "2026-11-21T12:00:02Z", "domain": server},
            "server_name": server, "bot": False, "title": "כותרת בעברית"}


def frame(value=None, identifier="cursor-1", event_type="message"):
    data = json.dumps(event() if value is None else value, ensure_ascii=False)
    return f"id: {identifier}\nevent: {event_type}\ndata: {data}\n\n".encode()


class Response:
    def __init__(self, chunks=(), status=200, mime="text/event-stream; charset=utf-8", headers=None):
        self.chunks = list(chunks)
        self.status_code = status
        self.headers = {"Content-Type": mime, **(headers or {})}
        self.closed = False
        self.iterated_chunks = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def iter_content(self, chunk_size):
        for chunk in self.chunks:
            self.iterated_chunks += 1
            if isinstance(chunk, Exception):
                raise chunk
            if callable(chunk):
                chunk = chunk()
            yield chunk


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("Unexpected extra network request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ParserTests(unittest.TestCase):
    def test_multiline_unicode_comments_and_bom(self):
        lines = ["\ufeff: heartbeat", "event: message", "id: cursor", "retry: 500",
                 'data: {"title":', 'data: "עברית"}', ""]
        state = {}
        result = list(stream.parse_sse_blocks(lines, state=state))
        self.assertEqual(json.loads(result[0]["data"]), {"title": "עברית"})
        self.assertEqual(result[0]["id"], "cursor")
        self.assertEqual(state, {"last_event_id": "cursor", "retry_ms": 500})

    def test_incomplete_frame_is_not_dispatched_or_checkpointed(self):
        state = {"last_event_id": "saved"}
        self.assertEqual(list(stream.parse_sse_blocks(["id: incomplete", "data: {}"], state=state)), [])
        self.assertEqual(state["last_event_id"], "saved")

    def test_control_frames_empty_id_and_invalid_retry(self):
        state = {"last_event_id": "old", "retry_ms": 1000}
        blocks = list(stream.parse_sse_blocks([
            "id: new", "retry: -2", "", "id:", "retry: ١٢", "",
            "id: ignored\0value", "data: ok", "",
        ], state=state))
        self.assertEqual(blocks, [{"event": "message", "data": "ok", "id": ""}])
        self.assertEqual(state, {"last_event_id": "", "retry_ms": 1000})

    def test_empty_data_dispatch_and_unknown_fields(self):
        self.assertEqual(list(stream.parse_sse_blocks(["foo: x", "data:", ""])),
                         [{"event": "message", "data": "", "id": ""}])

    def test_event_type_resets_between_frames(self):
        blocks = list(stream.parse_sse_blocks(["event: custom", "data: a", "", "data: b", ""]))
        self.assertEqual([b["event"] for b in blocks], ["custom", "message"])

    def test_event_and_line_budgets_fail_explicitly(self):
        with self.assertRaisesRegex(stream.WikimediaStreamError, "event_size"):
            list(stream.parse_sse_blocks(["data: אבג", ""], max_event_bytes=5))
        with self.assertRaisesRegex(stream.WikimediaStreamError, "line_limit"):
            list(stream.parse_sse_blocks([": comment"] * 3, max_lines=2))
        with self.assertRaisesRegex(stream.WikimediaStreamError, "line_size"):
            list(stream.parse_sse_blocks(["data: " + "a" * stream.MAX_LINE_BYTES]))

    def test_network_line_splitter_handles_cr_lf_and_split_utf8(self):
        raw = "data: עברית\r\ndata: שוב\r\rdata: tail\n\n".encode()
        response = Response([bytes([value]) for value in raw])
        lines = list(stream._response_lines(response, float("inf"), None))
        blocks = list(stream.parse_sse_blocks(lines))
        self.assertEqual([b["data"] for b in blocks], ["עברית\nשוב", "tail"])

    def test_oversized_unterminated_network_line_fails_before_eof(self):
        response = Response([b"x" * 100, b"y" * 100, b"z"])
        with patch.object(stream, "MAX_LINE_BYTES", 150):
            with self.assertRaisesRegex(stream.WikimediaStreamError, "line_size"):
                list(stream._response_lines(response, float("inf"), None))
        self.assertEqual(response.iterated_chunks, 2)


class SourceTests(unittest.TestCase):
    def collect(self, responses, **kwargs):
        session = Session(*responses)
        stats = {}
        with patch.object(requests, "Session", return_value=session), patch.object(stream.time, "sleep") as sleep:
            records = list(stream.iter_wikimedia_events(stats=stats, **kwargs))
        return records, stats, session, sleep

    def test_event_is_yielded_before_later_frames_are_read(self):
        response = Response([frame(event("e1")), frame(event("e2"), "cursor-2")])
        session = Session(response)
        stats = {}
        with patch.object(requests, "Session", return_value=session):
            iterator = stream.iter_wikimedia_events(max_events=2, stats=stats)
            self.assertEqual(next(iterator)["meta"]["id"], "e1")
            self.assertEqual(response.iterated_chunks, 1)
            self.assertFalse(response.closed)
            self.assertEqual(next(iterator)["meta"]["id"], "e2")
            with self.assertRaises(StopIteration):
                next(iterator)
        self.assertTrue(response.closed)
        self.assertTrue(session.closed)
        self.assertEqual(stats["stop_reason"], "max_events")

    def test_validation_canary_filter_and_duplicate_counters(self):
        invalid = event("bad")
        invalid["bot"] = "false"
        chunks = [b"data: not-json\n\n", frame({"meta": {"domain": "canary"}}),
                  frame(invalid), frame(event("fr", "fr.wikipedia.org")),
                  frame(event("e1")), frame(event("e1")), frame(event("e2"))]
        records, stats, session, _ = self.collect([Response(chunks)], max_events=2,
                                                  server_name="he.wikipedia.org")
        self.assertEqual([e["meta"]["id"] for e in records], ["e1", "e2"])
        self.assertEqual({k: stats[k] for k in ["frames", "received", "invalid", "canary", "filtered", "duplicates"]},
                         {"frames": 7, "received": 2, "invalid": 2, "canary": 1, "filtered": 1, "duplicates": 1})
        config = session.calls[0][1]
        self.assertTrue(config["stream"])
        self.assertEqual(config["headers"]["Accept"], "text/event-stream")
        self.assertIn("github.com/paulavrahami", config["headers"]["User-Agent"])
        self.assertNotIn("params", config)  # Filtering is explicitly client-side.

    def test_reconnect_resumes_after_complete_frame_and_deduplicates(self):
        first = Response([frame(event("e1"), "cursor-1"), requests.exceptions.ConnectionError("disconnect")])
        second = Response([frame(event("e1"), "cursor-1"), frame(event("e2"), "cursor-2")])
        records, stats, session, sleep = self.collect([first, second], max_events=2)
        self.assertEqual(len(records), 2)
        self.assertEqual(session.calls[1][1]["headers"]["Last-Event-ID"], "cursor-1")
        self.assertEqual((stats["reconnects"], stats["duplicates"], stats["last_event_id"]), (1, 1, "cursor-2"))
        self.assertTrue(first.closed and second.closed)
        sleep.assert_called_once_with(1.0)

    def test_incomplete_second_event_does_not_advance_reconnect_cursor(self):
        first = Response([frame(event("e1"), "cursor-1"), b"id: cursor-2\ndata: incomplete",
                          requests.exceptions.ChunkedEncodingError("disconnect")])
        second = Response([frame(event("e2"), "cursor-2")])
        records, _, session, _ = self.collect([first, second], max_events=2)
        self.assertEqual(len(records), 2)
        self.assertEqual(session.calls[1][1]["headers"]["Last-Event-ID"], "cursor-1")

    def test_sse_retry_and_http_retry_after_are_respected(self):
        for first in (Response([b"retry: 7000\n\n"]),
                      Response(status=429, headers={"Retry-After": "7"})):
            with self.subTest(first=first):
                records, stats, _, sleep = self.collect([first, Response([frame()])], max_events=1)
                self.assertEqual(len(records), 1)
                self.assertEqual(stats["reconnects"], 1)
                sleep.assert_called_once_with(7.0)

    def test_long_server_retry_delay_does_not_trigger_early_reconnect(self):
        for first in (Response([b"retry: 999999\n\n"]),
                      Response(status=429, headers={"Retry-After": "120"})):
            with self.subTest(first=first):
                session = Session(first)
                with patch.object(requests, "Session", return_value=session), patch.object(stream.time, "sleep") as sleep:
                    with self.assertRaisesRegex(stream.WikimediaStreamError, "retry_wait"):
                        list(stream.iter_wikimedia_events(max_seconds=30))
                self.assertEqual(len(session.calls), 1)
                sleep.assert_not_called()

    def test_http_date_retry_after_is_supported(self):
        first = Response(status=503, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"})
        with patch.object(stream.time, "time", return_value=1445412473):
            records, _, _, sleep = self.collect([first, Response([frame()])], max_events=1)
        self.assertEqual(len(records), 1)
        sleep.assert_called_once_with(7.0)

    def test_non_sse_success_response_fails_without_fallback(self):
        response = Response([b"<html>not a stream</html>"], mime="text/html")
        session = Session(response)
        stats = {}
        with patch.object(requests, "Session", return_value=session):
            with self.assertRaisesRegex(stream.WikimediaStreamError, "content_type"):
                list(stream.iter_wikimedia_events(stats=stats))
        self.assertEqual(stats["received"], 0)
        self.assertEqual(stats["stop_reason"], "error")
        self.assertTrue(response.closed and session.closed)
        self.assertEqual(len(session.calls), 1)

    def test_permanent_http_and_tls_errors_are_not_retried(self):
        for first, stage in ((Response(status=403), "http"),
                             (requests.exceptions.SSLError("certificate problem"), "tls")):
            with self.subTest(stage=stage):
                session = Session(first)
                with patch.object(requests, "Session", return_value=session), patch.object(stream.time, "sleep") as sleep:
                    with self.assertRaisesRegex(stream.WikimediaStreamError, stage):
                        list(stream.iter_wikimedia_events())
                self.assertEqual(len(session.calls), 1)
                sleep.assert_not_called()

    def test_explicit_sse_error_is_not_silently_ignored(self):
        session = Session(Response([frame({"error": "server error"}, event_type="error")]))
        with patch.object(requests, "Session", return_value=session):
            with self.assertRaisesRegex(stream.WikimediaStreamError, "server_event"):
                list(stream.iter_wikimedia_events())

    def test_retry_budget_is_finite_and_partial_count_is_preserved(self):
        first = Response([frame(), requests.exceptions.ReadTimeout("idle")])
        second = Response(status=503)
        session = Session(first, second)
        stats = {}
        with patch.object(requests, "Session", return_value=session), patch.object(stream.time, "sleep"):
            with self.assertRaisesRegex(stream.WikimediaStreamError, "reconnect_limit"):
                list(stream.iter_wikimedia_events(max_events=2, max_reconnects=1, stats=stats))
        self.assertEqual((stats["received"], stats["reconnects"]), (1, 1))
        self.assertTrue(first.closed and second.closed and session.closed)

    def test_time_limit_stops_quiet_stream_without_reconnecting(self):
        now = [0.0]
        def late_chunk():
            now[0] = 60
            return b": heartbeat\n\n"
        response = Response([late_chunk])
        with patch.object(stream.time, "monotonic", side_effect=lambda: now[0]):
            records, stats, session, _ = self.collect([response], max_seconds=10)
        self.assertEqual(records, [])
        self.assertEqual(stats["stop_reason"], "time_limit")
        self.assertEqual(len(session.calls), 1)
        self.assertTrue(response.closed)

    def test_pre_cancelled_stream_makes_no_http_request(self):
        stop = threading.Event()
        stop.set()
        records, stats, session, _ = self.collect([], stop_event=stop)
        self.assertEqual(records, [])
        self.assertEqual(stats["stop_reason"], "cancelled")
        self.assertEqual(session.calls, [])

    def test_generator_close_releases_response_and_session(self):
        response = Response([frame(), frame(event("later"))])
        session = Session(response)
        stats = {}
        with patch.object(requests, "Session", return_value=session):
            iterator = stream.iter_wikimedia_events(stats=stats)
            next(iterator)
            iterator.close()
        self.assertTrue(response.closed and session.closed)
        self.assertEqual(stats["stop_reason"], "closed_by_consumer")
        self.assertEqual(response.iterated_chunks, 1)

    def test_invalid_limits_fail_before_network_access(self):
        options = [{"max_events": 0}, {"max_events": True}, {"max_seconds": 0},
                   {"max_seconds": float("inf")}, {"max_reconnects": -1},
                   {"user_agent": "bad\nheader"}, {"server_name": ""}]
        with patch.object(requests, "Session") as factory:
            for config in options:
                with self.subTest(config=config), self.assertRaises(ValueError):
                    list(stream.iter_wikimedia_events(**config))
        factory.assert_not_called()

    def test_schema_requires_timezone_and_rejects_bad_meta(self):
        invalid = [[], {"meta": []}, event(""), event("naive"), event("bad-date")]
        invalid[-2]["meta"]["dt"] = "2026-11-21T12:00:00"
        invalid[-1]["meta"]["dt"] = "not-a-date"
        records, stats, _, _ = self.collect([Response([frame(x) for x in invalid] + [frame()])], max_events=1)
        self.assertEqual(len(records), 1)
        self.assertEqual(stats["invalid"], len(invalid))

    def test_cli_does_not_print_personal_event_fields(self):
        record = event()
        record.update(user="person", comment="personal note")
        session = Session(Response([frame(record)]))
        output = io.StringIO()
        with patch.object(requests, "Session", return_value=session), contextlib.redirect_stdout(output):
            code = stream.main(["--max-events", "1"])
        self.assertEqual(code, 0)
        self.assertNotIn("personal note", output.getvalue())
        self.assertNotIn("person", output.getvalue())
        self.assertNotIn(record["title"], output.getvalue())
        self.assertIn('"has_resume_cursor": true', output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
