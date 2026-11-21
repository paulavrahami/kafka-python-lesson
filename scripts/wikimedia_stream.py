"""Event-by-event Wikimedia EventStreams source for bounded or continuous labs.

The duration is checked between network reads and events. Connect/read timeouts
bound an idle request, but max_seconds is not a hard wall-clock deadline for DNS,
the operating system, or a downstream handler. The in-memory SSE cursor supports
short reconnects; it is not a durable checkpoint or a Kafka delivery guarantee.
Use max_events=None and max_seconds=None for continuous intake until cancellation.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from contextlib import closing
from datetime import datetime
from email.utils import parsedate_to_datetime
import json
import math
import os
import sys
import time

ENDPOINT = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_USER_AGENT = (
    "KafkaPythonLesson/1.1 (+https://github.com/paulavrahami/kafka-python-lesson)"
)
MAX_EVENT_BYTES = 262144
MAX_LINE_BYTES = 262144
CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 3.0
DEFAULT_DEDUP_CAPACITY = 10000


class WikimediaStreamError(RuntimeError):
    """An explicit source failure, with a short machine-readable stage."""

    def __init__(self, stage, message):
        self.stage = stage
        super().__init__(f"Wikimedia {stage}: {message}")


def parse_sse_blocks(lines, deadline=None, max_lines=200000, *, state=None,
                     max_event_bytes=MAX_EVENT_BYTES, stop_event=None):
    """Yield complete SSE data frames; comments and control-only frames do not yield.

    ``state`` receives the complete-frame cursor and the server's retry hint.
    A cursor from an incomplete final frame is deliberately not advanced, so a
    reconnect can replay data rather than silently skip an incomplete event.
    """
    if max_lines is not None and (not isinstance(max_lines, int)
                                 or isinstance(max_lines, bool) or max_lines <= 0):
        raise ValueError("max_lines must be a positive integer or None")
    state = state if state is not None else {}
    cursor = state.get("last_event_id", "")
    data, event_type, size = [], "message", 0
    first_line = True
    for number, raw_line in enumerate(lines, 1):
        if max_lines is not None and number > max_lines:
            raise WikimediaStreamError("line_limit", "SSE line limit exceeded")
        if ((deadline is not None and time.monotonic() >= deadline)
                or (stop_event is not None and stop_event.is_set())):
            return
        line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else raw_line
        if not isinstance(line, str):
            raise TypeError("SSE lines must be strings or bytes")
        line = line.rstrip("\r\n")
        if first_line:
            line = line.removeprefix("\ufeff")
            first_line = False
        if len(line.encode("utf-8")) > MAX_LINE_BYTES:
            raise WikimediaStreamError("line_size", "SSE line exceeds the lab limit")
        if line == "":
            state["last_event_id"] = cursor
            if data:
                yield {"event": event_type or "message", "data": "\n".join(data), "id": cursor}
            data, event_type, size = [], "message", 0
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            size += len(value.encode("utf-8")) + 1
            if size > max_event_bytes:
                raise WikimediaStreamError("event_size", "SSE event exceeds the lab limit")
            data.append(value)
        elif field == "event":
            event_type = value
        elif field == "id" and "\0" not in value:
            cursor = value
        elif field == "retry" and value.isascii() and value.isdigit():
            state["retry_ms"] = int(value)
    # EOF does not dispatch a data frame without its blank-line separator.


def _response_lines(response, deadline, stop_event):
    """Split CR/LF/CRLF incrementally, enforcing size before an entire line arrives."""
    pending = bytearray()
    after_cr = False
    for chunk in response.iter_content(chunk_size=1024):
        if ((deadline is not None and time.monotonic() >= deadline)
                or (stop_event is not None and stop_event.is_set())):
            return
        for value in chunk:
            if after_cr:
                after_cr = False
                if value == 10:
                    continue
            if value in (10, 13):
                yield bytes(pending)
                pending.clear()
                after_cr = value == 13
            else:
                pending.append(value)
                if len(pending) > MAX_LINE_BYTES:
                    raise WikimediaStreamError("line_size", "SSE line exceeds the lab limit")
    if pending:
        yield bytes(pending)


def _validated_event(data):
    event = json.loads(data)
    if not isinstance(event, dict):
        raise ValueError("Expected an object")
    meta = event.get("meta")
    if not isinstance(meta, dict):
        raise ValueError("Expected meta object")
    if meta.get("domain") == "canary":
        return None
    if not isinstance(meta.get("id"), str) or not meta["id"]:
        raise ValueError("Expected nonempty meta.id")
    if not isinstance(event.get("server_name"), str) or not event["server_name"]:
        raise ValueError("Expected server_name")
    if not isinstance(event.get("bot"), bool):
        raise ValueError("Expected boolean bot")
    stamp = meta.get("dt")
    if not isinstance(stamp, str):
        raise ValueError("Expected timestamp string")
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Expected timezone-aware timestamp")
    parsed.timestamp()
    return event


def _retry_delay(response, state):
    raw = response.headers.get("Retry-After", "") if response is not None else ""
    # A server's requested delay must never be shortened to reconnect early.
    if raw:
        try:
            seconds = (float(raw) if raw.isascii() and raw.isdigit()
                       else parsedate_to_datetime(raw).timestamp() - time.time())
            return max(0.25, seconds)
        except (TypeError, ValueError, OverflowError):
            pass
    try:
        seconds = float(state.get("retry_ms", 1000)) / 1000
    except (TypeError, ValueError, OverflowError):
        seconds = 1.0
    if math.isnan(seconds):
        seconds = 1.0
    return max(0.25, seconds)


def iter_wikimedia_events(*, max_events=100, max_seconds=60, server_name=None,
                          user_agent=None, max_reconnects=2, stats=None,
                          stop_event=None, dedup_capacity=DEFAULT_DEDUP_CAPACITY):
    """Yield accepted events immediately, with optional total count/time limits.

    Set both limits to None to run until stop_event, generator close, an interrupt,
    or an explicit failure. HTTP connection/read timeouts remain finite. The retry
    budget applies to consecutive disconnects without a validated ordinary event;
    source progress resets that budget. ``reconnects`` counts all reconnects.

    ``server_name`` is an optional exact client-side filter, such as
    ``he.wikipedia.org``; it does not reduce the upstream HTTP traffic. Duplicate
    meta.id values are ignored within this run. With a finite max_events, at most
    max_events IDs are retained, preserving deduplication across the bounded run.
    With max_events=None, an LRU cache retains only dedup_capacity recent IDs
    (default 10,000). An evicted ID can be accepted again; downstream storage
    should use a stable event ID rather than assume global exactly-once delivery.

    Close the generator if a downstream consumer stops early. No synthetic
    fallback, disk cursor, full-event log, or background thread is created here.
    """
    if max_events is not None and (not isinstance(max_events, int)
                                  or isinstance(max_events, bool) or max_events <= 0):
        raise ValueError("max_events must be a positive integer or None")
    if max_seconds is not None and (isinstance(max_seconds, bool)
                                   or not isinstance(max_seconds, (int, float))
                                   or not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ValueError("max_seconds must be positive and finite or None")
    if (not isinstance(max_reconnects, int) or isinstance(max_reconnects, bool)
            or max_reconnects < 0):
        raise ValueError("max_reconnects must be a nonnegative integer")
    if (not isinstance(dedup_capacity, int) or isinstance(dedup_capacity, bool)
            or dedup_capacity <= 0):
        raise ValueError("dedup_capacity must be a positive integer")
    if server_name is not None and (not isinstance(server_name, str) or not server_name.strip()):
        raise ValueError("server_name must be a nonempty exact hostname or None")
    agent = user_agent or os.environ.get("WIKIMEDIA_USER_AGENT") or DEFAULT_USER_AGENT
    if not isinstance(agent, str) or not agent.strip() or any(c in agent for c in "\r\n"):
        raise ValueError("user_agent must be a nonempty, single-line string")
    try:
        import requests
    except ImportError as exc:
        raise WikimediaStreamError("dependency", "requests is missing; run uv sync --locked") from exc
    report = stats if stats is not None else {}
    report.update(frames=0, received=0, invalid=0, canary=0, filtered=0,
                  duplicates=0, ignored=0, reconnects=0, connections=0,
                  last_event_id="", stop_reason="running", elapsed_seconds=0.0,
                  consecutive_reconnects=0, dedup_cache_size=0, dedup_evictions=0,
                  dedup_capacity=max_events if max_events is not None else dedup_capacity)
    started = time.monotonic()
    deadline = started + max_seconds if max_seconds is not None else None
    state, seen = {}, OrderedDict()
    transient = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                 requests.exceptions.ChunkedEncodingError)
    try:
        with requests.Session() as session:
            while True:
                if stop_event is not None and stop_event.is_set():
                    report["stop_reason"] = "cancelled"
                    return
                remaining = deadline - time.monotonic() if deadline is not None else float("inf")
                if remaining <= 0:
                    report["stop_reason"] = "time_limit"
                    return
                headers = {"Accept": "text/event-stream", "User-Agent": agent}
                if state.get("last_event_id"):
                    headers["Last-Event-ID"] = state["last_event_id"]
                delay = _retry_delay(None, state)
                retry_cause = "stream ended before the configured limits"
                try:
                    report["connections"] += 1
                    with session.get(ENDPOINT, headers=headers, stream=True,
                                     timeout=(min(CONNECT_TIMEOUT, remaining),
                                              min(READ_TIMEOUT, remaining))) as response:
                        status = response.status_code
                        if status == 429 or 500 <= status <= 599:
                            delay = _retry_delay(response, state)
                            retry_cause = f"transient HTTP {status}"
                        elif status != 200:
                            raise WikimediaStreamError("http", f"HTTP {status}; no synthetic fallback was used")
                        else:
                            mime = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                            if mime != "text/event-stream":
                                raise WikimediaStreamError("content_type", "expected text/event-stream")
                            blocks = parse_sse_blocks(
                                _response_lines(response, deadline, stop_event),
                                deadline=deadline, state=state, stop_event=stop_event,
                                max_lines=None,
                            )
                            for block in blocks:
                                report["frames"] += 1
                                report["last_event_id"] = state.get("last_event_id", "")
                                if block["event"] == "error":
                                    raise WikimediaStreamError("server_event", "server sent an SSE error event")
                                if block["event"] != "message":
                                    report["ignored"] += 1
                                    continue
                                try:
                                    event = _validated_event(block["data"])
                                except (ValueError, TypeError, KeyError, OverflowError, OSError):
                                    report["invalid"] += 1
                                    continue
                                if event is None:
                                    report["canary"] += 1
                                    continue
                                report["consecutive_reconnects"] = 0
                                if server_name is not None and event["server_name"] != server_name:
                                    report["filtered"] += 1
                                    continue
                                event_id = event["meta"]["id"]
                                if event_id in seen:
                                    seen.move_to_end(event_id)
                                    report["duplicates"] += 1
                                    continue
                                seen[event_id] = None
                                if len(seen) > report["dedup_capacity"]:
                                    seen.popitem(last=False)
                                    report["dedup_evictions"] += 1
                                report["dedup_cache_size"] = len(seen)
                                report["received"] += 1
                                report["elapsed_seconds"] = round(time.monotonic() - started, 3)
                                yield event
                                if max_events is not None and report["received"] >= max_events:
                                    report["stop_reason"] = "max_events"
                                    return
                            report["last_event_id"] = state.get("last_event_id", "")
                            delay = _retry_delay(None, state)
                except requests.exceptions.SSLError as exc:
                    raise WikimediaStreamError("tls", "TLS validation failed") from exc
                except transient as exc:
                    retry_cause = type(exc).__name__
                report["last_disconnect"] = retry_cause
                if stop_event is not None and stop_event.is_set():
                    report["stop_reason"] = "cancelled"
                    return
                remaining = deadline - time.monotonic() if deadline is not None else float("inf")
                if remaining <= 0:
                    report["stop_reason"] = "time_limit"
                    return
                if report["consecutive_reconnects"] >= max_reconnects:
                    raise WikimediaStreamError("reconnect_limit", f"retry budget exhausted after {retry_cause}")
                if not math.isfinite(delay) or delay >= remaining:
                    raise WikimediaStreamError(
                        "retry_wait", "server retry delay exceeds the remaining lab duration; no early reconnect was attempted"
                    )
                report["reconnects"] += 1
                report["consecutive_reconnects"] += 1
                if stop_event is not None:
                    stop_event.wait(delay)
                else:
                    time.sleep(delay)
    except GeneratorExit:
        report["stop_reason"] = "closed_by_consumer"
        raise
    except KeyboardInterrupt:
        report["stop_reason"] = "interrupted"
        raise
    except requests.exceptions.RequestException as exc:
        report["stop_reason"] = "error"
        report["error_stage"] = "http_client"
        raise WikimediaStreamError("http_client", type(exc).__name__) from exc
    except Exception as exc:
        report["stop_reason"] = "error"
        report["error_stage"] = getattr(exc, "stage", type(exc).__name__)
        raise
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["last_event_id"] = state.get("last_event_id", "")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Test the live Wikimedia SSE source without Kafka")
    parser.add_argument("--max-events", type=int, default=20)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--server-name", help="Exact client-side filter, e.g. he.wikipedia.org")
    parser.add_argument("--max-reconnects", type=int, default=2)
    parser.add_argument("--continuous", action="store_true",
                        help="Run without total event/time limits until Ctrl+C")
    args = parser.parse_args(argv)
    stats = {}
    code = 0
    try:
        with closing(iter_wikimedia_events(
            max_events=None if args.continuous else args.max_events,
            max_seconds=None if args.continuous else args.seconds,
            server_name=args.server_name, max_reconnects=args.max_reconnects,
            stats=stats,
        )) as events:
            for index, event in enumerate(events, 1):
                if index <= 3 or index % 10 == 0:
                    print(json.dumps({"received": index, "server_name": event["server_name"],
                                      "event_time": event["meta"]["dt"], "bot": event["bot"]}), flush=True)
    except KeyboardInterrupt:
        code = 130
    except (WikimediaStreamError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        code = 1
    safe_stats = {key: value for key, value in stats.items() if key != "last_event_id"}
    safe_stats["has_resume_cursor"] = bool(stats.get("last_event_id"))
    print(json.dumps(safe_stats, indent=2))
    return code or (0 if stats.get("received", 0) else 2)


if __name__ == "__main__":
    raise SystemExit(main())
