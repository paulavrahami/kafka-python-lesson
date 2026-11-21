"""A bounded, incremental Wikimedia -> Kafka -> Python classroom pipeline.

Create a fresh topic with the notebook's ``ensure_topics`` before calling this
module. The caller owns one Producer; a worker owns one Consumer and all the
aggregation state. Source rows are never accumulated into a list first.

This is a teaching example, not durable stream processing: counters and the
deduplication set live only for one bounded run. Kafka commits do not atomically
persist those counters. There is no exactly-once claim or saved SSE checkpoint.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from itertools import islice
import json
import math
import threading
import time
import uuid


class StreamingPipelineError(RuntimeError):
    """A failed run, with its partial measurements available as ``report``."""

    def __init__(self, stage, report):
        self.stage = stage
        self.report = report
        super().__init__({"stage": stage, **report})


def _client_config(bootstrap_servers):
    return {
        "bootstrap.servers": bootstrap_servers,
        "broker.address.family": "v4",
        "socket.timeout.ms": 10000,
        "socket.connection.setup.timeout.ms": 5000,
        "allow.auto.create.topics": False,
    }


def _event_fields(event):
    if not isinstance(event, dict) or not isinstance(event.get("meta"), dict):
        raise ValueError("Expected an event object with meta")
    identity, site, marker = event["meta"].get("id"), event.get("server_name"), event.get("bot")
    if not isinstance(identity, str) or not identity:
        raise ValueError("Expected nonempty string meta.id")
    if not isinstance(site, str) or not site:
        raise ValueError("Expected nonempty string server_name")
    if not isinstance(marker, bool):
        raise ValueError("Expected boolean bot")
    stamp = event["meta"].get("dt")
    if not isinstance(stamp, str):
        raise ValueError("Expected a timestamp string in meta.dt")
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Event timestamp must contain a timezone")
    return identity, site, "bot" if marker else "non-bot", int(parsed.timestamp() * 1000)


def _utc(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()


def _window_rows(counts, width_ms):
    return [
        {"key": key, "start_ms": start, "start": _utc(start),
         "end": _utc(start + width_ms), "count": count}
        for (key, start), count in sorted(counts.items())
    ]


def run_streaming_pipeline(
    events,
    *,
    topic,
    bootstrap_servers="127.0.0.1:9092",
    group_id=None,
    stop_event=None,
    on_progress=None,
    progress_every=10,
    drain_seconds=15,
    max_events=1000,
    max_pending=1000,
    queue_wait_seconds=5,
    delivery_timeout_seconds=30,
):
    """Publish and process events while the source iterator is still running.

    ``events`` must be bounded or observe ``stop_event``; the Wikimedia iterator
    does both. At most ``max_events`` rows are requested. A fresh topic is required
    for each invocation. A per-run message header detects unexpected old records.
    ``on_progress(report)`` may run in the Consumer thread, so it must be quick
    and thread-safe (``print`` is suitable for the notebook).

    A successful report has nonzero, equal accepted/confirmed/consumed/committed
    counts. Unique-event counts may be smaller when the input repeats meta.id.
    Errors raise ``StreamingPipelineError`` with a partial report. Ctrl+C stays
    a KeyboardInterrupt, with the partial report in ``streaming_report``.

    Deadlines are cooperative. Native Kafka calls retain their configured
    timeouts; shutdown waits up to 12 additional seconds for the worker to close.
    """
    for name, value in (("max_events", max_events), ("max_pending", max_pending),
                        ("progress_every", progress_every)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if max_events > 100000 or max_pending > 10000:
        raise ValueError("This bounded classroom runner allows at most 100000 events and 10000 pending messages")
    for name, value in (("drain_seconds", drain_seconds), ("queue_wait_seconds", queue_wait_seconds),
                        ("delivery_timeout_seconds", delivery_timeout_seconds)):
        if (not isinstance(value, (int, float)) or isinstance(value, bool)
                or not math.isfinite(value) or value <= 0):
            raise ValueError(f"{name} must be finite and positive")
    if not isinstance(topic, str) or not topic or not isinstance(bootstrap_servers, str) or not bootstrap_servers:
        raise ValueError("topic and bootstrap_servers must be nonempty strings")

    from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING, Producer, TopicPartition

    source_stop = stop_event if stop_event is not None else threading.Event()
    if source_stop.is_set():
        raise ValueError("stop_event is already set; use a fresh Event for a new run")
    consumer_stop, production_done, ready, consumer_done = (threading.Event() for _ in range(4))
    lock = threading.RLock()
    started = time.monotonic()
    run_id = uuid.uuid4().hex
    marker = run_id.encode("ascii")
    group = group_id or f"python-streaming-{run_id[:12]}"
    state = {
        "accepted": 0, "confirmed": 0, "delivery_failed": 0, "consumed": 0,
        "committed": 0, "unique_processed": 0, "duplicates": 0,
        "source_active": True, "remaining": 0, "queue_retries": 0,
        "processed_while_source_open": 0, "first_source_seconds": None,
        "first_consumer_seconds": None, "source_finished_seconds": None,
        "delivery_errors": [], "broker_errors": [], "worker_errors": [],
    }
    bot_counts, site_counts, time_counts, seen = Counter(), Counter(), Counter(), set()
    producer, worker, iterator = None, None, None
    primary_error = None
    stage, stop_reason = "initialization", "source_exhausted"

    def report():
        with lock:
            snapshot = {key: list(value) if isinstance(value, list) else value for key, value in state.items()}
            snapshot.update(
                topic=topic, group_id=group, run_id=run_id,
                complete=False,
                elapsed_seconds=round(time.monotonic() - started, 3),
                bot_counts=dict(sorted(bot_counts.items())),
                site_rows=_window_rows(site_counts, 60000),
                time_rows=_window_rows(time_counts, 10000),
            )
            snapshot["in_flight"] = max(0, state["accepted"] - state["confirmed"] - state["delivery_failed"])
            snapshot["consumer_lag_for_run"] = max(0, state["confirmed"] - state["committed"])
        return snapshot

    def notify():
        if on_progress is not None:
            on_progress(report())

    def broker_error(error):
        with lock:
            state["broker_errors"].append(str(error))
            del state["broker_errors"][:-8]

    def delivered(error, message):
        with lock:
            if error is None:
                state["confirmed"] += 1
            else:
                state["delivery_failed"] += 1
                state["delivery_errors"].append(str(error))
                del state["delivery_errors"][:-8]
                source_stop.set()

    def consume(partition_ids):
        consumer = None
        try:
            consumer = Consumer({
                **_client_config(bootstrap_servers), "group.id": group,
                "enable.auto.commit": False, "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
            })
            consumer.assign([TopicPartition(topic, part, OFFSET_BEGINNING) for part in partition_ids])
            ready.set()
            while not consumer_stop.is_set():
                with lock:
                    finished = production_done.is_set() and state["consumed"] >= state["accepted"]
                if finished:
                    break
                message = consumer.poll(0.1)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(error)
                headers = dict(message.headers() or [])
                if headers.get("lesson_run_id") != marker:
                    raise ValueError("Unexpected records in the topic; use a fresh topic for every streaming run")
                event = json.loads(message.value())
                identity, site, category, millis = _event_fields(event)
                with lock:
                    state["consumed"] += 1
                    if state["first_consumer_seconds"] is None:
                        state["first_consumer_seconds"] = round(time.monotonic() - started, 3)
                    if identity in seen:
                        state["duplicates"] += 1
                    else:
                        seen.add(identity)
                        bot_counts[category] += 1
                        site_counts[(site, millis // 60000 * 60000)] += 1
                        time_counts[("all", millis // 10000 * 10000)] += 1
                        state["unique_processed"] += 1
                committed = consumer.commit(message=message, asynchronous=False)
                if not committed or any(part.error is not None for part in committed):
                    raise RuntimeError(f"Kafka commit did not succeed: {committed}")
                with lock:
                    state["committed"] += 1
                    if state["source_active"]:
                        state["processed_while_source_open"] += 1
                    should_notify = state["committed"] % progress_every == 0
                if should_notify:
                    notify()
        except BaseException as exc:
            with lock:
                state["worker_errors"].append(f"{type(exc).__name__}: {exc}")
            source_stop.set()
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as exc:
                    with lock:
                        state["worker_errors"].append(f"consumer_close: {type(exc).__name__}: {exc}")
                    source_stop.set()
            ready.set()
            consumer_done.set()

    try:
        stage = "broker_metadata"
        producer = Producer({
            **_client_config(bootstrap_servers), "enable.idempotence": True, "acks": "all",
            "delivery.timeout.ms": int(delivery_timeout_seconds * 1000),
            "queue.buffering.max.messages": max_pending, "queue.buffering.max.kbytes": 1024,
            "error_cb": broker_error,
        })
        metadata = producer.list_topics(timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None or not topic_metadata.partitions:
            raise RuntimeError("Topic is not ready; create a fresh topic with ensure_topics before streaming")
        if any(part.error is not None or part.leader < 0 for part in topic_metadata.partitions.values()):
            raise RuntimeError("Topic partitions do not have available leaders")
        stage = "consumer_startup"
        worker = threading.Thread(target=consume, args=(sorted(topic_metadata.partitions),),
                                  name=f"kafka-lesson-{run_id[:8]}", daemon=True)
        worker.start()
        if not ready.wait(12) or state["worker_errors"]:
            raise RuntimeError("Consumer could not start; inspect worker_errors")
        iterator = iter(events)
        stage = "source"
        for event in islice(iterator, max_events):
            with lock:
                if state["first_source_seconds"] is None:
                    state["first_source_seconds"] = round(time.monotonic() - started, 3)
            if state["worker_errors"]:
                stage = "consumer"
                raise RuntimeError("Consumer failed while the source was active")
            if state["delivery_errors"]:
                stage = "delivery"
                raise RuntimeError("Kafka reported a failed delivery")
            if source_stop.is_set():
                stop_reason = "cancelled"
                break
            _, site, _, millis = _event_fields(event)
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            queue_deadline = time.monotonic() + queue_wait_seconds
            while True:
                try:
                    producer.produce(topic=topic, key=site.encode("utf-8"), value=payload,
                                     timestamp=millis, headers=[("lesson_run_id", marker)], on_delivery=delivered)
                    with lock:
                        state["accepted"] += 1
                    break
                except BufferError:
                    if time.monotonic() >= queue_deadline:
                        stage = "producer_backpressure"
                        raise RuntimeError("Producer queue stayed full; the event was not silently dropped")
                    with lock:
                        state["queue_retries"] += 1
                    producer.poll(min(0.1, max(0, queue_deadline - time.monotonic())))
                    if source_stop.is_set():
                        stage = "delivery" if state["delivery_errors"] else "consumer"
                        raise RuntimeError("Streaming stopped while waiting for producer queue capacity")
            producer.poll(0)
        if state["accepted"] >= max_events:
            stop_reason = "max_events"
        elif source_stop.is_set() and not state["worker_errors"] and not state["delivery_errors"]:
            stop_reason = "cancelled"
    except KeyboardInterrupt as exc:
        primary_error, stage, stop_reason = exc, "interrupted", "interrupted"
        source_stop.set()
    except Exception as exc:
        primary_error = exc
        source_stop.set()
    finally:
        with lock:
            state["source_active"] = False
            state["source_finished_seconds"] = round(time.monotonic() - started, 3)
        source = iterator if iterator is not None else events
        if hasattr(source, "close"):
            try:
                source.close()
            except Exception as exc:
                if primary_error is None:
                    primary_error, stage = exc, "source_close"
                source_stop.set()
        if producer is not None:
            try:
                state["remaining"] = producer.flush(delivery_timeout_seconds + 5)
            except Exception as exc:
                if primary_error is None:
                    primary_error, stage = exc, "producer_flush"
                source_stop.set()
        production_done.set()
        if worker is not None:
            if primary_error is not None or state["worker_errors"] or state["delivery_errors"]:
                consumer_stop.set()
            elif not consumer_done.wait(drain_seconds):
                primary_error, stage = RuntimeError("Consumer drain deadline expired"), "consumer_drain"
                source_stop.set()
                consumer_stop.set()
            consumer_stop.set()
            worker.join(timeout=12)
            if worker.is_alive():
                primary_error, stage = RuntimeError("Consumer worker did not close within the shutdown limit"), "consumer_shutdown"
                source_stop.set()

    result = report()
    result["stop_reason"] = stop_reason
    result["complete"] = False
    result["worker_closed"] = worker is None or not worker.is_alive()
    if isinstance(primary_error, KeyboardInterrupt):
        primary_error.streaming_report = result
        raise primary_error
    if primary_error is not None:
        result["error"] = f"{type(primary_error).__name__}: {primary_error}"
        raise StreamingPipelineError(stage, result) from primary_error
    if result["worker_errors"]:
        raise StreamingPipelineError("consumer", result)
    if result["remaining"] or result["delivery_errors"] or result["confirmed"] != result["accepted"]:
        raise StreamingPipelineError("delivery", result)
    if result["committed"] != result["accepted"] or result["consumed"] != result["accepted"]:
        raise StreamingPipelineError("consumer_drain", result)
    if stop_reason == "cancelled":
        return result
    if result["accepted"] == 0:
        raise StreamingPipelineError("no_events", result)
    if (sum(result["bot_counts"].values()) != result["unique_processed"]
            or sum(row["count"] for row in result["site_rows"]) != result["unique_processed"]
            or sum(row["count"] for row in result["time_rows"]) != result["unique_processed"]):
        raise StreamingPipelineError("aggregation_invariant", result)
    result["complete"] = True
    if on_progress is not None:
        on_progress(result)
    return result
