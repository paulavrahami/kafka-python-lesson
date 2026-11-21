"""Concurrency and failure tests for the bounded streaming runner (no broker)."""
import importlib.util
import json
from pathlib import Path
import queue
import threading
import time
import types
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/kafka_streaming.py"
SPEC = importlib.util.spec_from_file_location("lesson_kafka_streaming", SCRIPT)
streaming = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(streaming)


def event(identity="one", *, bot=True, second=2, site="en.wikipedia.org"):
    return {"meta": {"id": identity, "dt": f"2026-11-21T12:00:{second:02d}Z"},
            "server_name": site, "bot": bot}


class Broker:
    """A test transport with independent producer and consumer clients."""

    def __init__(self, *, fail_delivery=False, commit_error=False, drop_after=None,
                 full_attempts=0, always_full=False, topic_exists=True):
        self.rows = queue.Queue()
        self.pending = []
        self.produced = []
        self.producer = self.consumer = None
        self.fail_delivery, self.commit_error = fail_delivery, commit_error
        self.drop_after = drop_after
        self.full_attempts, self.always_full = full_attempts, always_full
        self.topic_exists = topic_exists
        self.consume_thread = None
        owner = self

        class Producer:
            def __init__(self, config):
                self.config, self.flush_calls, self.produce_calls = config, 0, 0
                owner.producer = self

            def list_topics(self, timeout):
                partition = types.SimpleNamespace(error=None, leader=1)
                topic = types.SimpleNamespace(error=None, partitions={0: partition, 1: partition, 2: partition})
                return types.SimpleNamespace(topics={"fresh.test": topic} if owner.topic_exists else {})

            def produce(self, **row):
                self.produce_calls += 1
                if owner.always_full or self.produce_calls <= owner.full_attempts:
                    raise BufferError("queue full")
                owner.produced.append(row)
                owner.pending.append(row)
                if not owner.fail_delivery:
                    owner.rows.put(row)

            def poll(self, timeout):
                while owner.pending:
                    row = owner.pending.pop(0)
                    row["on_delivery"]("delivery timed out" if owner.fail_delivery else None, None)
                if timeout:
                    time.sleep(min(timeout, 0.001))

            def flush(self, timeout):
                self.flush_calls += 1
                self.flush_timeout = timeout
                self.poll(0)
                return 0

        class Consumer:
            def __init__(self, config):
                self.config, self.closed, self.poll_count, self.commits = config, False, 0, []
                owner.consumer = self

            def assign(self, partitions):
                self.assignments = partitions

            def poll(self, timeout):
                owner.consume_thread = threading.current_thread().name
                try:
                    row = owner.rows.get(timeout=min(timeout, 0.01))
                except queue.Empty:
                    return None
                self.poll_count += 1
                if owner.drop_after is not None and self.poll_count > owner.drop_after:
                    return None
                return types.SimpleNamespace(
                    error=lambda: None, headers=lambda: row["headers"], value=lambda: row["value"])

            def commit(self, message, asynchronous):
                if asynchronous:
                    raise AssertionError("The lesson must confirm commits")
                self.commits.append(message)
                return [types.SimpleNamespace(error="commit failed" if owner.commit_error else None)]

            def close(self):
                self.closed = True

        def partition(topic, number, offset):
            return types.SimpleNamespace(topic=topic, partition=number, offset=offset)

        self.module = types.SimpleNamespace(
            Producer=Producer, Consumer=Consumer, TopicPartition=partition, OFFSET_BEGINNING=-2,
            KafkaError=types.SimpleNamespace(_PARTITION_EOF=-191), KafkaException=RuntimeError,
        )

    def run(self, rows, **kwargs):
        with patch.dict("sys.modules", {"confluent_kafka": self.module}):
            return streaming.run_streaming_pipeline(rows, topic="fresh.test", drain_seconds=0.15, **kwargs)


class StreamingTests(unittest.TestCase):
    def test_consumer_processes_before_source_finishes(self):
        broker, processed, closed, progress = Broker(), threading.Event(), [], []

        def callback(snapshot):
            progress.append(snapshot)
            if snapshot["committed"]:
                processed.set()

        def source():
            try:
                yield event()
                self.assertTrue(processed.wait(1), "The source was buffered before Kafka consumption")
                yield event("two", bot=False, second=12)
            finally:
                closed.append(True)

        result = broker.run(source(), on_progress=callback, progress_every=1)
        self.assertTrue(result["complete"])
        self.assertEqual([result[k] for k in ("accepted", "confirmed", "consumed", "committed")], [2, 2, 2, 2])
        self.assertGreaterEqual(result["processed_while_source_open"], 1)
        self.assertTrue(any(row["source_active"] and row["committed"] for row in progress))
        self.assertLessEqual(result["first_consumer_seconds"], result["source_finished_seconds"])
        self.assertEqual(result["bot_counts"], {"bot": 1, "non-bot": 1})
        self.assertEqual(sum(row["count"] for row in result["site_rows"]), 2)
        self.assertEqual(len(result["time_rows"]), 2)
        self.assertEqual(closed, [True])
        self.assertTrue(broker.consumer.closed)
        self.assertNotEqual(broker.consume_thread, threading.current_thread().name)

    def test_duplicate_id_is_committed_but_not_counted_twice(self):
        broker = Broker()
        result = broker.run([event(), event()])
        self.assertEqual((result["accepted"], result["committed"], result["unique_processed"], result["duplicates"]), (2, 2, 1, 1))
        self.assertEqual(result["bot_counts"], {"bot": 1})
        self.assertEqual(result["site_rows"][0]["count"], 1)

    def test_failed_delivery_is_not_success_even_with_empty_queue(self):
        broker = Broker(fail_delivery=True)
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run([event()])
        report = raised.exception.report
        self.assertEqual(report["remaining"], 0)
        self.assertEqual((report["accepted"], report["confirmed"], report["delivery_failed"]), (1, 0, 1))
        self.assertFalse(report["complete"])
        self.assertTrue(report["delivery_errors"])
        self.assertTrue(broker.consumer.closed)

    def test_queue_full_waits_and_retries_the_same_event(self):
        broker = Broker(full_attempts=2)
        result = broker.run([event(), event("two")], queue_wait_seconds=0.1)
        self.assertEqual(result["queue_retries"], 2)
        self.assertEqual([json.loads(row["value"])["meta"]["id"] for row in broker.produced], ["one", "two"])
        self.assertEqual(result["accepted"], 2)

    def test_permanently_full_queue_fails_without_claiming_delivery(self):
        broker = Broker(always_full=True)
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run([event()], queue_wait_seconds=0.01)
        self.assertEqual(raised.exception.stage, "producer_backpressure")
        self.assertEqual(raised.exception.report["accepted"], 0)
        self.assertTrue(broker.consumer.closed)

    def test_source_failure_flushes_and_closes_consumer(self):
        broker, closed = Broker(), []

        def source():
            try:
                yield event()
                raise ValueError("source disconnected")
            finally:
                closed.append(True)

        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run(source())
        self.assertEqual(raised.exception.stage, "source")
        self.assertIn("source disconnected", raised.exception.report["error"])
        self.assertEqual(broker.producer.flush_calls, 1)
        self.assertTrue(broker.consumer.closed)
        self.assertEqual(closed, [True])

    def test_keyboard_interrupt_preserves_signal_and_partial_report(self):
        broker = Broker()

        def source():
            yield event()
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt) as raised:
            broker.run(source())
        self.assertEqual(raised.exception.streaming_report["stop_reason"], "interrupted")
        self.assertFalse(raised.exception.streaming_report["complete"])
        self.assertTrue(raised.exception.streaming_report["worker_closed"])
        self.assertTrue(broker.consumer.closed)
        self.assertEqual(broker.producer.flush_calls, 1)

    def test_commit_error_keeps_processed_and_committed_separate(self):
        broker = Broker(commit_error=True)
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run([event()])
        report = raised.exception.report
        self.assertEqual((report["consumed"], report["unique_processed"], report["committed"]), (1, 1, 0))
        self.assertFalse(report["complete"])
        self.assertIn("commit", report["worker_errors"][0].lower())
        self.assertTrue(broker.consumer.closed)

    def test_missing_consumption_is_not_reported_as_complete(self):
        broker = Broker(drop_after=1)
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run([event(), event("two")])
        report = raised.exception.report
        self.assertEqual((report["confirmed"], report["consumed"], report["committed"]), (2, 1, 1))
        self.assertEqual(raised.exception.stage, "consumer_drain")
        self.assertFalse(report["complete"])
        self.assertTrue(broker.consumer.closed)

    def test_topic_preflight_never_reads_source_on_failure(self):
        broker, touched = Broker(topic_exists=False), []

        def source():
            touched.append(True)
            yield event()

        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run(source())
        self.assertEqual(raised.exception.stage, "broker_metadata")
        self.assertEqual(touched, [])
        self.assertIsNone(broker.consumer)

    def test_max_events_does_not_request_an_extra_source_row(self):
        broker, touched, closed = Broker(), [], []

        def source():
            try:
                for i in range(3):
                    touched.append(i)
                    yield event(str(i))
            finally:
                closed.append(True)

        result = broker.run(source(), max_events=2)
        self.assertEqual(touched, [0, 1])
        self.assertEqual(result["stop_reason"], "max_events")
        self.assertEqual(closed, [True])

    def test_empty_source_does_not_claim_live_success(self):
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            Broker().run([])
        self.assertEqual(raised.exception.stage, "no_events")
        self.assertEqual(raised.exception.report["accepted"], 0)

    def test_shared_stop_event_returns_explicit_cancelled_report(self):
        broker, stop, processed = Broker(), threading.Event(), threading.Event()

        def source():
            yield event()
            self.assertTrue(processed.wait(1))
            stop.set()

        result = broker.run(source(), stop_event=stop, progress_every=1,
                            on_progress=lambda report: processed.set() if report["committed"] else None)
        self.assertFalse(result["complete"])
        self.assertEqual(result["stop_reason"], "cancelled")
        self.assertTrue(result["worker_closed"])

    def test_clients_keep_ipv4_manual_commits_and_bounded_queue(self):
        broker = Broker()
        broker.run([event()], max_pending=7)
        for client in (broker.producer, broker.consumer):
            self.assertEqual(client.config["broker.address.family"], "v4")
            self.assertFalse(client.config["allow.auto.create.topics"])
        self.assertTrue(broker.producer.config["enable.idempotence"])
        self.assertEqual(broker.producer.config["acks"], "all")
        self.assertEqual(broker.producer.config["queue.buffering.max.messages"], 7)
        self.assertFalse(broker.consumer.config["enable.auto.commit"])
        self.assertFalse(broker.consumer.config["enable.auto.offset.store"])
        self.assertEqual(len(broker.consumer.assignments), 3)
        self.assertEqual(broker.producer.flush_timeout, 35)

    def test_nonfinite_deadlines_are_rejected_before_clients_start(self):
        for parameter in ("drain_seconds", "queue_wait_seconds", "delivery_timeout_seconds"):
            for value in (float("inf"), float("nan")):
                with self.subTest(parameter=parameter, value=value):
                    broker = Broker()
                    with patch.dict("sys.modules", {"confluent_kafka": broker.module}):
                        with self.assertRaisesRegex(ValueError, "finite and positive"):
                            streaming.run_streaming_pipeline([], topic="fresh.test", **{parameter: value})
                    self.assertIsNone(broker.producer)
                    self.assertIsNone(broker.consumer)

    def test_reused_topic_record_fails_instead_of_contaminating_counts(self):
        broker = Broker()
        broker.rows.put({"headers": [("lesson_run_id", b"another-run")],
                         "value": json.dumps(event("old")).encode("utf-8")})
        with self.assertRaises(streaming.StreamingPipelineError) as raised:
            broker.run([event("new")])
        report = raised.exception.report
        self.assertFalse(report["complete"])
        self.assertEqual(report["unique_processed"], 0)
        self.assertEqual(report["committed"], 0)
        self.assertTrue(any("fresh topic" in message for message in report["worker_errors"]))
        self.assertTrue(broker.consumer.closed)


if __name__ == "__main__":
    unittest.main()
