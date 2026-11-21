"""Continuous-pipeline behavior tests with deterministic Kafka/OpenSearch fakes."""
import importlib.util
import json
import os
from pathlib import Path
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch


PATH = Path(__file__).resolve().parents[1] / "scripts/kafka_opensearch_streaming.py"
SPEC = importlib.util.spec_from_file_location("lesson_opensearch_streaming", PATH)
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


def event(identity="one", *, title="עריכה לדוגמה"):
    return {"meta": {"id": identity, "dt": "2026-11-21T12:00:02Z"},
            "server_name": "he.wikipedia.org", "bot": False, "title": title,
            "type": "edit", "wiki": "hewiki", "namespace": 0,
            "user": "not projected", "comment": "not projected", "unexpected": {"nested": "not projected"}}


class SearchFailure(Exception):
    def __init__(self, status):
        self.status_code = status
        super().__init__(f"HTTP {status}")


class Services:
    def __init__(self, *, search_failures=(), commit_error=False, always_full=False, full_attempts=0,
                 delivery_error=False, bad_ack=False, drop_after=None):
        self.condition = threading.Condition()
        self.log, self.pending, self.trace = [], [], []
        self.group_offsets, self.documents = {}, {}
        self.producers, self.consumers, self.search_clients = [], [], []
        self.index_ids, self.index_attempts = [], 0
        self.search_failures = list(search_failures)
        self.commit_error, self.always_full, self.full_attempts = commit_error, always_full, full_attempts
        self.delivery_error, self.bad_ack, self.drop_after = delivery_error, bad_ack, drop_after
        owner = self

        class Producer:
            def __init__(self, config):
                self.config, self.flush_calls, self.attempts = config, 0, 0
                owner.producers.append(self)

            def produce(self, **row):
                self.attempts += 1
                if owner.always_full or self.attempts <= owner.full_attempts:
                    raise BufferError("queue full")
                message = owner.add_raw(row["value"], row["headers"])
                owner.pending.append((row["on_delivery"], message))

            def poll(self, timeout):
                while owner.pending:
                    callback, message = owner.pending.pop(0)
                    callback("delivery failure" if owner.delivery_error else None, message)
                if timeout:
                    time.sleep(min(0.001, timeout))

            def flush(self, timeout):
                self.flush_calls += 1
                self.flush_timeout = timeout
                self.poll(0)
                return 0

        class Consumer:
            def __init__(self, config):
                self.config, self.closed, self.position, self.polls = config, False, None, 0
                owner.consumers.append(self)

            def subscribe(self, topics):
                self.subscriptions = topics
                self.position = owner.group_offsets.get(self.config["group.id"], -1) + 1

            def poll(self, timeout):
                with owner.condition:
                    if self.position >= len(owner.log):
                        owner.condition.wait(min(timeout, 0.01))
                    if self.position >= len(owner.log):
                        return None
                    message = owner.log[self.position]
                    self.position += 1
                    self.polls += 1
                    if owner.drop_after is not None and self.polls > owner.drop_after:
                        return None
                    return message

            def commit(self, *, message, asynchronous):
                if asynchronous:
                    raise AssertionError("Commit must be acknowledged")
                identity = json.loads(message.value())["meta"]["id"]
                owner.trace.append(("commit", identity))
                if owner.commit_error:
                    return [types.SimpleNamespace(error="failed")]
                owner.group_offsets[self.config["group.id"]] = message.offset()
                return [types.SimpleNamespace(error=None)]

            def close(self):
                self.closed = True

        self.kafka_module = types.SimpleNamespace(Producer=Producer, Consumer=Consumer,
                                                  KafkaError=types.SimpleNamespace(_PARTITION_EOF=-191),
                                                  KafkaException=RuntimeError)

    def add_raw(self, payload, headers=()):
        with self.condition:
            offset = len(self.log)
            message = types.SimpleNamespace(value=lambda: payload, headers=lambda: headers,
                                             error=lambda: None, topic=lambda: "persistent.test",
                                             partition=lambda: 0, offset=lambda: offset)
            self.log.append(message)
            self.condition.notify_all()
            return message

    def add_backlog(self, identity):
        self.add_raw(json.dumps(event(identity)).encode("utf-8"), [("lesson_pipeline_run_id", b"older-run")])

    def search_factory(self, *args, **kwargs):
        owner = self

        class SearchClient:
            closed = False

            def index(self, *, index, id, body, request_timeout):
                owner.index_attempts += 1
                owner.index_ids.append(id)
                owner.trace.append(("index_attempt", body["event_id"]))
                if owner.search_failures:
                    raise owner.search_failures.pop(0)
                if owner.bad_ack:
                    return {"result": "noop"}
                result = "updated" if id in owner.documents else "created"
                owner.documents[id] = body
                owner.trace.append(("indexed", body["event_id"]))
                return {"result": result, "_shards": {"successful": 1, "failed": 0}}

            def close(self):
                self.closed = True

        client = SearchClient()
        self.search_clients.append(client)
        return client

    def run(self, source, **kwargs):
        options = {"topic": "persistent.test", "index": "test-index", "group_id": "stable-group",
                   "drain_seconds": 0.3, "preflight": False, **kwargs}
        with patch.dict("sys.modules", {"confluent_kafka": self.kafka_module}):
            with patch.object(pipeline, "create_opensearch_client", side_effect=self.search_factory):
                return pipeline.run_opensearch_pipeline(source, **options)


class PipelineTests(unittest.TestCase):
    def test_index_ack_precedes_commit_and_projection_is_controlled(self):
        services = Services()
        report = services.run([event()])
        self.assertLess(services.trace.index(("indexed", "one")), services.trace.index(("commit", "one")))
        self.assertEqual((report["accepted"], report["confirmed"], report["indexed"], report["committed"]), (1, 1, 1, 1))
        body = next(iter(services.documents.values()))
        self.assertEqual(body["title"], "עריכה לדוגמה")
        self.assertLessEqual(set(body), set(pipeline.INDEX_PROPERTIES))
        self.assertNotIn("user", body)
        self.assertNotIn("unexpected", body)

    def test_transient_retry_reuses_document_id_and_commits_once(self):
        services = Services(search_failures=[SearchFailure(503)])
        report = services.run([event()])
        self.assertEqual(report["index_retries"], 1)
        self.assertEqual(services.index_ids[0], services.index_ids[1])
        self.assertEqual(services.trace.count(("commit", "one")), 1)
        self.assertEqual(len(services.documents), 1)

    def test_auth_error_is_not_retried_or_committed(self):
        services = Services(search_failures=[SearchFailure(403)])
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()])
        self.assertEqual(services.index_attempts, 1)
        self.assertEqual(raised.exception.report["committed"], 0)
        self.assertNotIn("stable-group", services.group_offsets)
        self.assertTrue(services.consumers[0].closed)

    def test_retry_budget_exhaustion_does_not_commit(self):
        services = Services(search_failures=[SearchFailure(503) for _ in range(3)])
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()], max_index_retries=2, drain_seconds=1)
        self.assertEqual((services.index_attempts, raised.exception.report["index_retries"]), (3, 2))
        self.assertEqual(raised.exception.report["committed"], 0)

    def test_invalid_ack_does_not_commit(self):
        services = Services(bad_ack=True)
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()])
        self.assertEqual((raised.exception.report["indexed"], raised.exception.report["committed"]), (0, 0))

    def test_stable_id_overwrites_duplicate_events(self):
        services = Services()
        report = services.run([event(), event(title="הגרסה האחרונה")])
        self.assertEqual((report["indexed"], report["committed"]), (2, 2))
        self.assertEqual(len(services.documents), 1)
        self.assertEqual(next(iter(services.documents.values()))["title"], "הגרסה האחרונה")
        self.assertEqual(len(services.index_ids[0]), 64)

    def test_backlog_count_may_exceed_current_producer_count(self):
        services = Services()
        services.add_backlog("old-a")
        services.add_backlog("old-b")
        report = services.run([event("new")])
        self.assertEqual((report["accepted"], report["committed"]), (1, 3))
        self.assertEqual((report["consumed_other_runs"], report["committed_current_run"]), (2, 1))
        self.assertTrue(report["producer_drained"])
        self.assertTrue(report["current_run_committed"])
        self.assertIsNone(report["backlog_fully_drained"])

    def test_same_group_resumes_saved_offsets_without_rereading_committed_rows(self):
        services = Services()
        services.run([event("first")])
        report = services.run([event("second")])
        self.assertEqual(report["consumed"], 1)
        self.assertEqual(services.trace.count(("indexed", "first")), 1)
        self.assertEqual(services.group_offsets["stable-group"], 1)
        self.assertEqual(services.consumers[1].subscriptions, ["persistent.test"])

    def test_failed_index_is_replayed_on_next_run(self):
        services = Services(search_failures=[SearchFailure(400)])
        with self.assertRaises(pipeline.OpenSearchPipelineError):
            services.run([event("replay-me")])
        report = services.run([event("new")])
        self.assertEqual(report["consumed_other_runs"], 1)
        self.assertEqual(report["indexed"], 2)
        self.assertEqual(len(services.documents), 2)

    def test_commit_failure_leaves_written_document_and_no_saved_offset(self):
        services = Services(commit_error=True)
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()])
        self.assertEqual((raised.exception.report["indexed"], raised.exception.report["committed"]), (1, 0))
        self.assertEqual(len(services.documents), 1)
        self.assertNotIn("stable-group", services.group_offsets)
        original_id = services.index_ids[0]
        services.commit_error = False
        replay = services.run([event()])
        self.assertEqual(replay["consumed_other_runs"], 1)
        self.assertEqual(replay["committed"], 2)
        self.assertEqual(set(services.index_ids), {original_id})
        self.assertEqual(len(services.documents), 1)

    def test_manual_stop_cleans_up_after_indexing_while_source_is_open(self):
        services, processed, closed = Services(), threading.Event(), []

        def source():
            try:
                yield event()
                self.assertTrue(processed.wait(1), "Indexing started only after source completion")
                raise KeyboardInterrupt()
            finally:
                closed.append(True)

        report = services.run(source(), progress_every=1,
                              on_progress=lambda result: processed.set() if result["committed"] else None)
        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["stop_reason"], "manual_stop")
        self.assertGreaterEqual(report["processed_while_source_open"], 1)
        self.assertEqual(closed, [True])
        self.assertTrue(report["worker_closed"])
        self.assertTrue(services.consumers[0].closed)
        self.assertTrue(services.search_clients[0].closed)
        self.assertEqual(services.producers[0].flush_calls, 1)

    def test_shared_stop_event_has_explicit_stopped_status(self):
        services, stop, processed = Services(), threading.Event(), threading.Event()

        def source():
            yield event()
            self.assertTrue(processed.wait(1))
            stop.set()

        report = services.run(source(), stop_event=stop, progress_every=1,
                              on_progress=lambda result: processed.set() if result["committed"] else None)
        self.assertEqual(report["status"], "stopped")
        self.assertTrue(report["worker_closed"])

    def test_source_error_flushes_producer_and_closes_both_worker_clients(self):
        services, processed, closed = Services(), threading.Event(), []

        def source():
            try:
                yield event()
                self.assertTrue(processed.wait(1))
                raise RuntimeError("source connection failed")
            finally:
                closed.append(True)

        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run(source(), progress_every=1,
                         on_progress=lambda result: processed.set() if result["committed"] else None)
        self.assertEqual(raised.exception.stage, "source")
        self.assertEqual(closed, [True])
        self.assertTrue(services.consumers[0].closed)
        self.assertTrue(services.search_clients[0].closed)
        self.assertEqual(services.producers[0].flush_calls, 1)

    def test_no_hidden_thousand_event_limit(self):
        services = Services()
        report = services.run((event(str(i)) for i in range(1100)), drain_seconds=3)
        self.assertEqual((report["source_received"], report["accepted"], report["committed"]), (1100, 1100, 1100))

    def test_temporary_queue_backpressure_retries_without_dropping(self):
        services = Services(full_attempts=2)
        report = services.run([event()])
        self.assertEqual((report["queue_retries"], report["accepted"], report["committed"]), (2, 1, 1))

    def test_permanent_queue_backpressure_has_bounded_failure(self):
        services = Services(always_full=True)
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()], queue_wait_seconds=0.01)
        self.assertEqual(raised.exception.stage, "producer_backpressure")
        self.assertEqual(raised.exception.report["accepted"], 0)
        self.assertTrue(raised.exception.report["worker_closed"])

    def test_drain_timeout_reports_remaining_kafka_work_without_claiming_backlog_complete(self):
        services = Services(drop_after=0)
        report = services.run([event()], drain_seconds=0.03)
        self.assertTrue(report["producer_drained"])
        self.assertFalse(report["current_run_committed"])
        self.assertIn("drain_timeout", report["stop_reason"])
        self.assertIsNone(report["backlog_fully_drained"])

    def test_delivery_failure_is_not_hidden_by_successful_indexing(self):
        services = Services(delivery_error=True)
        with self.assertRaises(pipeline.OpenSearchPipelineError) as raised:
            services.run([event()])
        self.assertEqual(raised.exception.report["remaining"], 0)
        self.assertFalse(raised.exception.report["producer_drained"])

    def test_nonfinite_timeouts_fail_before_clients_start(self):
        for name in ("drain_seconds", "request_timeout", "queue_wait_seconds", "delivery_timeout_seconds"):
            for value in (float("inf"), float("nan")):
                with self.subTest(name=name, value=value):
                    services = Services()
                    with self.assertRaisesRegex(ValueError, "finite and positive"):
                        services.run([], **{name: value})
                    self.assertEqual(services.producers, [])


class PreparationTests(unittest.TestCase):
    def test_new_index_uses_explicit_strict_mapping_and_is_closed(self):
        client = Mock()
        client.info.return_value = {"version": {"number": "test"}}
        client.indices.exists.return_value = False
        client.indices.create.return_value = {"acknowledged": True}
        client.indices.get_mapping.return_value = {"lesson": {"mappings": {"properties": pipeline.INDEX_PROPERTIES}}}
        with patch.object(pipeline, "create_opensearch_client", return_value=client):
            with patch.object(pipeline, "_prepare_kafka_topic", return_value={"topic_created": False}) as kafka:
                report = pipeline.prepare_pipeline(topic="persistent.test", index="lesson")
        body = client.indices.create.call_args.kwargs["body"]
        self.assertEqual(body["mappings"]["dynamic"], "strict")
        self.assertEqual(len(body["mappings"]["properties"]), 12)
        self.assertTrue(report["index_created"])
        client.close.assert_called_once()
        kafka.assert_called_once()

    def test_existing_compatible_index_is_never_recreated(self):
        client = Mock()
        client.info.return_value = {}
        client.indices.exists.return_value = True
        client.indices.get_mapping.return_value = {"lesson": {"mappings": {"properties": pipeline.INDEX_PROPERTIES}}}
        with patch.object(pipeline, "create_opensearch_client", return_value=client):
            with patch.object(pipeline, "_prepare_kafka_topic", return_value={}):
                report = pipeline.prepare_pipeline(topic="persistent.test", index="lesson")
        self.assertFalse(report["index_created"])
        client.indices.create.assert_not_called()

    def test_incompatible_mapping_is_rejected_without_mutation(self):
        client = Mock()
        client.info.return_value = {}
        client.indices.exists.return_value = True
        client.indices.get_mapping.return_value = {"lesson": {"mappings": {"properties": {}}}}
        with patch.object(pipeline, "create_opensearch_client", return_value=client):
            with patch.object(pipeline, "_prepare_kafka_topic") as kafka:
                with self.assertRaisesRegex(RuntimeError, "mapping is incompatible"):
                    pipeline.prepare_pipeline(topic="persistent.test", index="lesson")
        client.indices.create.assert_not_called()
        client.indices.put_mapping.assert_not_called()
        kafka.assert_not_called()
        client.close.assert_called_once()

    def test_https_keeps_certificate_verification_and_disables_hidden_retries(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("opensearchpy.OpenSearch") as constructor:
                pipeline.create_opensearch_client("https://search.example.org")
        options = constructor.call_args.kwargs
        self.assertTrue(options["verify_certs"])
        self.assertNotEqual(options.get("ssl_assert_hostname"), False)
        self.assertEqual(options["max_retries"], 0)
        self.assertFalse(options["retry_on_timeout"])

    def test_remote_http_and_embedded_credentials_are_rejected(self):
        for url in ("http://search.example.org", "https://user:password@search.example.org"):
            with self.subTest(url=url), patch("opensearchpy.OpenSearch") as constructor:
                with self.assertRaises(ValueError):
                    pipeline.create_opensearch_client(url)
                constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
