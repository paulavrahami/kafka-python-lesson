"""Fixed-data lesson invariants and Kafka failure boundaries; no live service."""
from collections import Counter
import contextlib
from html.parser import HTMLParser
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import lesson_basics as basics


def metadata(topic="lesson.test", count=3, leader=1, isrs=None, present=True):
    partitions = {i: SimpleNamespace(error=None, leader=leader, isrs=[1] if isrs is None else isrs)
                  for i in range(count)}
    return SimpleNamespace(
        topics={topic: SimpleNamespace(error=None, partitions=partitions)} if present else {},
        brokers={1: SimpleNamespace(host="advertised-host", port=19093)}, cluster_id="lesson-cluster",
    )


def message(event, offset=0):
    payload = basics.encode_event(event)[1]
    return SimpleNamespace(error=lambda: None, value=lambda: payload,
                           partition=lambda: 0, offset=lambda: offset)


class FixedDataTests(unittest.TestCase):
    def test_fixture_is_fixed_fresh_and_exactly_ten(self):
        first, second = basics.make_fixture(), basics.make_fixture()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 10)
        self.assertEqual([e["meta"]["id"] for e in first], [f"synthetic-{i:03d}" for i in range(1, 11)])
        self.assertEqual(sum(e["bot"] for e in first), 6)
        self.assertEqual(Counter(e["server_name"] for e in first),
                         {"en.wikipedia.org": 5, "he.wikipedia.org": 4, "de.wikipedia.org": 1})
        first[0]["meta"]["id"] = "changed"
        self.assertEqual(second[0]["meta"]["id"], "synthetic-001")

    def test_all_events_round_trip_with_key_and_time(self):
        for event in basics.make_fixture():
            key, value, stamp = basics.encode_event(event)
            self.assertEqual(key.decode(), event["server_name"])
            self.assertEqual(basics.decode_event(value), event)
            self.assertEqual(stamp, basics.event_millis(event))

    def test_schema_rejects_wrong_meta_bot_and_missing_timezone(self):
        cases = [{"meta": []}, basics.make_fixture()[0], basics.make_fixture()[0]]
        cases[1]["bot"] = "True"
        cases[2]["meta"]["dt"] = "2026-11-21T12:00:02"
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                basics.decode_event(json.dumps(value))

    def test_window_counts_match_the_exact_ten_records(self):
        events = basics.make_fixture()
        sites = basics.window_counts(events, 60, lambda event: event["server_name"])
        windows = basics.window_counts(events, 10, lambda event: "all")
        self.assertEqual(sum(row["count"] for row in sites), 10)
        self.assertEqual(sum(row["count"] for row in windows), 10)
        self.assertEqual([row["count"] for row in windows], [3, 2, 2, 1, 2])
        self.assertEqual(len(sites), 5)
        with self.assertRaises(ValueError):
            basics.window_counts(events, 0, lambda event: "all")

    def test_crash_replays_existing_fourth_event_without_an_eleventh_event(self):
        events = basics.make_fixture()
        result = basics.idempotent_recovery_demo(events)
        self.assertEqual(result["before"], {"documents": 4, "next_offset": 3, "write_attempts": 4})
        self.assertEqual(result["after"], {"documents": 10, "next_offset": 10, "write_attempts": 11})
        self.assertEqual(result["attempts"]["synthetic-004"], 2)
        self.assertEqual(set(result["documents"]), {event["meta"]["id"] for event in events})
        self.assertEqual(events, basics.make_fixture())

    def test_existing_group_is_not_reset_by_earliest(self):
        self.assertEqual(basics.starting_position(7, "earliest"), 7)
        self.assertEqual(basics.starting_position(None, "earliest"), 0)
        self.assertEqual(basics.starting_position(None, "latest"), 10)


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.lesson = basics.KafkaLesson("custom-broker:19092")

    def test_configuration_is_instance_scoped_and_ipv4(self):
        other = basics.KafkaLesson("other-broker:29092")
        self.assertEqual(self.lesson.client_config()["bootstrap.servers"], "custom-broker:19092")
        self.assertEqual(other.client_config()["bootstrap.servers"], "other-broker:29092")
        self.assertEqual(self.lesson.client_config("advertised:19093")["bootstrap.servers"], "advertised:19093")
        for config in [self.lesson.client_config(), other.client_config()]:
            self.assertEqual(config["broker.address.family"], "v4")
            self.assertEqual(config["socket.connection.setup.timeout.ms"], 5000)

    def test_custom_bootstrap_and_advertised_address_are_both_checked(self):
        admin, probe = Mock(), Mock()
        admin.list_topics.return_value = metadata()
        probe.list_topics.return_value = metadata()
        with patch("confluent_kafka.admin.AdminClient", side_effect=[admin, probe]) as factory:
            ready = self.lesson.ensure_topics(["lesson.test"])
        self.assertEqual(ready, {"lesson.test": 3})
        self.assertEqual([call.args[0]["bootstrap.servers"] for call in factory.call_args_list],
                         ["custom-broker:19092", "advertised-host:19093"])
        self.assertTrue(all(call.args[0]["broker.address.family"] == "v4" for call in factory.call_args_list))
        admin.create_topics.assert_not_called()

    def test_missing_topic_created_with_three_partitions_and_one_replica(self):
        admin, probe = Mock(), Mock()
        admin.list_topics.side_effect = [metadata(present=False), metadata()]
        probe.list_topics.return_value = metadata()
        admin.create_topics.return_value = {"lesson.test": Mock()}
        with patch("confluent_kafka.admin.AdminClient", side_effect=[admin, probe]):
            self.assertEqual(self.lesson.ensure_topics(["lesson.test"]), {"lesson.test": 3})
        created = admin.create_topics.call_args.args[0][0]
        self.assertEqual(created.topic, "lesson.test")
        self.assertEqual(created.num_partitions, 3)
        self.assertEqual(created.replication_factor, 1)

    def test_partition_without_registered_leader_or_isr_is_not_ready(self):
        for value in [metadata(leader=-1, isrs=[]), metadata(leader=2, isrs=[2]), metadata(isrs=[])]:
            with self.subTest(value=value):
                self.assertIn("lesson.test", self.lesson._topic_readiness_problems(value, ["lesson.test"], 3))
        with self.assertRaises(RuntimeError) as raised:
            self.lesson._topic_readiness_problems(metadata(count=1), ["lesson.test"], 3)
        self.assertEqual(raised.exception.args[0]["stage"], "topic_configuration")

    def test_transient_metadata_failure_retries_once_on_the_same_client(self):
        from confluent_kafka import KafkaError, KafkaException
        client = Mock()
        expected = metadata()
        client.list_topics.side_effect = [KafkaException(KafkaError(KafkaError._TIMED_OUT)), expected]
        with patch.object(basics.time, "sleep") as sleep:
            self.assertIs(self.lesson._read_kafka_metadata(client, 4), expected)
        self.assertEqual(client.list_topics.call_count, 2)
        self.assertTrue(all(call.kwargs == {"timeout": 4} for call in client.list_topics.call_args_list))
        sleep.assert_called_once_with(0.25)

    def test_permanent_metadata_failure_does_not_retry(self):
        from confluent_kafka import KafkaError, KafkaException
        client = Mock()
        client.list_topics.side_effect = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
        with patch.object(basics.time, "sleep") as sleep, self.assertRaises(KafkaException):
            self.lesson._read_kafka_metadata(client)
        self.assertEqual(client.list_topics.call_count, 1)
        sleep.assert_not_called()

    def test_advertised_failure_report_retains_custom_bootstrap(self):
        from confluent_kafka import KafkaError, KafkaException
        probe = Mock()
        probe.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        with patch("confluent_kafka.admin.AdminClient", return_value=probe), patch.object(basics.time, "sleep"):
            with self.assertRaises(RuntimeError) as raised:
                self.lesson._verify_broker_addresses(metadata())
        report = raised.exception.args[0]
        self.assertEqual(report["stage"], "advertised_broker_connection")
        self.assertEqual(report["bootstrap_servers"], "custom-broker:19092")
        self.assertEqual(report["broker"], "advertised-host:19093")


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.lesson = basics.KafkaLesson("custom-broker:19092")
        self.lesson.ensure_topics = Mock(return_value={"lesson.test": 3})

    def test_preflight_failure_never_constructs_producer(self):
        self.lesson.ensure_topics.side_effect = RuntimeError("No partition leader")
        with patch("confluent_kafka.Producer") as producer:
            with self.assertRaisesRegex(RuntimeError, "No partition leader"):
                self.lesson.publish_events(basics.make_fixture(), "lesson.test")
        producer.assert_not_called()

    def publish(self, failure=False, incomplete_callbacks=False):
        from confluent_kafka import KafkaError
        made = []
        class Producer:
            def __init__(self, config):
                self.config, self.rows = config, []
                made.append(self)
            def produce(self, **row):
                self.rows.append(row)
            def poll(self, timeout):
                pass
            def flush(self, timeout):
                self.flush_timeout = timeout
                rows = self.rows[:-1] if incomplete_callbacks else self.rows
                for i, row in enumerate(rows):
                    msg = SimpleNamespace(key=lambda: row["key"], partition=lambda: 0, offset=lambda: i)
                    row["on_delivery"](KafkaError(KafkaError._MSG_TIMED_OUT) if failure else None, msg)
                return 0
        with patch("confluent_kafka.Producer", Producer):
            try:
                result = self.lesson.publish_events(basics.make_fixture(), "lesson.test")
            except RuntimeError as exc:
                result = exc
        return result, made[0]

    def test_all_ten_deliveries_confirmed_with_proven_client_settings(self):
        result, producer = self.publish()
        self.assertEqual((result["accepted"], result["confirmed"]), (10, 10))
        self.assertEqual(producer.config["bootstrap.servers"], "custom-broker:19092")
        self.assertEqual(producer.config["broker.address.family"], "v4")
        self.assertTrue(producer.config["enable.idempotence"])
        self.assertEqual(producer.config["acks"], "all")
        self.assertFalse(producer.config["allow.auto.create.topics"])
        self.assertGreater(producer.flush_timeout * 1000, producer.config["delivery.timeout.ms"])
        self.lesson.ensure_topics.assert_called_once_with(["lesson.test"])

    def test_empty_queue_with_failed_delivery_is_not_reported_as_success(self):
        result, _ = self.publish(failure=True)
        self.assertIsInstance(result, RuntimeError)
        report = result.args[0]
        self.assertEqual((report["remaining"], report["accepted"], report["confirmed"]), (0, 10, 0))
        self.assertEqual(report["stage"], "delivery")
        self.assertEqual(report["bootstrap_servers"], "custom-broker:19092")

    def test_missing_callback_is_not_reported_as_success(self):
        result, _ = self.publish(incomplete_callbacks=True)
        self.assertIsInstance(result, RuntimeError)
        self.assertEqual(result.args[0]["confirmed"], 9)


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.lesson = basics.KafkaLesson("custom-broker:19092")

    def test_process_precedes_each_manual_commit_and_ten_records_return(self):
        consumer = Mock()
        consumer.poll.side_effect = [message(event, i) for i, event in enumerate(basics.make_fixture())]
        sequence = []
        consumer.commit.side_effect = lambda **kwargs: sequence.append("commit") or [SimpleNamespace(error=None)]
        with patch("confluent_kafka.Consumer", return_value=consumer) as factory:
            receipts = self.lesson.consume_events("lesson.test", "first", lambda event: sequence.append("handle"))
        self.assertEqual(len(receipts), 10)
        self.assertEqual(sequence, ["handle", "commit"] * 10)
        config = factory.call_args.args[0]
        self.assertEqual(config["bootstrap.servers"], "custom-broker:19092")
        self.assertEqual(config["broker.address.family"], "v4")
        self.assertFalse(config["enable.auto.commit"])
        self.assertFalse(config["enable.auto.offset.store"])
        consumer.close.assert_called_once()

    def test_partial_read_raises_clear_diagnostic_instead_of_claiming_ten(self):
        consumer = Mock()
        consumer.poll.side_effect = [message(basics.make_fixture()[0]), None]
        consumer.commit.return_value = [SimpleNamespace(error=None)]
        with patch("confluent_kafka.Consumer", return_value=consumer):
            with patch.object(basics.time, "monotonic", side_effect=[0, 0, 0, 1, 1, 20]):
                with self.assertRaises(RuntimeError) as raised:
                    self.lesson.consume_events("lesson.test", "first", lambda event: None)
        report = raised.exception.args[0]
        self.assertEqual(report["stage"], "consume_incomplete")
        self.assertEqual((report["expected"], report["processed_and_committed"]), (10, 1))
        self.assertEqual(report["bootstrap_servers"], "custom-broker:19092")
        consumer.close.assert_called_once()

    def test_handler_failure_prevents_commit_and_closes_consumer(self):
        consumer = Mock()
        consumer.poll.return_value = message(basics.make_fixture()[0])
        handler = Mock(side_effect=RuntimeError("handler failed"))
        with patch("confluent_kafka.Consumer", return_value=consumer):
            with self.assertRaisesRegex(RuntimeError, "handler failed"):
                self.lesson.consume_events("lesson.test", "first", handler)
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    def test_commit_error_prevents_success_report(self):
        consumer = Mock()
        consumer.poll.return_value = message(basics.make_fixture()[0])
        consumer.commit.return_value = [SimpleNamespace(error="commit failed")]
        with patch("confluent_kafka.Consumer", return_value=consumer):
            with self.assertRaisesRegex(RuntimeError, "Commit failed"):
                self.lesson.consume_events("lesson.test", "first", lambda event: None)
        consumer.close.assert_called_once()

    def test_empty_commit_result_prevents_success_report(self):
        consumer = Mock()
        consumer.poll.return_value = message(basics.make_fixture()[0])
        consumer.commit.return_value = []
        with patch("confluent_kafka.Consumer", return_value=consumer):
            with self.assertRaisesRegex(RuntimeError, "Commit failed"):
                self.lesson.consume_events("lesson.test", "first", lambda event: None)
        consumer.close.assert_called_once()


class NotebookTests(unittest.TestCase):
    def notebook(self):
        return json.loads((ROOT / "01_Kafka_Synthetic_HE.ipynb").read_text())

    def test_offline_run_all_preserves_the_ten_event_invariants(self):
        namespace = {}
        with patch("confluent_kafka.Producer", side_effect=AssertionError("Unexpected live Producer")):
            with patch("confluent_kafka.Consumer", side_effect=AssertionError("Unexpected live Consumer")):
                with patch("confluent_kafka.admin.AdminClient", side_effect=AssertionError("Unexpected live Admin")):
                    with contextlib.redirect_stdout(io.StringIO()):
                        for cell in self.notebook()["cells"]:
                            if cell["cell_type"] != "code":
                                continue
                            source = ''.join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
                            exec(compile(source, cell["id"], "exec"), namespace)
                            if cell["id"] == "synthetic-config":
                                namespace["USE_KAFKA"] = False
        self.assertEqual(namespace["summary"], {
            "mode": "OFFLINE Python only", "source_events": 10, "first_read_events": 10,
            "replay_events": 10, "unique_recovery_documents": 10,
            "recovery_write_attempts": 11, "bot": 6, "non_bot": 4,
        })
        self.assertEqual(len(namespace["FIXTURE"]), 10)

    def test_notebook_defaults_outputs_and_rtl_markup(self):
        notebook = self.notebook()
        config = next(cell["source"] for cell in notebook["cells"] if cell["id"] == "synthetic-config")
        self.assertIn("USE_KAFKA = True", config)
        self.assertIn("MAX_MESSAGES = 10", config)
        self.assertEqual(notebook["metadata"]["language_info"]["version"], "3.13.12")
        class CheckRTL(HTMLParser):
            bad = []
            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                if tag in {"h1", "h2", "h3", "p", "li", "th", "td"}:
                    if attrs.get("dir") != "rtl" or attrs.get("align") != "right":
                        self.bad.append(tag)
                if tag in {"pre", "code"} and attrs.get("dir") != "ltr":
                    self.bad.append(tag)
        parser = CheckRTL()
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                self.assertEqual(cell["outputs"], [])
                self.assertIsNone(cell["execution_count"])
                self.assertNotIn("RUN_LIVE_WIKIMEDIA", cell["source"])
                self.assertNotIn("RUN_LIVE_OPENSEARCH", cell["source"])
            else:
                parser.feed(cell["source"])
        self.assertEqual(parser.bad, [])
        diagram = next(cell for cell in notebook["cells"] if cell["id"] == "synthetic-map")
        self.assertIn("Kafka_Synthetic_Project_HE.png", diagram["attachments"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
