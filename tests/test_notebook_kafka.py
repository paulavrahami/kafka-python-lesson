"""Regression tests for the notebook's live-Kafka helpers, without a broker."""
import json
from itertools import islice
from pathlib import Path
from types import SimpleNamespace
import time
import unittest
from unittest.mock import Mock, patch


def helpers():
    path = Path(__file__).resolve().parents[1] / "Kafka_Python_Step_by_Step_HE.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    namespace = {"RUN_ID": "test", "BOOTSTRAP_SERVERS": "127.0.0.1:9092",
                 "json": json, "islice": islice, "time": time}
    for cell in notebook["cells"]:
        source = "".join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
        if cell["cell_type"] == "code" and any(name in source for name in ("def ensure_topics(", "def publish_json_rows(", "def consume_events(")):
            exec(compile(source, "notebook-helper", "exec"), namespace)
    return namespace


def metadata(leader=1, isrs=None, count=3, present=True):
    parts = {i: SimpleNamespace(error=None, leader=leader, isrs=[1] if isrs is None else isrs)
             for i in range(count)}
    return SimpleNamespace(topics={"wiki.test": SimpleNamespace(error=None, partitions=parts)} if present else {},
                           brokers={1: SimpleNamespace(host="localhost", port=9092)}, cluster_id="lab")


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.ns = helpers()

    def test_three_partitions_without_leaders_are_not_ready(self):
        result = self.ns["_topic_readiness_problems"](metadata(leader=-1, isrs=[]), ["wiki.test"], 3)
        self.assertEqual(len(result["wiki.test"]), 3)

    def test_leader_must_be_registered_and_in_isr(self):
        for fixture in (metadata(leader=2, isrs=[2]), metadata(isrs=[])):
            with self.subTest(metadata=fixture):
                self.assertIn("wiki.test", self.ns["_topic_readiness_problems"](fixture, ["wiki.test"], 3))

    def test_waits_for_missing_metadata_then_leaders(self):
        admin = Mock()
        admin.list_topics.side_effect = [metadata(present=False), metadata(leader=-1, isrs=[]), metadata()]
        with patch.object(time, "sleep"):
            result = self.ns["_wait_for_topic_leaders"](admin, ["wiki.test"], 3, 5)
        self.assertEqual(result, {"wiki.test": 3})
        self.assertEqual(admin.list_topics.call_count, 3)
        self.assertTrue(all(0 < call.kwargs["timeout"] <= 5 for call in admin.list_topics.call_args_list))

    def test_readiness_timeout_reports_pending_partitions(self):
        admin = Mock()
        admin.list_topics.return_value = metadata(leader=-1, isrs=[])
        with patch.object(time, "monotonic", side_effect=[0, 0, 0, 1, 2]), patch.object(time, "sleep"):
            with self.assertRaises(RuntimeError) as raised:
                self.ns["_wait_for_topic_leaders"](admin, ["wiki.test"], 3, 1)
        self.assertEqual(raised.exception.args[0]["stage"], "topic_readiness")
        self.assertIn("wiki.test", raised.exception.args[0]["pending"])

    def test_existing_wrong_partition_count_is_not_changed(self):
        with self.assertRaises(RuntimeError) as raised:
            self.ns["_topic_readiness_problems"](metadata(count=1), ["wiki.test"], 3)
        self.assertEqual(raised.exception.args[0]["stage"], "topic_configuration")

    def test_existing_ready_topic_needs_no_creation(self):
        from confluent_kafka.admin import AdminClient
        admin = Mock(spec=AdminClient)
        admin.list_topics.return_value = metadata()
        with patch("confluent_kafka.admin.AdminClient", return_value=admin) as constructor:
            result = self.ns["ensure_topics"](["wiki.test"])
        self.assertEqual(result, {"wiki.test": 3})
        admin.create_topics.assert_not_called()
        self.assertEqual(constructor.call_count, 2)
        for call in constructor.call_args_list:
            self.assertEqual(call.args[0]["broker.address.family"], "v4")
            self.assertEqual(call.args[0]["socket.connection.setup.timeout.ms"], 5000)

    def test_advertised_connection_failure_is_specific(self):
        from confluent_kafka import KafkaError, KafkaException
        probe = Mock()
        probe.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        with patch("confluent_kafka.admin.AdminClient", return_value=probe):
            with self.assertRaises(RuntimeError) as raised:
                self.ns["_verify_broker_addresses"](metadata())
        report = raised.exception.args[0]
        self.assertEqual(report["stage"], "advertised_broker_connection")
        self.assertEqual(report["broker"], "localhost:9092")

    def test_creation_future_timeout_keeps_topic_context(self):
        from concurrent.futures import TimeoutError as FutureTimeoutError
        admin = Mock()
        admin.list_topics.return_value = metadata(present=False)
        future = Mock()
        future.result.side_effect = FutureTimeoutError()
        admin.create_topics.return_value = {"wiki.test": future}
        with patch("confluent_kafka.admin.AdminClient", return_value=admin):
            with self.assertRaises(RuntimeError) as raised:
                self.ns["ensure_topics"](["wiki.test"])
        self.assertEqual(raised.exception.args[0]["stage"], "topic_creation")
        self.assertEqual(raised.exception.args[0]["topic"], "wiki.test")

    def test_metadata_retry_keeps_same_client_and_timeout(self):
        from confluent_kafka import KafkaError, KafkaException
        client = Mock()
        expected = metadata()
        client.list_topics.side_effect = [KafkaException(KafkaError(KafkaError._TIMED_OUT)), expected]
        with patch.object(time, "sleep") as sleep:
            actual = self.ns["_read_kafka_metadata"](client, timeout=4)
        self.assertIs(actual, expected)
        self.assertEqual(client.list_topics.call_count, 2)
        self.assertTrue(all(call.kwargs == {"timeout": 4} for call in client.list_topics.call_args_list))
        sleep.assert_called_once_with(0.25)

    def test_metadata_retry_does_not_repeat_permanent_failure(self):
        from confluent_kafka import KafkaError, KafkaException
        client = Mock()
        client.list_topics.side_effect = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
        with patch.object(time, "sleep") as sleep, self.assertRaises(KafkaException):
            self.ns["_read_kafka_metadata"](client)
        self.assertEqual(client.list_topics.call_count, 1)
        sleep.assert_not_called()

    def test_metadata_retry_has_a_two_attempt_limit(self):
        from confluent_kafka import KafkaError, KafkaException
        client = Mock()
        client.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TIMED_OUT))
        with patch.object(time, "sleep"), self.assertRaises(KafkaException):
            self.ns["_read_kafka_metadata"](client)
        self.assertEqual(client.list_topics.call_count, 2)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.ns = helpers()
        self.ns["ensure_topics"] = Mock(return_value={"wiki.test": 3})

    def test_failed_preflight_never_constructs_a_producer(self):
        self.ns["ensure_topics"].side_effect = RuntimeError("No leader")
        with patch("confluent_kafka.Producer") as producer:
            with self.assertRaisesRegex(RuntimeError, "No leader"):
                self.ns["publish_json_rows"]([{"key": "x"}], "wiki.test", lambda r: r["key"])
        producer.assert_not_called()

    def publish(self, fail=False, transient=False):
        from confluent_kafka import KafkaError
        class Producer:
            def __init__(self, config):
                self.config = config
                self.rows = []
            def produce(self, **row):
                self.rows.append(row)
            def poll(self, timeout):
                pass
            def flush(self, timeout):
                if fail or transient:
                    self.config["error_cb"](KafkaError(KafkaError._ALL_BROKERS_DOWN))
                for index, row in enumerate(self.rows):
                    msg = SimpleNamespace(key=lambda: row["key"], partition=lambda: 0, offset=lambda: index)
                    row["on_delivery"](KafkaError(KafkaError._MSG_TIMED_OUT) if fail else None, msg)
                self.flush_timeout = timeout
                return 0
        made = []
        def factory(config):
            client = Producer(config)
            made.append(client)
            return client
        with patch("confluent_kafka.Producer", side_effect=factory):
            try:
                result = self.ns["publish_json_rows"]([{"key": "x"}, {"key": "y"}], "wiki.test", lambda r: r["key"])
            except RuntimeError as exc:
                result = exc
        return result, made[0]

    def test_empty_queue_with_failed_deliveries_still_fails_with_context(self):
        result, producer = self.publish(fail=True)
        self.assertIsInstance(result, RuntimeError)
        report = result.args[0]
        self.assertEqual((report["remaining"], report["accepted"], report["confirmed"]), (0, 2, 0))
        self.assertEqual(report["topic"], "wiki.test")
        self.assertEqual(report["bootstrap_servers"], "127.0.0.1:9092")
        self.assertEqual(len(report["delivery_errors"]), 2)
        self.assertTrue(report["broker_errors"])
        self.assertGreater(producer.flush_timeout * 1000, producer.config["delivery.timeout.ms"])

    def test_transient_broker_error_does_not_override_successful_delivery(self):
        result, producer = self.publish(transient=True)
        self.assertEqual((result["accepted"], result["confirmed"]), (2, 2))
        self.ns["ensure_topics"].assert_called_once_with(["wiki.test"])
        self.assertFalse(producer.config["allow.auto.create.topics"])
        self.assertTrue(producer.config["enable.idempotence"])
        self.assertEqual(producer.config["broker.address.family"], "v4")
        self.assertEqual(producer.config["socket.connection.setup.timeout.ms"], 5000)

    def test_consumer_uses_the_same_ipv4_policy(self):
        consumer = Mock()
        with patch("confluent_kafka.Consumer", return_value=consumer) as constructor:
            result = self.ns["consume_events"]("wiki.test", "group-test", lambda event: None, max_messages=0)
        self.assertEqual(result, [])
        config = constructor.call_args.args[0]
        self.assertEqual(config["broker.address.family"], "v4")
        self.assertEqual(config["socket.connection.setup.timeout.ms"], 5000)
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
