"""Address-family consistency and bounded metadata recovery; no live broker."""

import contextlib
import importlib.util
import io
from pathlib import Path
import types
import unittest
from unittest.mock import Mock, call, patch

from confluent_kafka import KafkaError, KafkaException


SOURCE = Path(__file__).resolve().parents[1] / "scripts/check_kafka.py"
SPEC = importlib.util.spec_from_file_location("connection_settings", SOURCE)
diagnostics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostics)


def cluster():
    broker = types.SimpleNamespace(id=1, host="localhost", port=9092)
    return types.SimpleNamespace(brokers={1: broker}, cluster_id="lab", topics={})


class ConnectionSettingsTests(unittest.TestCase):
    def test_default_ipv4_and_setup_timeout_apply_to_both_endpoints(self):
        clients = [Mock(), Mock()]
        for client in clients:
            client.list_topics.return_value = cluster()
        admin = Mock(side_effect=clients)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            diagnostics.check("127.0.0.1:9092", 10, admin)
        self.assertEqual(admin.call_count, 2)
        self.assertEqual(
            [item.args[0]["bootstrap.servers"] for item in admin.call_args_list],
            ["127.0.0.1:9092", "localhost:9092"],
        )
        for item in admin.call_args_list:
            config = item.args[0]
            self.assertEqual(config["broker.address.family"], "v4")
            self.assertEqual(config["socket.connection.setup.timeout.ms"], 5000)
            self.assertFalse(config["allow.auto.create.topics"])
        self.assertIn("broker.address.family=v4", output.getvalue())
        self.assertIn("10 seconds per request", output.getvalue())

    def test_address_family_override_applies_to_bootstrap_and_advertised(self):
        for family in ("any", "v6"):
            admin = Mock()
            admin.return_value.list_topics.return_value = cluster()
            with self.subTest(family=family), contextlib.redirect_stdout(io.StringIO()):
                diagnostics.check("localhost:9092", 10, admin, address_family=family)
            self.assertEqual(admin.call_count, 2)
            self.assertTrue(all(
                item.args[0]["broker.address.family"] == family for item in admin.call_args_list
            ))

    def test_failing_endpoint_is_printed_before_metadata_request(self):
        cases = [
            ([RuntimeError("bootstrap unavailable")], "Checking bootstrap metadata: 127.0.0.1:9092"),
            ([cluster(), RuntimeError("advertised unavailable")],
             "Checking advertised broker metadata: localhost:9092 (broker 1)"),
        ]
        for outcomes, expected in cases:
            with self.subTest(endpoint=expected), contextlib.redirect_stdout(io.StringIO()) as output:
                with patch.object(diagnostics, "metadata", side_effect=outcomes):
                    with self.assertRaises(RuntimeError):
                        diagnostics.check("127.0.0.1:9092", 10, object)
            self.assertIn(expected, output.getvalue())
            self.assertNotIn("SUCCESS:", output.getvalue())

    def test_transient_errors_retry_once_on_same_client(self):
        errors = [
            KafkaError(KafkaError._TIMED_OUT),
            KafkaError(KafkaError._TRANSPORT),
            KafkaError(KafkaError._ALL_BROKERS_DOWN),
            KafkaError(KafkaError.REQUEST_TIMED_OUT, "temporary", False, True),
        ]
        for error in errors:
            expected = cluster()
            client = Mock()
            client.list_topics.side_effect = [KafkaException(error), expected]
            admin = Mock(return_value=client)
            with self.subTest(error=error), patch.object(diagnostics.time, "sleep") as pause:
                result = diagnostics.metadata("localhost:9092", 3, admin)
            self.assertIs(result, expected)
            admin.assert_called_once()
            self.assertEqual(client.list_topics.call_args_list, [call(timeout=3), call(timeout=3)])
            pause.assert_called_once_with(0.25)

    def test_permanent_and_non_kafka_errors_are_not_retried(self):
        errors = [
            KafkaException(KafkaError(KafkaError.CLUSTER_AUTHORIZATION_FAILED)),
            KafkaException(KafkaError(KafkaError._INVALID_ARG)),
            RuntimeError("invalid configuration"),
        ]
        for error in errors:
            admin = Mock()
            admin.return_value.list_topics.side_effect = error
            with self.subTest(error=error), patch.object(diagnostics.time, "sleep") as pause:
                with self.assertRaises(type(error)) as raised:
                    diagnostics.metadata("localhost:9092", 10, admin)
            self.assertIs(raised.exception, error)
            admin.assert_called_once()
            admin.return_value.list_topics.assert_called_once_with(timeout=10)
            pause.assert_not_called()

    def test_repeated_failure_stops_after_two_requests(self):
        error = KafkaException(KafkaError(KafkaError._TIMED_OUT))
        admin = Mock()
        admin.return_value.list_topics.side_effect = error
        with patch.object(diagnostics.time, "sleep") as pause:
            with self.assertRaises(KafkaException) as raised:
                diagnostics.metadata("localhost:9092", 2, admin)
        self.assertIs(raised.exception, error)
        admin.assert_called_once()
        self.assertEqual(admin.return_value.list_topics.call_args_list, [call(timeout=2), call(timeout=2)])
        pause.assert_called_once_with(0.25)

    def test_cli_forwards_address_family_override(self):
        for family in ("any", "v4", "v6"):
            with self.subTest(family=family), patch.object(diagnostics, "check") as check:
                code = diagnostics.main(["--address-family", family, "--timeout", "4"])
            self.assertEqual(code, 0)
            self.assertEqual(check.call_args.args[:2], ("127.0.0.1:9092", 4))
            self.assertEqual(check.call_args.kwargs, {"topic": None, "address_family": family})

    def test_cli_rejects_unknown_family_before_checking(self):
        with patch.object(diagnostics, "check") as check, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                diagnostics.main(["--address-family", "ipv4"])
        self.assertEqual(raised.exception.code, 2)
        check.assert_not_called()


if __name__ == "__main__":
    unittest.main()
