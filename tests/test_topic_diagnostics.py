"""Topic-readiness diagnostics with Kafka metadata doubles; no live broker."""

import contextlib
import importlib.util
import io
from pathlib import Path
import types
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "scripts/check_kafka.py"
SPEC = importlib.util.spec_from_file_location("topic_diagnostics", SOURCE)
diagnostics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostics)


def cluster(leader=1, isrs=None, topic_error=None, partition_error=None):
    part = types.SimpleNamespace(
        leader=leader, replicas=[1], isrs=[1] if isrs is None else isrs, error=partition_error
    )
    topic = types.SimpleNamespace(error=topic_error, partitions={0: part})
    broker = types.SimpleNamespace(id=1, host="localhost", port=9092)
    return types.SimpleNamespace(topics={"wiki.current": topic}, brokers={1: broker}, cluster_id="lab")


class TopicDiagnosticsTests(unittest.TestCase):
    def run_topic_check(self, value, name="wiki.current"):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            diagnostics.check_topic_metadata(value, name)
        return output.getvalue()

    def test_ready_metadata_prints_partition_details(self):
        text = self.run_topic_check(cluster())
        self.assertIn("partitions: 1", text)
        self.assertIn("leader=1, replicas=[1], ISR=[1]", text)
        self.assertIn("Topic metadata OK", text)

    def test_missing_topic_explains_run_id_and_creation_cells(self):
        with self.assertRaisesRegex(RuntimeError, "RUN_ID changed.*topic-creation"):
            self.run_topic_check(cluster(), "wiki.new-run")

    def test_topic_error_is_reported(self):
        with self.assertRaisesRegex(RuntimeError, "Topic metadata error.*authorization failed"):
            self.run_topic_check(cluster(topic_error="authorization failed"))

    def test_empty_partitions_fail(self):
        value = cluster()
        value.topics["wiki.current"].partitions = {}
        with self.assertRaisesRegex(RuntimeError, "no partitions"):
            self.run_topic_check(value)

    def test_partition_error_fails_even_when_leader_is_known(self):
        with self.assertRaisesRegex(RuntimeError, "partition 0: leader unavailable"):
            self.run_topic_check(cluster(partition_error="leader unavailable"))

    def test_unelected_or_unadvertised_leader_fails(self):
        for leader, expected in [(-1, "no elected leader"), (2, "absent from broker metadata")]:
            with self.subTest(leader=leader), self.assertRaisesRegex(RuntimeError, expected):
                self.run_topic_check(cluster(leader=leader))

    def test_empty_isr_and_isr_without_leader_fail(self):
        for isrs in [[], [2]]:
            with self.subTest(isrs=isrs), self.assertRaisesRegex(RuntimeError, "no synchronized leader"):
                self.run_topic_check(cluster(isrs=isrs))

    def test_optional_topic_uses_existing_all_topics_metadata_without_extra_request(self):
        admin = Mock()
        admin.return_value.list_topics.return_value = cluster()
        with contextlib.redirect_stdout(io.StringIO()) as output:
            diagnostics.check("127.0.0.1:9092", 10, admin, topic="wiki.current")
        self.assertEqual(admin.call_count, 2)  # Bootstrap and its advertised broker.
        for call in admin.call_args_list:
            self.assertIs(call.args[0]["allow.auto.create.topics"], False)
        self.assertEqual(admin.return_value.list_topics.call_count, 2)
        for call in admin.return_value.list_topics.call_args_list:
            self.assertEqual(call.args, ())
            self.assertEqual(call.kwargs, {"timeout": 10})
        self.assertIn("run the notebook for producer/consumer validation", output.getvalue())

    def test_cli_passes_topic_to_check(self):
        with patch.object(diagnostics, "check") as check:
            code = diagnostics.main(["--topic", "wiki.current"])
        self.assertEqual(code, 0)
        self.assertEqual(check.call_args.kwargs, {"topic": "wiki.current", "address_family": "v4"})


if __name__ == "__main__":
    unittest.main()
