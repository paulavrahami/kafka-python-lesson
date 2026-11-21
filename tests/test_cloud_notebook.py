"""Execute the Cloud notebook with finite local fakes, without network access.

The actual configuration loader, preflight, producer/consumer runner and seven
notebook code cells run. Only external services and the unbounded source are
replaced. Credentials below are deliberately fake and exist in a temporary file.
"""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import kafka_opensearch_streaming as runner
from scripts import wikimedia_stream as source
from tests.test_kafka_opensearch_streaming import Services, TopicAdmin, event


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "03_Wikimedia_Confluent_Cloud_HE.ipynb"
TOPIC = "python.wikimedia.cloud.v1"
GROUP = "python-wikimedia-cloud-opensearch-v1"
INDEX = "wikimedia-cloud-recentchanges-v1"
BOOTSTRAP = "pkc-lesson.eu-central-1.aws.confluent.cloud:9092"
FAKE_KEY = "notebook-test-key-123"
FAKE_SECRET = "notebook/test+secret=456"


class CloudNotebookTests(unittest.TestCase):
    def test_all_seven_cells_use_real_loader_and_runner_with_three_finite_events(self):
        notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
        cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
        self.assertEqual(len(cells), 7)
        services = Services()
        admin = TopicAdmin(forbid_create=True)
        fixtures = [event(f"cloud-notebook-{number}") for number in range(3)]
        original_add_raw = services.add_raw
        source_calls = []
        queries = []

        def add_raw(payload, headers=()):
            message = original_add_raw(payload, headers)
            message.topic = lambda: TOPIC
            return message

        services.add_raw = add_raw

        def finite_source(**kwargs):
            source_calls.append(kwargs)
            self.assertIsNone(kwargs["max_events"])
            self.assertIsNone(kwargs["max_seconds"])
            self.assertFalse(kwargs["stop_event"].is_set())
            stats = kwargs["stats"]
            for row in fixtures:
                stats["received"] = stats.get("received", 0) + 1
                yield row
            stats["stop_reason"] = "finite_test_source_exhausted"

        def search_factory(*args, **kwargs):
            client = services.search_factory(*args, **kwargs)
            client.count = lambda **options: {"count": len(services.documents)}

            def search(*, index, body):
                self.assertEqual(index, INDEX)
                queries.append(body)
                rows = list(services.documents.values())
                if "aggs" in body:
                    self.assertEqual(body["query"]["range"]["event_time"],
                                     {"gte": "now-15m", "lte": "now"})
                    self.assertEqual(body["aggs"]["by_site"]["terms"]["field"], "server_name")
                    self.assertEqual(body["aggs"]["by_bot"]["terms"]["field"], "bot")
                    self.assertEqual(body["aggs"]["per_minute"]["date_histogram"]["fixed_interval"], "1m")
                    return {"aggregations": {name: {"buckets": []} for name in body["aggs"]}}
                self.assertEqual(body["sort"], [{"event_time": {"order": "desc"}}])
                if "term" in body["query"]:
                    site = body["query"]["term"]["server_name"]
                    rows = [row for row in rows if row["server_name"] == site]
                else:
                    self.assertEqual(body["query"], {"match_all": {}})
                return {"hits": {"total": {"value": len(rows), "relation": "eq"},
                                 "hits": [{"_source": row} for row in rows[:body["size"]]]}}

            client.search = search
            return client

        output, namespace = io.StringIO(), {"__name__": "__cloud_notebook_test__"}
        previous_directory = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="cloud-notebook-test-") as temporary:
            properties = Path(temporary) / "confluent-cloud.properties"
            properties.write_text(
                f"bootstrap.servers={BOOTSTRAP}\n"
                "security.protocol=SASL_SSL\n"
                "sasl.mechanism=PLAIN\n"
                f"sasl.username={FAKE_KEY}\n"
                f"sasl.password={FAKE_SECRET}\n", encoding="utf-8",
            )
            try:
                os.chdir(temporary)
                with admin.installed(services), \
                        patch.object(source, "iter_wikimedia_events", side_effect=finite_source), \
                        patch.object(runner, "create_opensearch_client", side_effect=search_factory), \
                        redirect_stdout(output):
                    for number, cell in enumerate(cells, 1):
                        code = "".join(cell["source"])
                        exec(compile(code, f"Notebook03 cell {number}", "exec"), namespace)
            finally:
                os.chdir(previous_directory)

        self.assertEqual(len(source_calls), 1)
        self.assertTrue(namespace["stop_event"].is_set())
        self.assertEqual(namespace["source_stats"]["received"], 3)
        report = namespace["stream_report"]
        for name in ("accepted", "confirmed", "consumed", "indexed", "committed", "committed_current_run"):
            self.assertEqual(report[name], 3, name)
        self.assertEqual(report["status"], "source_exhausted")
        self.assertEqual(report["consumed_other_runs"], 0)
        self.assertTrue(report["producer_drained"])
        self.assertTrue(report["current_run_committed"])
        self.assertTrue(report["worker_closed"])
        self.assertIsNone(report["backlog_fully_drained"])
        self.assertEqual(len(services.documents), 3)
        self.assertEqual(len(queries), 3)
        self.assertTrue(all(client.closed for client in services.search_clients))
        self.assertTrue(all(consumer.closed for consumer in services.consumers))
        self.assertEqual(services.group_offsets[GROUP], 2)
        self.assertEqual(admin.created, [])
        self.assertEqual([request["topic"] for request in admin.metadata_requests], [TOPIC])
        for client in (*admin.clients, *services.producers, *services.consumers):
            config = client.config
            self.assertEqual(config["bootstrap.servers"], BOOTSTRAP)
            self.assertEqual(config["security.protocol"], "SASL_SSL")
            self.assertEqual(config["sasl.mechanism"], "PLAIN")
            self.assertTrue(config["enable.ssl.certificate.verification"])
            self.assertEqual(config["ssl.endpoint.identification.algorithm"], "https")
        self.assertFalse(services.consumers[0].config["enable.auto.commit"])
        self.assertTrue(services.producers[0].config["enable.idempotence"])
        self.assertEqual(services.producers[0].config["delivery.timeout.ms"], 60000)
        for row in fixtures:
            identity = row["meta"]["id"]
            self.assertLess(services.trace.index(("indexed", identity)),
                            services.trace.index(("commit", identity)))
        displayed = output.getvalue()
        self.assertIn("Documents in index: 3", displayed)
        self.assertIn("SASL_SSL", displayed)
        self.assertNotIn(FAKE_KEY, displayed)
        self.assertNotIn(FAKE_SECRET, displayed)


if __name__ == "__main__":
    unittest.main()
