"""Mocked Docker-control and Kafka metadata checks; no live broker is used."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import types
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lab = load("kafka_docker")
host = load("check_kafka")


def container(running=True):
    return {"Config": {"Image": lab.IMAGE},
            "HostConfig": {"PortBindings": {"9092/tcp": [lab.BINDING]}},
            "State": {"Running": running, "Status": "running" if running else "exited"}}


def result(output="", code=0, error=""):
    return subprocess.CompletedProcess([], code, output, error)


class DockerTests(unittest.TestCase):
    def setUp(self):
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.errors = contextlib.redirect_stderr(io.StringIO())
        self.output.__enter__()
        self.errors.__enter__()

    def tearDown(self):
        self.errors.__exit__(None, None, None)
        self.output.__exit__(None, None, None)

    def test_new_container_has_loopback_binding_and_no_delete(self):
        with patch.object(lab, "docker", side_effect=[result(), result(), result("new-id")]) as run:
            with patch.object(lab, "wait_ready") as ready:
                lab.start(None, 120)
        calls = [call.args for call in run.call_args_list]
        self.assertIn(("pull", lab.IMAGE), calls)
        self.assertEqual(calls[-1], ("run", "--detach", "--name", lab.NAME, "--publish", "127.0.0.1:9092:9092", lab.IMAGE))
        self.assertFalse(any("rm" in call or "prune" in call for call in calls))
        ready.assert_called_once_with(120)

    def test_existing_running_container_is_reused(self):
        with patch.object(lab, "docker") as run, patch.object(lab, "wait_ready") as ready:
            lab.start(container(), 120)
        run.assert_not_called()
        ready.assert_called_once_with(120)

    def test_stopped_container_is_started_without_recreation(self):
        with patch.object(lab, "docker") as run, patch.object(lab, "wait_ready"):
            lab.start(container(False), 120)
        run.assert_called_once_with("start", lab.NAME, timeout=60)

    def test_mismatched_existing_image_and_public_port_are_rejected(self):
        for field in ("image", "port"):
            value = container()
            if field == "image":
                value["Config"]["Image"] = "unrelated:latest"
            else:
                value["HostConfig"]["PortBindings"]["9092/tcp"] = [{"HostIp": "", "HostPort": "9092"}]
            with self.subTest(field=field), patch.object(lab, "docker") as run:
                with self.assertRaises(lab.LabError):
                    lab.start(value, 120)
                run.assert_not_called()

    def test_start_port_conflict_does_not_delete_container(self):
        with patch.object(lab, "docker", side_effect=[result("image-id"), result(code=125, error="port is already allocated")]) as run:
            with self.assertRaisesRegex(lab.LabError, "Port 9092"):
                lab.start(None, 120)
        self.assertEqual(len(run.call_args_list), 2)

    def test_missing_cli_and_unavailable_daemon_are_actionable(self):
        with patch.object(lab.shutil, "which", return_value=None):
            with self.assertRaisesRegex(lab.LabError, "Install Docker Desktop"):
                lab.check_engine()
        with patch.object(lab.shutil, "which", return_value="docker"), patch.dict(lab.os.environ, {}, clear=True):
            with patch.object(lab, "docker", side_effect=[result("unix:///var/run/docker.sock"), result(code=1, error="unavailable")]):
                with self.assertRaisesRegex(lab.LabError, "Open Docker Desktop"):
                    lab.check_engine()

    def test_remote_context_and_windows_engine_are_rejected(self):
        with patch.object(lab.shutil, "which", return_value="docker"), patch.dict(lab.os.environ, {}, clear=True):
            with patch.object(lab, "docker", return_value=result("ssh://remote-host")):
                with self.assertRaisesRegex(lab.LabError, "local Docker context"):
                    lab.check_engine()
            with patch.object(lab, "docker", side_effect=[result("npipe:////./pipe/docker_engine"), result("windows")]):
                with self.assertRaisesRegex(lab.LabError, "Linux containers"):
                    lab.check_engine()

    def test_successful_ready_check_is_read_only_and_bounded(self):
        with patch.object(lab, "docker", return_value=result()) as run:
            lab.wait_ready(120)
        self.assertEqual(run.call_args.args, tuple(lab.TOPICS_COMMAND))
        self.assertIn("--list", run.call_args.args)
        self.assertEqual(run.call_args.kwargs["input_text"], lab.READINESS_CONFIG)
        self.assertLessEqual(run.call_args.kwargs["timeout"], 15)

    def test_readiness_timeout_leaves_container_in_place(self):
        with patch.object(lab.time, "monotonic", side_effect=[0, 0, 0, 1, 2]), patch.object(lab.time, "sleep"):
            with patch.object(lab, "docker", return_value=result(code=1)) as run:
                with self.assertRaisesRegex(lab.LabError, "left in place"):
                    lab.wait_ready(1)
        self.assertEqual(len(run.call_args_list), 1)

    def test_stop_only_validated_named_container(self):
        with patch.object(lab, "check_engine"), patch.object(lab, "inspect_container", return_value=container()), patch.object(lab, "docker") as run:
            self.assertEqual(lab.main(["stop"]), 0)
        run.assert_called_once_with("stop", "--time", "30", lab.NAME, timeout=45, stream=True)
        value = container()
        value["Config"]["Image"] = "other"
        with patch.object(lab, "check_engine"), patch.object(lab, "inspect_container", return_value=value), patch.object(lab, "docker") as run:
            self.assertEqual(lab.main(["stop"]), 1)
        run.assert_not_called()

    def test_subprocess_has_no_shell_and_timeout_becomes_error(self):
        with patch.object(lab.subprocess, "run", return_value=result()) as run:
            lab.docker("info")
        self.assertNotIn("shell", run.call_args.kwargs)
        self.assertEqual(run.call_args.args[0], ["docker", "info"])
        with patch.object(lab.subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 30)):
            with self.assertRaisesRegex(lab.LabError, "timed out"):
                lab.docker("info")

    def test_interrupt_returns_130(self):
        with patch.object(lab, "check_engine", side_effect=KeyboardInterrupt):
            self.assertEqual(lab.main(["start"]), 130)


class MetadataTests(unittest.TestCase):
    def metadata(self, cluster="lesson", brokers=True):
        broker = types.SimpleNamespace(id=1, host="localhost", port=9092)
        return types.SimpleNamespace(cluster_id=cluster, brokers={1: broker} if brokers else {}, topics={})

    def test_checks_bootstrap_and_advertised_address(self):
        with patch.object(host, "metadata", return_value=self.metadata()) as read, contextlib.redirect_stdout(io.StringIO()):
            host.check("127.0.0.1:9092", 10, object)
        self.assertEqual([call.args[0] for call in read.call_args_list], ["127.0.0.1:9092", "localhost:9092"])

    def test_unreachable_advertised_endpoint_fails(self):
        with patch.object(host, "metadata", side_effect=[self.metadata(), RuntimeError("unreachable")]), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "unreachable"):
                host.check("127.0.0.1:9092", 10, object)

    def test_empty_metadata_and_wrong_cluster_fail(self):
        with patch.object(host, "metadata", return_value=self.metadata(brokers=False)):
            with self.assertRaisesRegex(RuntimeError, "without any brokers"):
                host.check("127.0.0.1:9092", 10, object)
        with patch.object(host, "metadata", side_effect=[self.metadata(), self.metadata("other")]), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "different Kafka cluster"):
                host.check("127.0.0.1:9092", 10, object)

    def test_metadata_does_not_create_topics(self):
        created = []
        class Admin:
            def __init__(self, configuration):
                created.append(configuration)
            def list_topics(self, **kwargs):
                created.append(kwargs)
                return "metadata"
        self.assertEqual(host.metadata("127.0.0.1:9092", 10, Admin), "metadata")
        self.assertFalse(created[0]["allow.auto.create.topics"])
        self.assertEqual(created[1], {"timeout": 10})


if __name__ == "__main__":
    unittest.main(verbosity=2)
