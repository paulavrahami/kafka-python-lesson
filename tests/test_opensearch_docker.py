"""Local OpenSearch Docker lifecycle checks; no Docker daemon is used."""

from io import BytesIO, StringIO
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from scripts import opensearch_docker as lab


def result(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def container(running=True):
    return {
        "Config": {"Image": lab.IMAGE, "Labels": dict(lab.LABELS),
                   "Env": [f"{key}={value}" for key, value in lab.ENVIRONMENT.items()]},
        "HostConfig": {"PortBindings": {"9200/tcp": [dict(lab.BINDING)]},
                       "NetworkMode": "bridge",
                       "Ulimits": [{"Name": "memlock", "Soft": -1, "Hard": -1},
                                   {"Name": "nofile", "Soft": 65536, "Hard": 65536}]},
        "Mounts": [{"Type": "volume", "Name": lab.VOLUME,
                    "Destination": lab.DATA_PATH, "RW": True}],
        "State": {"Running": running, "Status": "running" if running else "exited"},
    }


class DockerSafetyTests(unittest.TestCase):
    def test_command_has_only_loopback_port_persistent_volume_and_pinned_image(self):
        argv = lab.run_arguments()
        options = {}
        for index in range(2, len(argv) - 1, 2):
            options.setdefault(argv[index], []).append(argv[index + 1])
        self.assertEqual(argv[:2], ["run", "--detach"])
        self.assertEqual(argv[-1], "opensearchproject/opensearch:3.10.0")
        self.assertEqual(options["--publish"], ["127.0.0.1:9200:9200"])
        self.assertEqual(options["--mount"], [f"type=volume,source={lab.VOLUME},target={lab.DATA_PATH}"])
        self.assertEqual(set(options["--ulimit"]), {"memlock=-1:-1", "nofile=65536:65536"})
        self.assertIn("OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m", options["--env"])
        self.assertIn("DISABLE_SECURITY_PLUGIN=true", options["--env"])
        self.assertIn("DISABLE_INSTALL_DEMO_CONFIG=true", options["--env"])
        self.assertNotIn("--rm", argv)
        self.assertNotIn("--network", argv)

    def test_missing_docker_has_actionable_error_without_commands(self):
        with patch.object(lab.shutil, "which", return_value=None), patch.object(lab, "docker") as docker:
            with self.assertRaisesRegex(lab.LabError, "Install Docker Desktop"):
                lab.check_engine()
        docker.assert_not_called()

    def test_remote_docker_is_rejected_before_any_service_command(self):
        with patch.object(lab.shutil, "which", return_value="docker"), \
                patch.dict(lab.os.environ, {"DOCKER_HOST": "tcp://cloud.example:2376"}, clear=True), \
                patch.object(lab, "docker") as docker:
            with self.assertRaisesRegex(lab.LabError, "local Docker context"):
                lab.check_engine()
        docker.assert_not_called()

    def test_explicit_context_overrides_docker_host_and_requires_linux(self):
        with patch.object(lab.shutil, "which", return_value="docker"), \
                patch.dict(lab.os.environ, {"DOCKER_HOST": "tcp://cloud.example:2376",
                                           "DOCKER_CONTEXT": "desktop-linux"}, clear=True), \
                patch.object(lab, "docker", side_effect=[result("npipe:////./pipe/docker_engine"),
                                                        result("linux")]) as docker:
            lab.check_engine()
        self.assertEqual(docker.call_args_list[0].args[:2], ("context", "inspect"))

    def test_windows_container_engine_is_rejected(self):
        with patch.object(lab.shutil, "which", return_value="docker"), \
                patch.dict(lab.os.environ, {"DOCKER_HOST": "npipe:////./pipe/docker_engine"}, clear=True), \
                patch.object(lab, "docker", return_value=result("windows")):
            with self.assertRaisesRegex(lab.LabError, "Linux containers"):
                lab.check_engine()

    def test_existing_mismatched_containers_are_not_started_or_modified(self):
        fixtures = []
        changed = container(False)
        changed["HostConfig"]["PortBindings"]["9200/tcp"][0]["HostIp"] = "0.0.0.0"
        fixtures.append(changed)
        changed = container(False)
        changed["HostConfig"]["NetworkMode"] = "host"
        fixtures.append(changed)
        changed = container(False)
        changed["Config"]["Image"] = "opensearchproject/opensearch:latest"
        fixtures.append(changed)
        changed = container(False)
        changed["Config"]["Env"] = [entry for entry in changed["Config"]["Env"]
                                    if not entry.startswith("DISABLE_SECURITY_PLUGIN=")]
        fixtures.append(changed)
        changed = container(False)
        changed["Mounts"][0]["Name"] = "someone-elses-data"
        fixtures.append(changed)
        changed = container(False)
        changed["Config"]["Labels"] = {}
        fixtures.append(changed)
        for fixture in fixtures:
            with self.subTest(fixture=fixture), patch.object(lab, "docker") as docker:
                with self.assertRaisesRegex(lab.LabError, "not modified"):
                    lab.start(fixture, 5)
                docker.assert_not_called()

    def test_unowned_existing_volume_is_never_mounted_or_modified(self):
        with patch.object(lab, "docker", side_effect=[result(lab.VOLUME + "\n"),
                                                     result(json.dumps([{"Labels": {}}]))]) as docker:
            with self.assertRaisesRegex(lab.LabError, "contents were not modified"):
                lab.start(None, 5)
        self.assertEqual([call.args[:2] for call in docker.call_args_list],
                         [("volume", "ls"), ("volume", "inspect")])

    def test_owned_volume_is_reused_without_create(self):
        with patch.object(lab, "docker", side_effect=[result(lab.VOLUME + "\n"),
                                                     result(json.dumps([{"Labels": lab.LABELS}]))]) as docker:
            lab.ensure_volume()
        self.assertEqual(len(docker.call_args_list), 2)

    def test_new_volume_is_labelled_and_never_deleted(self):
        with patch.object(lab, "docker", return_value=result()) as docker:
            lab.ensure_volume()
        create = docker.call_args_list[-1].args
        self.assertEqual(create[:2], ("volume", "create"))
        self.assertIn("--label", create)
        self.assertEqual(create[-1], lab.VOLUME)

    def test_existing_stopped_container_keeps_data_and_is_restarted(self):
        with patch.object(lab, "docker", return_value=result()) as docker, \
                patch.object(lab, "wait_ready") as wait:
            lab.start(container(False), 120)
        docker.assert_called_once_with("start", lab.NAME, timeout=60)
        wait.assert_called_once_with(120)

    def test_first_start_uses_named_volume_and_returns_after_readiness(self):
        with patch.object(lab, "docker", side_effect=[result("image-id"), result("container-id")]) as docker, \
                patch.object(lab, "ensure_volume") as volume, patch.object(lab, "wait_ready") as wait:
            lab.start(None, 45)
        volume.assert_called_once_with()
        self.assertEqual(docker.call_args_list[1].args, tuple(lab.run_arguments()))
        wait.assert_called_once_with(45)

    def test_stop_preserves_container_and_volume(self):
        with patch.object(lab, "check_engine"), patch.object(lab, "inspect_container", return_value=container()), \
                patch.object(lab, "docker", return_value=result()) as docker:
            self.assertEqual(lab.main(["stop"]), 0)
        docker.assert_called_once_with("stop", "--time", "30", lab.NAME, timeout=45, stream=True)


class ReadinessTests(unittest.TestCase):
    def probe(self, info=None, health=None):
        info = info if info is not None else {"version": {"distribution": "opensearch"}, "cluster_name": lab.NAME}
        health = health if health is not None else {"status": "yellow", "number_of_nodes": 1, "timed_out": False}
        opener = Mock()
        opener.open.side_effect = [BytesIO(json.dumps(info).encode()), BytesIO(json.dumps(health).encode())]
        with patch.object(lab, "build_opener", return_value=opener):
            value = lab.probe_ready()
        return value, opener

    def test_single_node_yellow_health_is_ready(self):
        (ready, detail), opener = self.probe()
        self.assertTrue(ready)
        self.assertIn("yellow", detail)
        self.assertEqual(opener.open.call_args_list[0].args[0], "http://127.0.0.1:9200/")
        self.assertEqual(opener.open.call_args_list[1].args[0], "http://127.0.0.1:9200/_cluster/health")

    def test_wrong_service_or_cluster_is_not_success(self):
        for info in ({"version": {"distribution": "elasticsearch"}, "cluster_name": lab.NAME},
                     {"version": {"distribution": "opensearch"}, "cluster_name": "different-cluster"}):
            with self.subTest(info=info):
                (ready, _), opener = self.probe(info=info)
                self.assertFalse(ready)
                self.assertEqual(opener.open.call_count, 1)

    def test_red_timed_out_or_invalid_health_is_not_success(self):
        for health in ({"status": "red", "number_of_nodes": 1, "timed_out": False},
                       {"status": "green", "number_of_nodes": 1, "timed_out": True},
                       {"status": "green", "number_of_nodes": 0, "timed_out": False}, []):
            with self.subTest(health=health):
                (ready, _), _ = self.probe(health=health)
                self.assertFalse(ready)

    def test_connection_refused_is_reported_without_traceback(self):
        opener = Mock()
        opener.open.side_effect = OSError("connection refused")
        with patch.object(lab, "build_opener", return_value=opener):
            ready, detail = lab.probe_ready()
        self.assertFalse(ready)
        self.assertIn("connection refused", detail)

    def test_readiness_deadline_explains_windows_setting_and_preserves_data(self):
        with patch.object(lab.time, "monotonic", side_effect=[0, 0, 0.2, 1.1, 1.1]), \
                patch.object(lab.time, "sleep"), \
                patch.object(lab, "probe_ready", return_value=(False, "connection refused")), \
                patch.object(lab, "docker") as docker:
            with self.assertRaisesRegex(lab.LabError, "vm.max_map_count") as raised:
                lab.wait_ready(1)
        self.assertIn("left in place", str(raised.exception))
        docker.assert_not_called()

    def test_status_requires_api_readiness_not_just_container_running(self):
        with patch.object(lab, "check_engine"), patch.object(lab, "inspect_container", return_value=container()), \
                patch.object(lab, "probe_ready", return_value=(False, "starting")):
            self.assertEqual(lab.main(["status"]), 1)

    def test_invalid_deadline_rejected_before_docker(self):
        with patch.object(lab, "check_engine") as engine, patch("sys.stderr", new_callable=StringIO):
            for seconds in ("0", "601", "nan"):
                with self.subTest(seconds=seconds), self.assertRaises(SystemExit):
                    lab.main(["start", "--wait-seconds", seconds])
        engine.assert_not_called()


if __name__ == "__main__":
    unittest.main()
