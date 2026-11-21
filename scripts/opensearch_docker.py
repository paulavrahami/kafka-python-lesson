"""Manage the local OpenSearch teaching container without deleting saved data.

Run: uv run --locked python scripts/opensearch_docker.py start
Actions: start, status, logs, stop. Requires a running local Docker Linux engine.
This development-only setup disables authentication/TLS and binds to 127.0.0.1.
Official setup: https://docs.opensearch.org/latest/install-and-configure/install-opensearch/docker/
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, build_opener

NAME = "opensearch-python-lab"
IMAGE = "opensearchproject/opensearch:3.10.0"
VOLUME = "opensearch-python-lab-data"
DATA_PATH = "/usr/share/opensearch/data"
URL = "http://127.0.0.1:9200"
BINDING = {"HostIp": "127.0.0.1", "HostPort": "9200"}
LABELS = {"io.kafka-python-lesson.component": "opensearch-local-dev"}
ENVIRONMENT = {
    "discovery.type": "single-node",
    "cluster.name": NAME,
    "node.name": NAME,
    "bootstrap.memory_lock": "true",
    "OPENSEARCH_JAVA_OPTS": "-Xms512m -Xmx512m",
    "DISABLE_INSTALL_DEMO_CONFIG": "true",
    "DISABLE_SECURITY_PLUGIN": "true",
}
SETUP_HINT = (
    "OpenSearch needs vm.max_map_count >= 262144 on the Docker host.\n"
    "Windows / Docker Desktop WSL: wsl -d docker-desktop\n"
    "Then run: sysctl -w vm.max_map_count=262144\n"
    "Then run: exit\n"
    "Allow at least 4 GB RAM for Docker Desktop; Kafka and the notebook also use memory.\n"
    "Check logs with: uv run --locked python scripts/opensearch_docker.py logs"
)


class LabError(Exception):
    """An actionable local setup error."""


def docker(*args, timeout=30, check=True, stream=False):
    try:
        result = subprocess.run(
            ["docker", *args], capture_output=not stream, text=True,
            encoding="utf-8", errors="replace", timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise LabError(f"Docker command timed out: docker {' '.join(args)}") from exc
    except OSError as exc:
        raise LabError(f"Could not run Docker: {exc}") from exc
    if check and result.returncode:
        detail = (result.stderr or result.stdout or "See the Docker output above.").strip()
        raise LabError(f"docker {' '.join(args)} failed:\n{detail}")
    return result


def check_engine():
    if shutil.which("docker") is None:
        raise LabError("Docker CLI is missing. Install Docker Desktop, then reopen the terminal.")
    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint or os.environ.get("DOCKER_CONTEXT"):
        endpoint = docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}").stdout.strip()
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"unix", "npipe"} and not (
        parsed.scheme == "tcp" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    ):
        raise LabError("This helper requires a local Docker context. Select your local Docker Desktop context.")
    result = docker("info", "--format", "{{.OSType}}", check=False)
    if result.returncode:
        raise LabError("Docker engine is unavailable. Open Docker Desktop and wait until it is running.\n"
                       + (result.stderr or "").strip())
    if result.stdout.strip() != "linux":
        raise LabError("OpenSearch needs Linux containers. Switch Docker Desktop to Linux containers.")


def read_inspect(kind, name):
    try:
        result = json.loads(docker(kind, "inspect", name).stdout)
        if not isinstance(result, list) or not result or not isinstance(result[0], dict):
            raise ValueError("Expected an inspect object")
        return result[0]
    except (ValueError, TypeError) as exc:
        raise LabError(f"Could not read the {kind} configuration for {name}.") from exc


def inspect_container():
    found = docker("container", "ls", "--all", "--filter", f"name=^/{NAME}$", "--format", "{{.ID}}")
    return read_inspect("container", NAME) if found.stdout.strip() else None


def validate_container(container):
    config = container.get("Config") or {}
    host = container.get("HostConfig") or {}
    env = dict(item.split("=", 1) for item in (config.get("Env") or []) if "=" in item)
    limits = {item.get("Name"): (item.get("Soft"), item.get("Hard"))
              for item in (host.get("Ulimits") or [])}
    mounts = [mount for mount in (container.get("Mounts") or [])
              if mount.get("Destination") == DATA_PATH]
    valid_mount = (len(mounts) == 1 and mounts[0].get("Type") == "volume"
                   and mounts[0].get("Name") == VOLUME and mounts[0].get("RW") is True)
    if not (
        config.get("Image") in {IMAGE, "docker.io/" + IMAGE}
        and host.get("PortBindings") == {"9200/tcp": [BINDING]}
        and host.get("NetworkMode") in {"bridge", "default"}
        and all(env.get(key) == value for key, value in ENVIRONMENT.items())
        and all((config.get("Labels") or {}).get(key) == value for key, value in LABELS.items())
        and limits.get("memlock") == (-1, -1)
        and limits.get("nofile") == (65536, 65536)
        and valid_mount
    ):
        raise LabError(
            f"A container named {NAME} already exists with a different configuration.\n"
            f"Expected {IMAGE}, localhost port 9200, and named data volume {VOLUME}.\n"
            f"Inspect it with: docker inspect {NAME}\n"
            "It was not modified. Resolve the name/configuration conflict before retrying."
        )


def ensure_volume():
    found = docker("volume", "ls", "--filter", f"name={VOLUME}", "--format", "{{.Name}}")
    if VOLUME in found.stdout.splitlines():
        existing = read_inspect("volume", VOLUME)
        if not all((existing.get("Labels") or {}).get(key) == value for key, value in LABELS.items()):
            raise LabError(
                f"Volume {VOLUME} already exists but is not labelled for this lesson.\n"
                f"Inspect it with: docker volume inspect {VOLUME}\n"
                "Its contents were not modified. Resolve the name conflict before retrying."
            )
        print(f"Reusing saved data in {VOLUME}.", flush=True)
        return
    args = ["volume", "create"]
    for key, value in LABELS.items():
        args.extend(["--label", f"{key}={value}"])
    docker(*args, VOLUME)


def run_arguments():
    args = ["run", "--detach", "--name", NAME, "--publish", "127.0.0.1:9200:9200",
            "--mount", f"type=volume,source={VOLUME},target={DATA_PATH}",
            "--ulimit", "memlock=-1:-1", "--ulimit", "nofile=65536:65536"]
    for key, value in LABELS.items():
        args.extend(["--label", f"{key}={value}"])
    for key, value in ENVIRONMENT.items():
        args.extend(["--env", f"{key}={value}"])
    args.append(IMAGE)
    return args


def probe_ready(timeout=2):
    """Check the host port, service identity, and writable primary shards.

    Bypass HTTP proxy variables: this probe must go directly to loopback.
    Yellow is healthy enough for a single node with unassigned replicas.
    """
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(URL + "/", timeout=timeout) as response:
            info = json.load(response)
        if (info.get("version", {}).get("distribution") != "opensearch"
                or info.get("cluster_name") != NAME):
            return False, "Port 9200 is not serving this lesson's OpenSearch cluster."
        with opener.open(URL + "/_cluster/health", timeout=timeout) as response:
            health = json.load(response)
        ready = (health.get("status") in {"yellow", "green"}
                 and health.get("number_of_nodes", 0) >= 1
                 and health.get("timed_out") is False)
        return ready, f"Cluster health: {health.get('status', 'unknown')}"
    except (HTTPError, URLError, OSError, ValueError, TypeError, AttributeError) as exc:
        return False, f"OpenSearch is not ready at {URL}: {exc}"


def wait_ready(seconds):
    deadline = time.monotonic() + seconds
    last_problem = "No health response received."
    print(f"Waiting for OpenSearch at {URL}...", flush=True)
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        ready, last_problem = probe_ready(timeout=min(2, max(0.1, remaining / 2)))
        if ready:
            print(f"OpenSearch is ready: {URL}\nData volume: {VOLUME}")
            return
        time.sleep(max(0, min(2, deadline - time.monotonic())))
    raise LabError(f"OpenSearch was not ready within {seconds} seconds.\n{last_problem}\n"
                   "The container and its data were left in place.\n" + SETUP_HINT)


def start(container, seconds):
    print("Local development only: OpenSearch authentication/TLS are disabled; host binding is 127.0.0.1.")
    if container is None:
        # Validate or create labelled storage before starting a container.
        ensure_volume()
        if not docker("image", "ls", "--quiet", IMAGE).stdout.strip():
            print(f"Downloading {IMAGE}; the first download may take several minutes...", flush=True)
            docker("pull", IMAGE, timeout=900, stream=True)
        result = docker(*run_arguments(), timeout=60, check=False)
        if result.returncode:
            raise LabError("Could not start OpenSearch. Port 9200 may already be in use.\n"
                           + (result.stderr or result.stdout or "").strip()
                           + "\nNo containers or volumes were deleted.\n" + SETUP_HINT)
    else:
        validate_container(container)
        state = container.get("State") or {}
        if state.get("Paused") or state.get("Restarting"):
            raise LabError("The lesson container is paused or restarting. Check Docker Desktop.")
        if not state.get("Running"):
            print(f"Starting the existing {NAME} container with its saved data...", flush=True)
            docker("start", NAME, timeout=60)
        else:
            print(f"{NAME} is already running; checking readiness...", flush=True)
    wait_ready(seconds)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "status", "stop", "logs"])
    parser.add_argument("--wait-seconds", type=int, default=120,
                        help="OpenSearch readiness limit for start (default: 120)")
    args = parser.parse_args(argv)
    if not 1 <= args.wait_seconds <= 600:
        parser.error("--wait-seconds must be between 1 and 600")
    try:
        check_engine()
        container = inspect_container()
        if args.action == "start":
            start(container, args.wait_seconds)
            return 0
        if container is None:
            print(f"{NAME} does not exist. Run the start command first.")
            return 1 if args.action in {"status", "logs"} else 0
        validate_container(container)
        state = container.get("State") or {}
        if args.action == "status":
            print(f"Container: {NAME}\nImage: {IMAGE}\nState: {state.get('Status', 'unknown')}\n"
                  f"Address: {URL}\nData volume: {VOLUME}")
            if not state.get("Running") or state.get("Paused") or state.get("Restarting"):
                return 1
            ready, detail = probe_ready()
            print(detail)
            return 0 if ready else 1
        if args.action == "logs":
            docker("logs", "--tail", "80", NAME, stream=True)
        elif state.get("Running"):
            docker("stop", "--time", "30", NAME, timeout=45, stream=True)
            print(f"OpenSearch stopped. Container and volume {VOLUME} are preserved; run start to reuse them.")
        else:
            print(f"OpenSearch is already stopped. Saved data remains in {VOLUME}.")
        return 0
    except LabError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. No data was deleted. Check status; the container may still be running.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
