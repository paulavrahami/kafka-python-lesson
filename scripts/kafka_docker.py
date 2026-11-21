"""Manage the local teaching container without deleting containers or data.

Run from the project directory: uv run --locked python scripts/kafka_docker.py start
The Docker CLI and a running local Docker Desktop/Linux engine are prerequisites.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse

NAME = "kafka-python-lab"
IMAGE = "apache/kafka:4.4.0"
BINDING = {"HostIp": "127.0.0.1", "HostPort": "9092"}
TOPICS_COMMAND = [
    "exec", "--interactive", NAME, "/opt/kafka/bin/kafka-topics.sh",
    "--bootstrap-server", "localhost:9092", "--list", "--command-config", "/dev/stdin",
]
READINESS_CONFIG = "request.timeout.ms=5000\ndefault.api.timeout.ms=5000\n"


class LabError(Exception):
    """An actionable local setup error."""


def docker(*args, timeout=30, check=True, stream=False, input_text=None):
    try:
        result = subprocess.run(
            ["docker", *args], capture_output=not stream, text=True,
            encoding="utf-8", errors="replace", timeout=timeout, check=False,
            input=input_text,
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
        raise LabError("Docker engine is unavailable. Open Docker Desktop, wait until it is running, then retry.\n"
                       + (result.stderr or "").strip())
    if result.stdout.strip() != "linux":
        raise LabError("Kafka needs Linux containers. Switch Docker Desktop to Linux containers, then retry.")


def inspect_container():
    found = docker("container", "ls", "--all", "--filter", f"name=^/{NAME}$", "--format", "{{.ID}}")
    if not found.stdout.strip():
        return None
    try:
        return json.loads(docker("container", "inspect", NAME).stdout)[0]
    except (ValueError, IndexError, TypeError) as exc:
        raise LabError("Could not read the lesson container configuration.") from exc


def validate_container(container):
    image = container.get("Config", {}).get("Image")
    bindings = container.get("HostConfig", {}).get("PortBindings") or {}
    if image not in {IMAGE, "docker.io/" + IMAGE} or bindings.get("9092/tcp") != [BINDING]:
        raise LabError(
            f"A container named {NAME} already exists with a different image or port binding.\n"
            f"Expected {IMAGE} and 127.0.0.1:9092 -> 9092/tcp.\n"
            f"Inspect it with: docker inspect {NAME}\n"
            "It was not modified. Resolve the name/configuration conflict before retrying."
        )


def wait_ready(seconds):
    deadline = time.monotonic() + seconds
    print("Waiting for Kafka to answer metadata requests...", flush=True)
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            result = docker(*TOPICS_COMMAND, timeout=min(15, max(0.1, remaining)), check=False,
                            input_text=READINESS_CONFIG)
            if result.returncode == 0:
                print("Kafka is ready inside the container.")
                print("Host bootstrap address: 127.0.0.1:9092")
                print("Next: uv run --locked python scripts/check_kafka.py")
                return
        except LabError as exc:
            if "timed out" not in str(exc):
                raise
        time.sleep(max(0, min(2, deadline - time.monotonic())))
    raise LabError(f"Kafka was not ready within {seconds} seconds. The container was left in place.\n"
                   f"Check: docker logs --tail 80 {NAME}\n"
                   "Retry start after the broker has finished starting.")


def start(container, seconds):
    if container is None:
        present = docker("image", "ls", "--quiet", IMAGE).stdout.strip()
        if not present:
            print(f"Downloading {IMAGE}; the first download may take several minutes...", flush=True)
            docker("pull", IMAGE, timeout=900, stream=True)
        print(f"Creating {NAME}...", flush=True)
        result = docker("run", "--detach", "--name", NAME, "--publish", "127.0.0.1:9092:9092", IMAGE,
                        timeout=60, check=False)
        if result.returncode:
            raise LabError("Could not start the lesson container. Port 9092 may already be in use.\n"
                           + (result.stderr or result.stdout or "").strip()
                           + "\nNo containers were deleted. Check Docker Desktop before retrying.")
    else:
        validate_container(container)
        state = container.get("State", {})
        if state.get("Paused") or state.get("Restarting"):
            raise LabError("The lesson container is paused or restarting. Check its state in Docker Desktop.")
        if not state.get("Running"):
            print(f"Starting the existing {NAME} container...", flush=True)
            docker("start", NAME, timeout=60)
        else:
            print(f"{NAME} is already running; checking readiness...", flush=True)
    wait_ready(seconds)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "status", "stop", "logs"])
    parser.add_argument("--wait-seconds", type=int, default=120, help="Kafka readiness limit for start (default: 120)")
    args = parser.parse_args(argv)
    if args.wait_seconds < 1 or args.wait_seconds > 600:
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
        state = container.get("State", {})
        if args.action == "status":
            print(f"Container: {NAME}\nImage: {IMAGE}\nState: {state.get('Status', 'unknown')}")
            print("Published address: 127.0.0.1:9092 (Kafka readiness is checked separately)")
            return 0 if state.get("Running") and not state.get("Paused") else 1
        if args.action == "logs":
            docker("logs", "--tail", "80", NAME, stream=True)
        elif state.get("Running"):
            docker("stop", "--time", "30", NAME, timeout=45, stream=True)
            print("Lesson container stopped. Run start to use this same container again.")
        else:
            print("Lesson container is already stopped.")
        return 0
    except LabError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. The container may still be running; check Docker Desktop or run status.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
