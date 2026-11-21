"""Read-only Kafka metadata check from the notebook's host Python environment.

Run: uv run --locked python scripts/check_kafka.py
For one notebook topic, append: --topic NAME
This checks both the bootstrap address and the broker addresses Kafka advertises.
It does not create topics, produce messages, or change consumer offsets.
IPv4 is the default for this lab's 127.0.0.1 Docker port binding.
Each address gets at most two metadata requests; --timeout applies to each request.
"""

import argparse
import sys
import time


def _transient_metadata_error(exc):
    # Keep successful metadata doubles and the missing-package CLI independent
    # of this import. Only actual Kafka errors qualify for a retry.
    try:
        from confluent_kafka import KafkaError, KafkaException
    except ImportError:
        return False
    if not isinstance(exc, KafkaException) or not exc.args:
        return False
    error = exc.args[0]
    return isinstance(error, KafkaError) and (
        error.retriable()
        or error.code() in {KafkaError._TIMED_OUT, KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN}
    )


def metadata(address, timeout, admin_class, address_family="v4"):
    """Read metadata with one bounded retry on the same client for transient errors."""
    client = admin_class({
        "bootstrap.servers": address,
        "client.id": "kafka-python-lesson-check",
        "broker.address.family": address_family,
        "socket.connection.setup.timeout.ms": 5000,
        "socket.timeout.ms": int(timeout * 1000),
        "allow.auto.create.topics": False,
        "log_level": 0,
    })
    for attempt in range(2):
        try:
            return client.list_topics(timeout=timeout)
        except Exception as exc:
            if attempt == 1 or not _transient_metadata_error(exc):
                raise
            # Retain the client's DNS/connection progress between requests.
            time.sleep(0.25)


def endpoint(host, port):
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def check_topic_metadata(cluster, topic):
    """Inspect existing metadata only; do not request or create an unknown topic."""
    topic_meta = cluster.topics.get(topic)
    if topic_meta is None:
        raise RuntimeError(
            f"Topic is missing: {topic}. If RUN_ID changed, rerun the notebook's "
            "configuration and topic-creation cells in order. Use the current WIKI_TOPIC value."
        )
    if topic_meta.error:
        raise RuntimeError(f"Topic metadata error for {topic}: {topic_meta.error}")
    if not topic_meta.partitions:
        raise RuntimeError(f"Topic {topic} has no partitions in its metadata.")
    print(f"Topic: {topic} | partitions: {len(topic_meta.partitions)}")
    problems = []
    for number, part in sorted(topic_meta.partitions.items()):
        replicas = list(part.replicas or [])
        synchronized = list(part.isrs or [])
        print(f"  Partition {number}: leader={part.leader}, replicas={replicas}, ISR={synchronized}")
        if part.error:
            problems.append(f"partition {number}: {part.error}")
        if part.leader < 0:
            problems.append(f"partition {number} has no elected leader")
        elif part.leader not in cluster.brokers:
            problems.append(f"partition {number} leader {part.leader} is absent from broker metadata")
        elif part.leader not in synchronized:
            problems.append(f"partition {number} has no synchronized leader (leader is absent from ISR)")
    if problems:
        raise RuntimeError(f"Topic {topic} is not ready: " + "; ".join(problems))
    print("Topic metadata OK: every partition has an advertised, synchronized leader.")


def check(address, timeout, admin_class, topic=None, address_family="v4"):
    print(f"Connection settings: broker.address.family={address_family}; connection setup timeout=5000 ms.")
    print(f"Metadata: at most 2 requests per address, {timeout:g} seconds per request.")
    print(f"Checking bootstrap metadata: {address}", flush=True)
    first = metadata(address, timeout, admin_class, address_family=address_family)
    if not first.brokers:
        raise RuntimeError("Kafka answered without any brokers in its metadata.")
    print(f"Bootstrap metadata OK: {address}")
    for broker in first.brokers.values():
        advertised = endpoint(broker.host, broker.port)
        print(f"Checking advertised broker metadata: {advertised} (broker {broker.id})", flush=True)
        response = metadata(advertised, timeout, admin_class, address_family=address_family)
        if not response.brokers:
            raise RuntimeError(f"Broker {advertised} answered without any broker metadata.")
        if first.cluster_id and response.cluster_id and first.cluster_id != response.cluster_id:
            raise RuntimeError(f"Broker {advertised} belongs to a different Kafka cluster.")
        print(f"Advertised broker metadata OK: {advertised} (broker {broker.id})")
    if topic is not None:
        check_topic_metadata(first, topic)
    print(f"SUCCESS: host-to-Kafka metadata connection works. Topics currently visible: {len(first.topics)}")
    print("An empty topic list is normal before the notebook creates its lesson topics.")
    print("This is a metadata connection check; run the notebook for producer/consumer validation.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--timeout", type=float, default=10,
                        help="Seconds per metadata request; at most 2 requests per address (default: 10)")
    parser.add_argument("--address-family", choices=("any", "v4", "v6"), default="v4",
                        help="Broker IP family; v4 matches this lab's Docker binding (default: v4)")
    parser.add_argument("--topic", help="Inspect an existing topic's partitions, leaders, replicas and ISR")
    args = parser.parse_args(argv)
    if args.timeout < 1 or args.timeout > 60:
        parser.error("--timeout must be between 1 and 60 seconds")
    if args.topic is not None and not args.topic.strip():
        parser.error("--topic must be a non-empty topic name")
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        print("Missing confluent-kafka. Run uv sync --locked in the project directory.", file=sys.stderr)
        return 1
    try:
        check(args.bootstrap_servers, args.timeout, AdminClient, topic=args.topic,
              address_family=args.address_family)
        return 0
    except Exception as exc:
        print(f"ERROR: Kafka metadata connection failed: {exc}", file=sys.stderr)
        print("Open Docker Desktop and run the Docker start helper. Run this check on the same machine", file=sys.stderr)
        print("as Docker, in the project's .venv. If bootstrap works but an advertised broker fails,", file=sys.stderr)
        print("check Kafka advertised.listeners and the published port; a TCP port check alone is insufficient.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nConnection check interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
