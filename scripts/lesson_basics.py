"""Helpers for Notebook 01: exactly ten fixed synthetic events.

The Kafka methods are adapted from the previously tested combined notebook.
They perform real network operations only when the notebook calls them explicitly.
"""
from collections import Counter
import copy
from datetime import datetime, timedelta, timezone
from itertools import islice
import json
import time


def make_fixture():
    """Return a new copy of the same ten teaching events on every call."""
    base_time = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
    spec = [
        (2, "en.wikipedia.org", True), (4, "en.wikipedia.org", False),
        (8, "he.wikipedia.org", True), (10, "en.wikipedia.org", True),
        (12, "he.wikipedia.org", False), (20, "en.wikipedia.org", True),
        (25, "de.wikipedia.org", False), (45, "he.wikipedia.org", True),
        (61, "en.wikipedia.org", True), (65, "he.wikipedia.org", False),
    ]
    return [
        {"meta": {"id": f"synthetic-{i:03d}", "dt": (base_time + timedelta(seconds=seconds)).isoformat()},
         "server_name": website, "bot": bot, "type": "edit",
         "title": f"Synthetic page {i}", "synthetic": True}
        for i, (seconds, website, bot) in enumerate(spec, 1)
    ]


def event_millis(event):
    stamp = event["meta"]["dt"]
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Event timestamp must include a timezone")
    return int(parsed.timestamp() * 1000)


def encode_event(event):
    return (event["server_name"].encode("utf-8"),
            json.dumps(event, ensure_ascii=False).encode("utf-8"), event_millis(event))


def decode_event(payload):
    event = json.loads(payload)
    if not isinstance(event, dict) or not isinstance(event.get("meta"), dict):
        raise ValueError("Expected a JSON object with meta")
    if not isinstance(event["meta"].get("id"), str) or not event["meta"]["id"]:
        raise ValueError("Missing nonempty string meta.id")
    if not isinstance(event.get("server_name"), str) or not event["server_name"]:
        raise ValueError("Missing server_name")
    if not isinstance(event.get("bot"), bool):
        raise ValueError("Expected a boolean bot field")
    event_millis(event)
    return event


def starting_position(committed, reset, first_retained=0, end_offset=10):
    """A local one-partition teaching model; it does not change Kafka offsets."""
    if committed is not None and first_retained <= committed <= end_offset:
        return committed
    if reset == "earliest":
        return first_retained
    if reset == "latest":
        return end_offset
    raise ValueError("Unsupported reset policy in this teaching model")


def window_counts(events, width_seconds, key_fn):
    if width_seconds <= 0:
        raise ValueError("Window size must be positive")
    width_ms = width_seconds * 1000
    counts = Counter((key_fn(event), (event_millis(event) // width_ms) * width_ms)
                     for event in events)
    def utc_text(milliseconds):
        return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()
    return [{"key": key, "start_ms": start, "start": utc_text(start),
             "end": utc_text(start + width_ms), "count": count}
            for (key, start), count in sorted(counts.items())]


def idempotent_recovery_demo(events, crash_after=4):
    """Model a local sink write followed by a checkpoint; replay one existing event."""
    events = list(events)
    if not 1 <= crash_after <= len(events):
        raise ValueError("crash_after must identify an event in the input")
    documents, attempts = {}, Counter()
    next_offset = 0
    class TeachingCrash(RuntimeError):
        pass
    def process(fail_after_write_at=None):
        nonlocal next_offset
        for offset in range(next_offset, len(events)):
            event = events[offset]
            document_id = event["meta"]["id"]
            documents[document_id] = copy.deepcopy(event)
            attempts[document_id] += 1
            if offset == fail_after_write_at:
                raise TeachingCrash("Failure after write, before checkpoint")
            next_offset = offset + 1
    try:
        process(fail_after_write_at=crash_after - 1)
    except TeachingCrash:
        pass
    before = {"documents": len(documents), "next_offset": next_offset,
              "write_attempts": sum(attempts.values())}
    process()
    return {"before": before,
            "after": {"documents": len(documents), "next_offset": next_offset,
                      "write_attempts": sum(attempts.values())},
            "replayed_event_id": events[crash_after - 1]["meta"]["id"],
            "attempts": dict(attempts), "documents": documents}


class KafkaLesson:
    """The original lab's bounded Kafka helpers, scoped to one bootstrap address.

    Client libraries are imported only by live methods. The class preserves the
    IPv4 policy, advertised-address checks, topic readiness, bounded metadata
    retry, idempotent producer, delivery callbacks, and manual consumer commits.
    """

    def __init__(self, bootstrap_servers='127.0.0.1:9092'):
        if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
            raise ValueError('bootstrap_servers must be a nonempty string')
        self.bootstrap_servers = bootstrap_servers

    def client_config(self, address=None):
        return {'bootstrap.servers': address if address is not None else self.bootstrap_servers, 'broker.address.family': 'v4', 'socket.timeout.ms': 10000, 'socket.connection.setup.timeout.ms': 5000}

    def _read_kafka_metadata(self, client, timeout=10):
        from confluent_kafka import KafkaError, KafkaException
        transient = {KafkaError._TIMED_OUT, KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN}
        for attempt in range(2):
            try:
                return client.list_topics(timeout=timeout)
            except KafkaException as exc:
                error = exc.args[0]
                if attempt == 1 or not (error.retriable() or error.code() in transient):
                    raise
                time.sleep(0.25)

    def _topic_readiness_problems(self, metadata, names, partitions):
        pending = {}
        for name in names:
            topic_meta = metadata.topics.get(name)
            if topic_meta is None:
                pending[name] = 'Topic is not yet visible in metadata'
                continue
            if topic_meta.error is not None:
                pending[name] = str(topic_meta.error)
                continue
            if not topic_meta.partitions:
                pending[name] = 'No partition metadata yet'
                continue
            if len(topic_meta.partitions) != partitions:
                raise RuntimeError({'stage': 'topic_configuration', 'topic': name, 'expected_partitions': partitions, 'actual_partitions': len(topic_meta.partitions), 'hint': 'Inspect the existing topic. Its partition count was not changed.'})
            unavailable = []
            for partition_id, part in topic_meta.partitions.items():
                if part.error is not None or part.leader < 0 or part.leader not in metadata.brokers or (part.leader not in part.isrs):
                    unavailable.append({'partition': partition_id, 'leader': part.leader, 'isr': list(part.isrs), 'error': str(part.error) if part.error else None})
            if unavailable:
                pending[name] = unavailable
        return pending

    def _wait_for_topic_leaders(self, admin, names, partitions=3, timeout=30):
        from confluent_kafka import KafkaException
        deadline = time.monotonic() + timeout
        pending = {name: 'Waiting for metadata' for name in names}
        while time.monotonic() < deadline:
            try:
                metadata = admin.list_topics(timeout=max(0.01, min(5, deadline - time.monotonic())))
                pending = self._topic_readiness_problems(metadata, names, partitions)
                if not pending:
                    return {name: len(metadata.topics[name].partitions) for name in names}
            except KafkaException as exc:
                pending = {'metadata_error': str(exc)}
            time.sleep(max(0, min(0.5, deadline - time.monotonic())))
        raise RuntimeError({'stage': 'topic_readiness', 'bootstrap_servers': self.bootstrap_servers, 'pending': pending, 'hint': 'Check Docker logs and partition leaders. No records were queued for this batch.'})

    def _verify_broker_addresses(self, metadata, timeout=10):
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import AdminClient
        if not metadata.brokers:
            raise RuntimeError('Kafka returned metadata without any brokers.')
        for broker in metadata.brokers.values():
            host = f'[{broker.host}]' if ':' in broker.host else broker.host
            address = f'{host}:{broker.port}'
            try:
                probe = AdminClient({**self.client_config(address), 'allow.auto.create.topics': False})
                response = self._read_kafka_metadata(probe, timeout=timeout)
                if not response.brokers:
                    raise RuntimeError(f'No brokers returned from {address}')
                if metadata.cluster_id and response.cluster_id and (metadata.cluster_id != response.cluster_id):
                    raise RuntimeError(f'Advertised address {address} leads to another Kafka cluster')
            except KafkaException as exc:
                raise RuntimeError({'stage': 'advertised_broker_connection', 'broker': address, 'bootstrap_servers': self.bootstrap_servers, 'address_family': 'v4', 'error': str(exc), 'hint': 'IPv4 metadata connection failed; transient failures get at most one retry. Check Docker logs and scripts/check_kafka.py. No records were queued for this batch.'}) from exc

    def ensure_topics(self, names, partitions=3, ready_timeout=30):
        from concurrent.futures import TimeoutError as FutureTimeoutError
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
        names = list(dict.fromkeys(names))
        if not names:
            return {}
        admin = AdminClient({**self.client_config(), 'allow.auto.create.topics': False})
        try:
            initial = self._read_kafka_metadata(admin, timeout=10)
        except KafkaException as exc:
            raise RuntimeError({'stage': 'bootstrap_connection', 'bootstrap_servers': self.bootstrap_servers, 'address_family': 'v4', 'error': str(exc), 'hint': "Open Docker Desktop, run the Docker start/check helpers, and verify this Kernel's address."}) from exc
        self._verify_broker_addresses(initial)
        missing = [name for name in names if name not in initial.topics]
        if missing:
            futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1) for name in missing], operation_timeout=10, request_timeout=15)
            for name, future in futures.items():
                try:
                    future.result(timeout=16)
                except FutureTimeoutError as exc:
                    raise RuntimeError({'stage': 'topic_creation', 'topic': name, 'bootstrap_servers': self.bootstrap_servers, 'error': 'Topic-creation response timed out', 'hint': 'Check metadata and Docker logs before retrying; creation may still complete.'}) from exc
                except KafkaException as exc:
                    if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise RuntimeError({'stage': 'topic_creation', 'topic': name, 'bootstrap_servers': self.bootstrap_servers, 'error': str(exc)}) from exc
        return self._wait_for_topic_leaders(admin, names, partitions, ready_timeout)

    def publish_events(self, rows, topic, key_fn=lambda event: event['server_name'], timestamp_fn=None, limit=10):
        """Publish a finite batch after readiness; confirm every accepted delivery."""
        from confluent_kafka import Producer
        self.ensure_topics([topic])
        errors, confirmed, broker_errors = ([], [], [])

        def broker_error(err):
            broker_errors.append(str(err))
            del broker_errors[:-8]

        def delivered(err, msg):
            if err is not None:
                errors.append(str(err))
            else:
                confirmed.append({'key': msg.key().decode('utf-8'), 'partition': msg.partition(), 'offset': msg.offset()})
        delivery_timeout_seconds = 30
        producer = Producer({**self.client_config(), 'enable.idempotence': True, 'acks': 'all', 'allow.auto.create.topics': False, 'delivery.timeout.ms': delivery_timeout_seconds * 1000, 'error_cb': broker_error})
        accepted = 0
        try:
            for row in islice(rows, limit):
                args = {'topic': topic, 'key': key_fn(row).encode('utf-8'), 'value': json.dumps(row, ensure_ascii=False).encode('utf-8'), 'on_delivery': delivered}
                if timestamp_fn is not None:
                    args['timestamp'] = timestamp_fn(row)
                producer.produce(**args)
                accepted += 1
                producer.poll(0)
        finally:
            remaining = producer.flush(delivery_timeout_seconds + 5)
        if remaining or errors or len(confirmed) != accepted:
            raise RuntimeError({'stage': 'delivery', 'topic': topic, 'bootstrap_servers': self.bootstrap_servers, 'accepted': accepted, 'confirmed': len(confirmed), 'remaining': remaining, 'delivery_errors': errors, 'broker_errors': broker_errors, 'hint': 'Inspect scripts/check_kafka.py --topic <topic> and Docker logs. A timeout may include records already written; check before republishing.'})
        return {'accepted': accepted, 'confirmed': len(confirmed), 'records': confirmed}

    def consume_events(self, topic, group_id, handler, max_messages=10, deadline_seconds=12, require_exact=True):
        """Process before committing; a partial classroom read fails explicitly by default."""
        from confluent_kafka import Consumer, KafkaException
        consumer = Consumer({**self.client_config(), 'group.id': group_id, 'auto.offset.reset': 'earliest', 'enable.auto.commit': False, 'enable.auto.offset.store': False})
        processed = []
        deadline = time.monotonic() + deadline_seconds
        try:
            consumer.subscribe([topic])
            while len(processed) < max_messages and time.monotonic() < deadline:
                msg = consumer.poll(min(0.5, max(0, deadline - time.monotonic())))
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                event = decode_event(msg.value())
                handler(event)
                commits = consumer.commit(message=msg, asynchronous=False)
                if not commits or any((partition.error is not None for partition in commits)):
                    raise RuntimeError(f'Commit failed: {commits}')
                processed.append({'event_id': event['meta']['id'], 'partition': msg.partition(), 'offset': msg.offset()})
        finally:
            consumer.close()
        if require_exact and len(processed) != max_messages:
            raise RuntimeError({'stage': 'consume_incomplete', 'topic': topic, 'group_id': group_id, 'expected': max_messages, 'processed_and_committed': len(processed), 'bootstrap_servers': self.bootstrap_servers, 'hint': 'Read ended before all expected records arrived. Inspect Kafka and the group offsets; do not report a successful ten-event run.'})
        return processed
