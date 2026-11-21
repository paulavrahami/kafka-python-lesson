"""Continuous Wikimedia -> persistent Kafka topic/group -> OpenSearch.

There is no event-count or total-runtime limit. Stop the notebook cell (Ctrl+C)
to close the source, flush the Producer and give the Consumer a bounded drain
period. Failed indexing never advances the Kafka offset. Replayed records use
the same SHA-256 document ID, so retries replace a document instead of creating
duplicates. This provides replay-safe indexing, not a cross-system transaction.

Memory is bounded by client queues, one current event, counters, eight recent
errors and per-partition offsets. There are no accumulated event lists or sets.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
import re
import threading
import time
from urllib.parse import urlparse
import uuid


INDEX_PROPERTIES = {
    "event_id": {"type": "keyword"},
    "event_time": {"type": "date"},
    "server_name": {"type": "keyword"},
    "bot": {"type": "boolean"},
    "type": {"type": "keyword"},
    "title": {"type": "text"},
    "wiki": {"type": "keyword"},
    "namespace": {"type": "integer"},
    "kafka_topic": {"type": "keyword"},
    "kafka_partition": {"type": "integer"},
    "kafka_offset": {"type": "long"},
    "indexed_at": {"type": "date"},
}


class OpenSearchPipelineError(RuntimeError):
    """A failed pipeline with a bounded partial ``report`` and a ``stage``."""

    def __init__(self, stage, report):
        self.stage, self.report = stage, report
        super().__init__({"stage": stage, **report})


def _positive_seconds(name, value):
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or value <= 0):
        raise ValueError(f"{name} must be finite and positive")


def _names(topic, index):
    if not isinstance(topic, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic) or topic in {".", ".."}:
        raise ValueError("Use one concrete Kafka topic name")
    if (not isinstance(index, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,199}", index)
            or index in {".", ".."}):
        raise ValueError("Use one concrete lowercase OpenSearch index name")


def _kafka_config(bootstrap_servers, kafka_config=None):
    """Return connection settings; role guarantees are added by each client.

    A supplied cloud mapping is authoritative, including its bootstrap address.
    Only the shared loader's connection/security allowlist is accepted, so it
    cannot replace commits, idempotence, callbacks or queue bounds.
    """
    if kafka_config is None:
        return {"bootstrap.servers": bootstrap_servers, "broker.address.family": "v4",
                "socket.timeout.ms": 10000, "socket.connection.setup.timeout.ms": 5000,
                "allow.auto.create.topics": False}
    from scripts.confluent_cloud import validate_kafka_connection_config
    connection = validate_kafka_connection_config(kafka_config)
    return {**connection, "broker.address.family": "any",
            "socket.timeout.ms": 30000, "socket.connection.setup.timeout.ms": 30000,
            "api.version.request.timeout.ms": 30000, "allow.auto.create.topics": False,
            # Native stderr messages bypass Python redaction. Structured errors
            # still reach metadata exceptions, consumer.poll and error_cb.
            "log_level": 0}


def _kafka_error_text(error, connection):
    if "sasl.password" in connection:
        from scripts.confluent_cloud import redact_kafka_error
        message = redact_kafka_error(error, connection)
    else:
        message = str(error)
    normalized = message.lower().replace("_", " ")
    if "cluster authorization failed" in normalized:
        message += (" Check the cluster policy for idempotent producers (InitProducerId); "
                    "where required, grant IDEMPOTENT_WRITE.")
    return message


def _topic_options(partitions, create_topic, replication_factor):
    for name, value in (("partitions", partitions), ("replication_factor", replication_factor)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if not isinstance(create_topic, bool):
        raise ValueError("create_topic must be a boolean")


def create_opensearch_client(opensearch_url="http://127.0.0.1:9200", *, request_timeout=5):
    """Create a client; HTTP is allowed only for the local Docker lab.

    HTTPS verifies certificates and hostnames. Optional credentials come from
    OPENSEARCH_USERNAME / OPENSEARCH_PASSWORD; optional private CA file from
    OPENSEARCH_CA_CERT. Credentials in the URL are rejected.
    """
    from opensearchpy import OpenSearch
    _positive_seconds("request_timeout", request_timeout)
    parsed = urlparse(opensearch_url)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in {"", "/"}):
        raise ValueError("OpenSearch URL must be an HTTP(S) cluster root without credentials or query parameters")
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Use HTTPS for a non-local OpenSearch server")
    user, password = os.environ.get("OPENSEARCH_USERNAME"), os.environ.get("OPENSEARCH_PASSWORD")
    if bool(user) != bool(password):
        raise ValueError("Set both OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD, or neither")
    options = {"hosts": [opensearch_url], "verify_certs": True,
               "timeout": request_timeout, "max_retries": 0, "retry_on_timeout": False,
               "http_compress": True}
    if user:
        options["http_auth"] = (user, password)
    if os.environ.get("OPENSEARCH_CA_CERT"):
        options["ca_certs"] = os.environ["OPENSEARCH_CA_CERT"]
    return OpenSearch(**options)


def _prepare_kafka_topic(topic, bootstrap_servers, partitions, ready_timeout, *,
                         kafka_config=None, create_topic=True, replication_factor=1):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    _topic_options(partitions, create_topic, replication_factor)
    _positive_seconds("ready_timeout", ready_timeout)
    connection = _kafka_config(bootstrap_servers, kafka_config)
    try:
        admin = AdminClient(connection)
    except Exception as error:
        raise RuntimeError("Kafka AdminClient could not start: " + _kafka_error_text(error, connection)) from None
    created = creation_requested = False
    request_limit = 30 if kafka_config is not None else 10
    deadline = time.monotonic() + ready_timeout
    while time.monotonic() < deadline:
        try:
            # Request this topic only. With auto creation disabled, this works
            # with topic-scoped ACLs and never needs a cluster-wide topic list.
            metadata = admin.list_topics(topic=topic, timeout=max(0.01, min(request_limit, deadline - time.monotonic())))
        except Exception as error:
            raise RuntimeError("Kafka topic metadata request failed: " + _kafka_error_text(error, connection)
                               + ". Check connectivity, Kafka API credentials and DESCRIBE access to this topic.") from None
        info = metadata.topics.get(topic)
        if info is not None and info.error is not None:
            problem = info.error
            code = problem.code()
            if code == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                raise PermissionError("Kafka topic metadata access was denied; grant DESCRIBE on the requested topic. "
                                      "The check does not need access to other topics or CREATE when create_topic=False.") from None
            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                info = None
            elif code != KafkaError.LEADER_NOT_AVAILABLE:
                raise RuntimeError("Kafka topic metadata error: " + _kafka_error_text(problem, connection)) from None
        if info is None:
            if not create_topic:
                raise RuntimeError("The requested Kafka topic is missing or not visible. Create it in the cluster "
                                   "with the expected partitions and grant topic access, then retry. No topic was created.") from None
            if not creation_requested:
                try:
                    remaining = max(0.01, deadline - time.monotonic())
                    future = admin.create_topics(
                        [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)],
                        operation_timeout=min(request_limit, remaining), request_timeout=min(request_limit, remaining),
                    )[topic]
                    future.result(timeout=remaining)
                    created = True
                except KafkaException as error:
                    if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise RuntimeError("Kafka topic creation failed: " + _kafka_error_text(error, connection)) from None
                except Exception as error:
                    raise RuntimeError("Kafka topic creation failed: " + _kafka_error_text(error, connection)) from None
                creation_requested = True
                continue
        elif info.error is None and info.partitions:
            if len(info.partitions) != partitions:
                raise RuntimeError("The existing topic has a different partition count; it was not modified")
            if all(part.error is None and part.leader in metadata.brokers and part.leader in part.isrs
                   for part in info.partitions.values()):
                return {"topic": topic, "partitions": partitions, "topic_created": created}
        if kafka_config is None:
            request_limit = 5
        time.sleep(max(0, min(0.25, deadline - time.monotonic())))
    raise TimeoutError("Kafka topic did not acquire ready partition leaders")


def prepare_pipeline(*, topic, bootstrap_servers="127.0.0.1:9092",
                     opensearch_url="http://127.0.0.1:9200", index="wikimedia-recentchanges",
                     request_timeout=5, ready_timeout=30, partitions=3,
                     kafka_config=None, create_topic=True, replication_factor=1):
    """Check services and create only a missing lesson topic/index.

    Existing resources and offsets are retained. A new local index uses one
    primary shard, no replicas and an explicit strict mapping. Kafka uses one
    replica copy by default for the single-broker lab. With create_topic=False,
    Kafka metadata is read only for the named, pre-created topic. For Cloud,
    supply a validated connection mapping and allow ready_timeout=60 for TLS.
    This helper does not delete data or print connection credentials.
    """
    _names(topic, index)
    _positive_seconds("request_timeout", request_timeout)
    _positive_seconds("ready_timeout", ready_timeout)
    _topic_options(partitions, create_topic, replication_factor)
    connection = _kafka_config(bootstrap_servers, kafka_config)
    try:
        client = create_opensearch_client(opensearch_url, request_timeout=request_timeout)
        try:
            version = client.info(request_timeout=request_timeout).get("version", {}).get("number", "unknown")
            created = False
            if not client.indices.exists(index=index, request_timeout=request_timeout):
                try:
                    result = client.indices.create(index=index, body={
                        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                        "mappings": {"dynamic": "strict", "properties": INDEX_PROPERTIES},
                    }, request_timeout=request_timeout)
                    if not result.get("acknowledged"):
                        raise RuntimeError("OpenSearch did not acknowledge index creation")
                    created = True
                except Exception as exc:
                    if getattr(exc, "error", None) != "resource_already_exists_exception":
                        raise
            mapping = client.indices.get_mapping(index=index, request_timeout=request_timeout)
            properties = mapping.get(index, {}).get("mappings", {}).get("properties", {})
            incompatible = [name for name, spec in INDEX_PROPERTIES.items()
                            if properties.get(name, {}).get("type") != spec["type"]]
            if incompatible:
                raise RuntimeError("Existing OpenSearch mapping is incompatible for fields: " + ", ".join(incompatible))
        finally:
            client.close()
        result = _prepare_kafka_topic(topic, connection["bootstrap.servers"], partitions, ready_timeout,
                                      kafka_config=kafka_config, create_topic=create_topic,
                                      replication_factor=replication_factor)
        return {**result, "index": index, "index_created": created, "opensearch_version": version,
                "indexed_fields": list(INDEX_PROPERTIES)}
    except Exception as error:
        if kafka_config is not None:
            raise RuntimeError(_kafka_error_text(error, connection)) from None
        raise


def _event_fields(event):
    if not isinstance(event, dict) or not isinstance(event.get("meta"), dict):
        raise ValueError("Expected a JSON event with meta")
    identity, site, stamp = event["meta"].get("id"), event.get("server_name"), event["meta"].get("dt")
    if not isinstance(identity, str) or not identity or not isinstance(site, str) or not site:
        raise ValueError("Expected nonempty meta.id and server_name")
    if not isinstance(event.get("bot"), bool) or not isinstance(stamp, str):
        raise ValueError("Expected boolean bot and timestamp meta.dt")
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("meta.dt must contain a timezone")
    return identity, site, int(parsed.timestamp() * 1000)


def project_document(event, *, topic, partition, offset):
    """Return the stable document ID and a controlled twelve-field projection."""
    identity, site, _ = _event_fields(event)
    document = {"event_id": identity, "event_time": event["meta"]["dt"],
                "server_name": site, "bot": event["bot"],
                "kafka_topic": topic, "kafka_partition": partition, "kafka_offset": offset,
                "indexed_at": datetime.now(timezone.utc).isoformat()}
    for name in ("type", "title", "wiki"):
        if isinstance(event.get(name), str):
            document[name] = event[name]
    namespace = event.get("namespace")
    if isinstance(namespace, int) and not isinstance(namespace, bool) and -(2 ** 31) <= namespace < 2 ** 31:
        document["namespace"] = namespace
    return hashlib.sha256(identity.encode("utf-8")).hexdigest(), document


def _transient_index_error(error):
    from opensearchpy.exceptions import ConnectionError as SearchConnectionError, SSLError
    if isinstance(error, SSLError):
        return False
    return isinstance(error, SearchConnectionError) or getattr(error, "status_code", None) in {408, 429, 502, 503, 504}


def run_opensearch_pipeline(
    events, *, topic, bootstrap_servers="127.0.0.1:9092",
    opensearch_url="http://127.0.0.1:9200", index="wikimedia-recentchanges",
    group_id="python-wikimedia-opensearch", stop_event=None, on_progress=None,
    progress_every=25, drain_seconds=15, request_timeout=5, max_index_retries=2,
    max_pending=1000, queue_wait_seconds=5, delivery_timeout_seconds=30,
    preflight=True, kafka_config=None, create_topic=True, replication_factor=1,
):
    """Run until source exhaustion, shared stop_event or notebook interruption.

    No count or elapsed-time cap is applied. Consumer.subscribe resumes the
    persistent group's committed offsets; earliest applies only without a
    usable saved offset. Existing backlog is indexed before newer records in
    each partition. OpenSearch acknowledgement precedes synchronous commit.

    Ctrl+C returns a stopped report after cleanup. Errors raise
    OpenSearchPipelineError with a partial report. on_progress may execute in
    the worker thread and must be quick/thread-safe. Native calls use finite
    timeouts, so interruption and shutdown are cooperative, not instantaneous.

    Optional kafka_config supplies connection/security fields for Cloud and
    takes precedence over bootstrap_servers. Consumer/Producer guarantees and
    bounded queues are retained. Set create_topic=False for pre-created Cloud
    topics; replication_factor is used only when creating a missing topic.
    """
    _names(topic, index)
    for name, value in (("drain_seconds", drain_seconds), ("request_timeout", request_timeout),
                        ("queue_wait_seconds", queue_wait_seconds), ("delivery_timeout_seconds", delivery_timeout_seconds)):
        _positive_seconds(name, value)
    for name, value in (("max_pending", max_pending), ("progress_every", progress_every)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if isinstance(max_index_retries, bool) or not isinstance(max_index_retries, int) or max_index_retries < 0:
        raise ValueError("max_index_retries must be a nonnegative integer")
    if not isinstance(group_id, str) or not group_id:
        raise ValueError("Use a persistent, nonempty group_id")
    _topic_options(3, create_topic, replication_factor)
    connection = _kafka_config(bootstrap_servers, kafka_config)
    cloud = kafka_config is not None
    worker_timeout = request_timeout + 12 + (30 if cloud else 0)
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

    source_stop = stop_event if stop_event is not None else threading.Event()
    if source_stop.is_set():
        raise ValueError("Use a fresh, unset stop_event")
    consumer_stop, source_done, ready, worker_done = (threading.Event() for _ in range(4))
    lock, started = threading.RLock(), time.monotonic()
    run_id = uuid.uuid4().hex
    marker = run_id.encode("ascii")
    state = {"source_received": 0, "accepted": 0, "confirmed": 0, "delivery_failed": 0,
             "consumed": 0, "indexed": 0, "committed": 0,
             "consumed_current_run": 0, "committed_current_run": 0, "consumed_other_runs": 0,
             "index_retries": 0, "queue_retries": 0, "remaining": 0, "source_active": True,
             "processed_while_source_open": 0, "first_indexed_seconds": None,
             "delivery_errors": [], "worker_errors": [], "broker_errors": []}
    confirmed_offsets, committed_offsets = {}, {}
    producer = worker = iterator = None
    error, stage, stop_reason = None, "preflight", "source_exhausted"

    def snapshot():
        with lock:
            result = {name: list(value) if isinstance(value, list) else value for name, value in state.items()}
            result.update(topic=topic, index=index, group_id=group_id, run_id=run_id,
                          elapsed_seconds=round(time.monotonic() - started, 3),
                          confirmed_offsets=dict(confirmed_offsets), committed_offsets=dict(committed_offsets))
            result["in_flight"] = max(0, state["accepted"] - state["confirmed"] - state["delivery_failed"])
        return result

    def notify():
        if on_progress is not None:
            on_progress(snapshot())

    def broker_error(problem):
        with lock:
            state["broker_errors"].append(_kafka_error_text(problem, connection)[:500])
            del state["broker_errors"][:-8]

    def delivered(problem, message):
        with lock:
            if problem is None:
                state["confirmed"] += 1
                part = message.partition()
                confirmed_offsets[part] = max(confirmed_offsets.get(part, -1), message.offset())
            else:
                state["delivery_failed"] += 1
                state["delivery_errors"].append(_kafka_error_text(problem, connection)[:500])
                del state["delivery_errors"][:-8]
                source_stop.set()

    def index_record(client, message, event):
        document_id, document = project_document(event, topic=message.topic(),
                                                  partition=message.partition(), offset=message.offset())
        for attempt in range(max_index_retries + 1):
            try:
                response = client.index(index=index, id=document_id, body=document, request_timeout=request_timeout)
            except Exception as problem:
                if attempt >= max_index_retries or not _transient_index_error(problem):
                    raise
                if consumer_stop.wait(min(1.0, 0.1 * (2 ** min(attempt, 5)))):
                    raise RuntimeError("Stopped during an indexing retry; this Kafka offset was not committed") from problem
                with lock:
                    state["index_retries"] += 1
                continue
            if (response.get("result") not in {"created", "updated"}
                    or response.get("_shards", {}).get("failed", 0)):
                raise RuntimeError("OpenSearch did not acknowledge successful indexing; offset was not committed")
            return

    def consume():
        consumer = client = None
        last_report = time.monotonic()
        try:
            client = create_opensearch_client(opensearch_url, request_timeout=request_timeout)
            consumer = Consumer({**connection, "group.id": group_id,
                                 "enable.auto.commit": False, "enable.auto.offset.store": False,
                                 "auto.offset.reset": "earliest", "queued.max.messages.kbytes": 1024})
            consumer.subscribe([topic])
            ready.set()
            while not consumer_stop.is_set():
                with lock:
                    drained = source_done.is_set() and all(
                        committed_offsets.get(part, -1) >= offset for part, offset in confirmed_offsets.items())
                if drained:
                    break
                message = consumer.poll(0.2)
                if message is None:
                    if time.monotonic() - last_report >= 5:
                        notify()
                        last_report = time.monotonic()
                    continue
                if message.error() is not None:
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                current = dict(message.headers() or []).get("lesson_pipeline_run_id") == marker
                with lock:
                    state["consumed"] += 1
                    state["consumed_current_run" if current else "consumed_other_runs"] += 1
                event = json.loads(message.value())
                index_record(client, message, event)
                with lock:
                    state["indexed"] += 1
                    if state["first_indexed_seconds"] is None:
                        state["first_indexed_seconds"] = round(time.monotonic() - started, 3)
                acknowledgements = consumer.commit(message=message, asynchronous=False)
                if not acknowledgements or any(part.error is not None for part in acknowledgements):
                    raise RuntimeError("Kafka commit failed after indexing; replay will replace the same document ID")
                with lock:
                    state["committed"] += 1
                    if current:
                        state["committed_current_run"] += 1
                    committed_offsets[message.partition()] = message.offset()
                    if state["source_active"]:
                        state["processed_while_source_open"] += 1
                    report_now = state["committed"] % progress_every == 0
                if report_now or time.monotonic() - last_report >= 5:
                    notify()
                    last_report = time.monotonic()
        except BaseException as problem:
            with lock:
                state["worker_errors"].append(f"{type(problem).__name__}: {_kafka_error_text(problem, connection)[:500]}")
                del state["worker_errors"][:-8]
            source_stop.set()
        finally:
            for name, resource in (("consumer", consumer), ("opensearch", client)):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as problem:
                        with lock:
                            state["worker_errors"].append(f"{name}_close: {type(problem).__name__}")
                            del state["worker_errors"][:-8]
                        source_stop.set()
            ready.set()
            worker_done.set()

    try:
        if preflight:
            prepare_pipeline(topic=topic, bootstrap_servers=connection["bootstrap.servers"], opensearch_url=opensearch_url,
                             index=index, request_timeout=request_timeout, ready_timeout=60 if cloud else 30,
                             kafka_config=kafka_config, create_topic=create_topic, replication_factor=replication_factor)
        stage = "producer_startup"
        producer = Producer({**connection, "enable.idempotence": True, "acks": "all",
                             "delivery.timeout.ms": int(delivery_timeout_seconds * 1000),
                             "queue.buffering.max.messages": max_pending, "queue.buffering.max.kbytes": 1024,
                             "error_cb": broker_error})
        stage = "consumer_startup"
        worker = threading.Thread(target=consume, name=f"wikimedia-opensearch-{run_id[:8]}", daemon=True)
        worker.start()
        if not ready.wait(worker_timeout) or state["worker_errors"]:
            raise RuntimeError("Consumer/OpenSearch worker could not start")
        iterator = iter(events)
        stage = "source"
        for event in iterator:
            if state["worker_errors"] or state["delivery_errors"]:
                stage = "consumer" if state["worker_errors"] else "delivery"
                raise RuntimeError("A pipeline worker failed; inspect the partial report")
            if source_stop.is_set():
                stop_reason = "stop_requested"
                break
            with lock:
                state["source_received"] += 1
            _, site, millis = _event_fields(event)
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            queue_deadline = time.monotonic() + queue_wait_seconds
            while True:
                try:
                    producer.produce(topic=topic, key=site.encode("utf-8"), value=payload, timestamp=millis,
                                     headers=[("lesson_pipeline_run_id", marker)], on_delivery=delivered)
                    with lock:
                        state["accepted"] += 1
                    break
                except BufferError:
                    if time.monotonic() >= queue_deadline:
                        stage = "producer_backpressure"
                        raise RuntimeError("Producer queue stayed full; event was not silently dropped")
                    with lock:
                        state["queue_retries"] += 1
                    producer.poll(min(0.1, max(0, queue_deadline - time.monotonic())))
                    if source_stop.is_set():
                        stage = "delivery" if state["delivery_errors"] else "consumer"
                        raise RuntimeError("Pipeline stopped while waiting for producer capacity")
            producer.poll(0)
        if source_stop.is_set() and not state["worker_errors"] and not state["delivery_errors"]:
            stop_reason = "stop_requested"
    except KeyboardInterrupt:
        stop_reason = "manual_stop"
        source_stop.set()
    except Exception as problem:
        error = problem
        source_stop.set()
    finally:
        with lock:
            state["source_active"] = False
        source = iterator if iterator is not None else events
        if hasattr(source, "close"):
            try:
                source.close()
            except Exception as problem:
                if error is None:
                    error, stage = problem, "source_close"
        if producer is not None:
            try:
                state["remaining"] = producer.flush(delivery_timeout_seconds + 5)
            except Exception as problem:
                if error is None:
                    error, stage = problem, "producer_flush"
        source_done.set()
        if worker is not None:
            if error is not None or state["worker_errors"] or state["delivery_errors"]:
                consumer_stop.set()
            elif not worker_done.wait(drain_seconds):
                consumer_stop.set()
                # Outstanding Kafka records remain available for the same group.
                stop_reason += "_drain_timeout"
            consumer_stop.set()
            worker.join(timeout=max(12, worker_timeout))
            if worker.is_alive():
                error, stage = RuntimeError("Consumer worker has not closed within the shutdown limit"), "consumer_shutdown"

    result = snapshot()
    result.update(stop_reason=stop_reason, status="stopped" if stop_reason.startswith(("manual_stop", "stop_requested")) else "source_exhausted",
                  worker_closed=worker is None or not worker.is_alive())
    result["producer_drained"] = (result["remaining"] == 0 and result["confirmed"] == result["accepted"]
                                  and result["delivery_failed"] == 0)
    result["current_run_committed"] = result["committed_current_run"] == result["accepted"]
    result["backlog_fully_drained"] = None  # A counter from this run cannot establish total cluster lag.
    if error is not None or result["worker_errors"] or not result["producer_drained"]:
        if error is None:
            stage = "consumer" if result["worker_errors"] else "delivery"
        result["status"] = "failed"
        if error is not None:
            result["error"] = f"{type(error).__name__}: {_kafka_error_text(error, connection)[:500]}"
        if cloud:
            # The original exception can contain SASL values even after its
            # message is masked; do not expose it as a traceback cause.
            raise OpenSearchPipelineError(stage, result) from None
        raise OpenSearchPipelineError(stage, result) from error
    if on_progress is not None:
        on_progress(result)
    return result
