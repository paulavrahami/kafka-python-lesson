"""Read the Cloud lesson's Kafka connection settings without exposing credentials.

This is a deliberately small ``key=value`` format, not a general Java properties
parser. Blank lines and whole-line ``#``/``!`` comments are accepted. Values are
literal after surrounding whitespace is removed: no quoting, escape expansion,
environment interpolation, includes, or executable expressions are evaluated.

Keep the real properties file outside version control. Display only
``safe_connection_summary(config)`` in a notebook, never the configuration dict.
This module uses only Python's standard library and does not connect to Kafka.

Client properties:
https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
https://docs.confluent.io/cloud/current/client-apps/config-client.html
"""

from collections.abc import Mapping
import ipaddress
import json
from pathlib import Path
import re
from urllib.parse import quote, quote_plus


MAX_CONFIG_BYTES = 64 * 1024
_REQUIRED = frozenset({
    "bootstrap.servers", "security.protocol", "sasl.mechanism",
    "sasl.username", "sasl.password",
})
_OPTIONAL = frozenset({"ssl.ca.location", "client.id"})
_TLS = {
    "enable.ssl.certificate.verification": True,
    "ssl.endpoint.identification.algorithm": "https",
}
_ALLOWED = _REQUIRED | _OPTIONAL | _TLS.keys()
_SECRET_FIELDS = ("sasl.username", "sasl.password")
_DNS_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z")


class ConfluentCloudConfigError(ValueError):
    """Invalid local settings; messages never echo supplied names or values."""


def _canonical_key(key):
    if key == "sasl.mechanisms":
        return "sasl.mechanism"
    if key == "sasl.jaas.config":
        raise ConfluentCloudConfigError(
            "Java JAAS settings are not supported. Use separate sasl.username and sasl.password settings."
        )
    if not isinstance(key, str) or key not in _ALLOWED:
        raise ConfluentCloudConfigError(
            "Unsupported Kafka setting. Only this lesson's connection settings are allowed; "
            "consumer, producer, debug, and include settings must stay in the application."
        )
    return key


def _placeholder(value):
    normalized = value.strip().casefold()
    return (
        bool(re.search(r"<[^>]*>|\$\{|\{\{", value))
        or normalized in {
            "placeholder", "changeme", "change_me", "change-me", "replace_me", "replace-me",
            "api_key", "api_secret", "kafka_api_key", "kafka_api_secret", "todo", "...",
            "redacted", "[redacted]",
        }
        or bool(re.match(r"^(?:your|replace(?:_with)?|insert|paste|enter)[_ -]", normalized))
        or bool(re.fullmatch(r"(?:x{3,}|\*{3,}|0{3,})", normalized))
    )


def _literal_string(value, name):
    # ``name`` is always a fixed, whitelisted field name, never user-controlled.
    if not isinstance(value, str) or not value.strip():
        raise ConfluentCloudConfigError(f"{name} must contain a nonempty string.")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ConfluentCloudConfigError(f"{name} must not contain control characters.")
    if _placeholder(value):
        raise ConfluentCloudConfigError(
            f"Replace the placeholder for {name} with the setting from your Kafka cluster."
        )
    return value


def _bootstrap_addresses(value):
    message = (
        "bootstrap.servers must contain DNS host:port addresses separated by commas. "
        "Use the Cloud Kafka endpoint, without a URL scheme, IP address, or localhost."
    )
    result = []
    for entry in value.split(","):
        entry = entry.strip()
        if entry.count(":") != 1:
            raise ConfluentCloudConfigError(message)
        host, port = entry.split(":", 1)
        host = (host[:-1] if host.endswith(".") else host).lower()
        labels = host.split(".")
        if (
            len(labels) < 2 or len(host) > 253
            or any(not _DNS_LABEL.fullmatch(label) for label in labels)
            or any(re.fullmatch(r"(?:pkc-)?x{3,}", label, re.IGNORECASE) for label in labels)
            or "localhost" in labels or host.endswith(".localdomain")
            or all(label.isdigit() for label in labels)
            or not re.fullmatch(r"[0-9]{1,5}", port)
            or not 1 <= int(port) <= 65535
        ):
            raise ConfluentCloudConfigError(message)
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise ConfluentCloudConfigError(message)
        result.append(f"{host}:{int(port)}")
    return ",".join(result)


def validate_kafka_connection_config(config):
    """Return a fresh, normalized Cloud connection dict with mandatory TLS checks.

    Required fields are bootstrap.servers, security.protocol=SASL_SSL,
    sasl.mechanism=PLAIN, sasl.username, and sasl.password. The plural mechanism
    alias is accepted. Optional ssl.ca.location and client.id remain literal.
    The two verification fields may be supplied only with their secure values.
    Validating the returned dict again is supported and does not change it.

    No role-specific settings (group.id, commits, acks, queues, callbacks, debug,
    or timeouts) can enter through this mapping. The runner adds those itself.
    Validation checks local syntax, not DNS reachability, credentials or ACLs.
    """
    if not isinstance(config, Mapping):
        raise ConfluentCloudConfigError("Kafka connection settings must be a mapping.")
    normalized = {}
    for key, value in config.items():
        name = _canonical_key(key)
        if name in normalized:
            raise ConfluentCloudConfigError("Duplicate Kafka connection setting or mechanism alias.")
        normalized[name] = value
    missing = _REQUIRED - normalized.keys()
    if missing:
        raise ConfluentCloudConfigError("Missing required Kafka settings: " + ", ".join(sorted(missing)))
    for name in _REQUIRED | _OPTIONAL:
        if name in normalized:
            normalized[name] = _literal_string(normalized[name], name)
    if normalized["security.protocol"].upper() != "SASL_SSL":
        raise ConfluentCloudConfigError("The Cloud lesson requires security.protocol=SASL_SSL.")
    if normalized["sasl.mechanism"].upper() != "PLAIN":
        raise ConfluentCloudConfigError("The Cloud API-key lesson requires sasl.mechanism=PLAIN.")
    for name in _SECRET_FIELDS:
        if any(char.isspace() for char in normalized[name]):
            raise ConfluentCloudConfigError(f"{name} must not contain whitespace.")
    verification = normalized.get("enable.ssl.certificate.verification", True)
    if not (verification is True or isinstance(verification, str) and verification.casefold() == "true"):
        raise ConfluentCloudConfigError("TLS certificate verification must remain enabled.")
    hostname = normalized.get("ssl.endpoint.identification.algorithm", "https")
    if not isinstance(hostname, str) or hostname.casefold() != "https":
        raise ConfluentCloudConfigError("TLS hostname verification must remain enabled (https).")
    normalized["bootstrap.servers"] = _bootstrap_addresses(normalized["bootstrap.servers"])
    normalized["security.protocol"] = "SASL_SSL"
    normalized["sasl.mechanism"] = "PLAIN"
    normalized.update(_TLS)
    return normalized


def load_confluent_cloud_config(path="confluent-cloud.properties"):
    """Read a small UTF-8 properties file without logging any file contents.

    Split each assignment at its first ``=`` only, preserving ``=`` inside API
    secrets. Duplicates and malformed lines fail rather than override settings.
    A UTF-8 BOM and Windows newlines are accepted. The real file is gitignored.
    """
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(MAX_CONFIG_BYTES + 1)
    except (OSError, TypeError, ValueError):
        raise ConfluentCloudConfigError(
            "Cannot read the Cloud properties file. Create a local copy of confluent-cloud.properties.example."
        ) from None
    if len(data) > MAX_CONFIG_BYTES:
        raise ConfluentCloudConfigError("The Cloud properties file exceeds the supported size limit.")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeError:
        raise ConfluentCloudConfigError("The Cloud properties file must use UTF-8 encoding.") from None
    values = {}
    for number, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            raise ConfluentCloudConfigError(f"Expected one key=value setting on line {number}.")
        name = _canonical_key(key.strip())
        if name in values:
            raise ConfluentCloudConfigError(f"Duplicate Kafka connection setting on line {number}.")
        values[name] = value.strip()
    return validate_kafka_connection_config(values)


def redact_kafka_error(error, config):
    """Return diagnostic text with both SASL credential values masked.

    Exact values and common repr/JSON/URL-escaped forms are removed in one pass,
    longest first. This also works with an unvalidated mapping during failures.
    It does not log, mutate the error, or claim to scrub unrelated secrets.
    """
    try:
        message = str(error)
    except Exception:
        message = "Kafka operation failed; diagnostic text was unavailable."
    secrets = set()
    if isinstance(config, Mapping):
        for name in _SECRET_FIELDS:
            value = config.get(name)
            if isinstance(value, str) and value:
                secrets.update({value, repr(value)[1:-1], json.dumps(value)[1:-1],
                                quote(value, safe=""), quote_plus(value, safe="")})
    secrets.discard("")
    if not secrets:
        return message
    pattern = "|".join(re.escape(value) for value in sorted(secrets, key=len, reverse=True))
    return re.sub(pattern, lambda match: "[REDACTED]", message)


def safe_connection_summary(config):
    """Return display-safe connection facts, excluding the API key and secret."""
    validated = validate_kafka_connection_config(config)
    summary = {
        "bootstrap.servers": redact_kafka_error(validated["bootstrap.servers"], validated),
        "security.protocol": validated["security.protocol"],
        "sasl.mechanism": validated["sasl.mechanism"],
        "certificate_verification": True,
        "hostname_verification": True,
        "custom_ca_configured": "ssl.ca.location" in validated,
    }
    if "client.id" in validated:
        summary["client.id"] = redact_kafka_error(validated["client.id"], validated)
    return summary
