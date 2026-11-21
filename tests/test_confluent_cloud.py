"""Cloud credentials boundary tests. No account, network or Kafka client needed."""

from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.confluent_cloud import (
    ConfluentCloudConfigError,
    MAX_CONFIG_BYTES,
    load_confluent_cloud_config,
    redact_kafka_error,
    safe_connection_summary,
    validate_kafka_connection_config,
)


def config():
    # Deliberately fictitious credentials. These tests never make a connection.
    return {
        "bootstrap.servers": "pkc-unit123.eu-central-1.aws.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "UNITTEST00000001",
        "sasl.password": "not-a-real-secret+with/slashes==",
    }


class PropertiesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "confluent-cloud.properties"

    def write(self, settings=None, *, extra="", encoding="utf-8"):
        values = config() if settings is None else settings
        text = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n" + extra
        self.path.write_text(text, encoding=encoding)
        return self.path

    def test_bom_comments_windows_newlines_and_equals_inside_secret(self):
        data = config()
        data["sasl.password"] = "nonsecret=middle=padding=="
        text = "\ufeff# Local settings\r\n\r\n ! Another comment\r\n"
        text += "\r\n".join(f"  {key} = {value}  " for key, value in data.items())
        self.path.write_bytes(text.encode("utf-8"))
        result = load_confluent_cloud_config(self.path)
        self.assertEqual(result["sasl.password"], data["sasl.password"])
        self.assertTrue(result["enable.ssl.certificate.verification"])
        self.assertEqual(result["ssl.endpoint.identification.algorithm"], "https")

    def test_plural_mechanism_is_normalized(self):
        data = config()
        data["sasl.mechanisms"] = data.pop("sasl.mechanism")
        result = load_confluent_cloud_config(self.write(data))
        self.assertEqual(result["sasl.mechanism"], "PLAIN")
        self.assertNotIn("sasl.mechanisms", result)

    def test_duplicate_keys_and_alias_pairs_cannot_override_settings(self):
        for extra in ("sasl.password=hidden-override", "sasl.mechanisms=PLAIN"):
            with self.subTest(extra=extra):
                with self.assertRaisesRegex(ConfluentCloudConfigError, "Duplicate") as raised:
                    load_confluent_cloud_config(self.write(extra=extra))
                self.assertNotIn("hidden-override", str(raised.exception))

    def test_malformed_line_does_not_echo_its_contents(self):
        data = config()
        with self.assertRaisesRegex(ConfluentCloudConfigError, "key=value") as raised:
            load_confluent_cloud_config(self.write(extra=data["sasl.username"]))
        self.assertNotIn(data["sasl.username"], str(raised.exception))

    def test_unknown_setting_does_not_echo_name_or_value(self):
        hidden_key = "UNEXPECTED_SECRET_IN_KEY"
        hidden_value = "UNEXPECTED_SECRET_IN_VALUE"
        with self.assertRaisesRegex(ConfluentCloudConfigError, "Unsupported") as raised:
            load_confluent_cloud_config(self.write(extra=f"{hidden_key}={hidden_value}"))
        self.assertNotIn(hidden_key, str(raised.exception))
        self.assertNotIn(hidden_value, str(raised.exception))

    def test_no_interpolation_quote_stripping_or_backslash_expansion(self):
        data = config()
        data["client.id"] = '"$HOME"'
        data["ssl.ca.location"] = r"C:\certificates\root.pem"
        result = load_confluent_cloud_config(self.write(data))
        self.assertEqual(result["client.id"], data["client.id"])
        self.assertEqual(result["ssl.ca.location"], data["ssl.ca.location"])

    def test_hash_inside_secret_is_literal_not_an_inline_comment(self):
        data = config()
        data["sasl.password"] = "fictitious#fragment=part=="
        result = load_confluent_cloud_config(self.write(data))
        self.assertEqual(result["sasl.password"], data["sasl.password"])

    def test_include_and_executable_client_options_are_rejected(self):
        for field in ("include", "sasl.kerberos.kinit.cmd", "plugin.library.paths"):
            with self.subTest(field=field):
                with self.assertRaises(ConfluentCloudConfigError):
                    load_confluent_cloud_config(self.write(extra=f"{field}=never-execute-this"))

    def test_missing_file_does_not_echo_path_or_os_exception(self):
        path = Path(self.temp.name) / "SECRET_IN_PATH.properties"
        with self.assertRaisesRegex(ConfluentCloudConfigError, "Cannot read") as raised:
            load_confluent_cloud_config(path)
        self.assertNotIn("SECRET_IN_PATH", str(raised.exception))
        self.assertTrue(raised.exception.__suppress_context__)

    def test_non_utf8_and_oversize_files_fail_without_dumping_bytes(self):
        self.path.write_bytes(b"SECRET\xff")
        with self.assertRaisesRegex(ConfluentCloudConfigError, "UTF-8") as raised:
            load_confluent_cloud_config(self.path)
        self.assertNotIn("SECRET", str(raised.exception))
        self.path.write_bytes(b"#" * (MAX_CONFIG_BYTES + 1))
        with self.assertRaisesRegex(ConfluentCloudConfigError, "size limit"):
            load_confluent_cloud_config(self.path)

    def test_public_example_fails_until_placeholders_are_replaced(self):
        example = ROOT / "confluent-cloud.properties.example"
        with self.assertRaisesRegex(ConfluentCloudConfigError, "placeholder"):
            load_confluent_cloud_config(example)

    def test_loader_does_not_print_successful_or_failed_credentials(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            load_confluent_cloud_config(self.write())
            with self.assertRaises(ConfluentCloudConfigError):
                load_confluent_cloud_config(self.write(extra="debug=security"))
        self.assertEqual((out.getvalue(), err.getvalue()), ("", ""))


class ValidationTests(unittest.TestCase):
    def test_validation_is_idempotent_and_does_not_mutate_input(self):
        original = config()
        snapshot = deepcopy(original)
        validated = validate_kafka_connection_config(original)
        self.assertEqual(original, snapshot)
        self.assertEqual(validate_kafka_connection_config(validated), validated)
        self.assertIsNot(validated, original)

    def test_multiple_dns_addresses_are_normalized(self):
        data = config()
        data["bootstrap.servers"] = " PKC-ONE.aws.confluent.cloud.:9092,pkc-two.gcp.confluent.cloud:9093 "
        result = validate_kafka_connection_config(data)
        self.assertEqual(result["bootstrap.servers"],
                         "pkc-one.aws.confluent.cloud:9092,pkc-two.gcp.confluent.cloud:9093")

    def test_only_dns_host_port_addresses_are_accepted(self):
        invalid = [
            "localhost:9092", "sub.localhost:9092", "localhost.localdomain:9092",
            "127.0.0.1:9092", "10.0.0.1:9092", "0177.0.0.1:9092", "[::1]:9092",
            "https://pkc-one.aws.confluent.cloud:9092", "broker:9092",
            "pkc-one.aws.confluent.cloud", "pkc-one.aws.confluent.cloud:0",
            "pkc-one.aws.confluent.cloud:65536", "pkc-one.aws.confluent.cloud:port",
            "name@pkc-one.aws.confluent.cloud:9092", "pkc-one.aws.confluent.cloud:9092/path",
            "pkc-one.aws.confluent.cloud..:9092", "under_score.confluent.cloud:9092",
            "pkc-one.aws.confluent.cloud:9092,", "pkc-xxxxx.aws.confluent.cloud:9092",
        ]
        for address in invalid:
            with self.subTest(address=address):
                with self.assertRaises(ConfluentCloudConfigError):
                    validate_kafka_connection_config({**config(), "bootstrap.servers": address})

    def test_missing_fields_name_only_the_required_setting(self):
        for name in config():
            data = config()
            data.pop(name)
            with self.subTest(name=name):
                with self.assertRaisesRegex(ConfluentCloudConfigError, "Missing required") as raised:
                    validate_kafka_connection_config(data)
                for secret in (config()["sasl.username"], config()["sasl.password"]):
                    self.assertNotIn(secret, str(raised.exception))

    def test_placeholder_credentials_are_rejected(self):
        for value in ("<KAFKA_API_KEY>", "YOUR_API_SECRET", "replace_me", "${API_KEY}",
                      "{{API_KEY}}", "changeme", "********", "xxx"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ConfluentCloudConfigError, "placeholder") as raised:
                    validate_kafka_connection_config({**config(), "sasl.password": value})
                self.assertNotIn(value, str(raised.exception))

    def test_protocol_and_mechanism_cannot_disable_authentication_or_encryption(self):
        for field, values in {
            "security.protocol": ["PLAINTEXT", "SASL_PLAINTEXT", "SSL"],
            "sasl.mechanism": ["GSSAPI", "OAUTHBEARER", "PLAIN,SCRAM-SHA-256"],
        }.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ConfluentCloudConfigError):
                        validate_kafka_connection_config({**config(), field: value})

    def test_tls_certificate_verification_cannot_be_disabled(self):
        for value in (False, "false", "0", 0, 1, None, ""):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ConfluentCloudConfigError, "certificate verification"):
                    validate_kafka_connection_config({**config(), "enable.ssl.certificate.verification": value})

    def test_tls_hostname_verification_cannot_be_disabled(self):
        for value in ("none", "", None, False):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ConfluentCloudConfigError, "hostname verification"):
                    validate_kafka_connection_config({**config(), "ssl.endpoint.identification.algorithm": value})

    def test_secure_explicit_tls_values_and_connection_options_are_allowed(self):
        data = {**config(), "enable.ssl.certificate.verification": "TRUE",
                "ssl.endpoint.identification.algorithm": "HTTPS",
                "client.id": "lesson-cloud", "ssl.ca.location": "/tmp/lesson-ca.pem"}
        result = validate_kafka_connection_config(data)
        self.assertIs(result["enable.ssl.certificate.verification"], True)
        self.assertEqual(result["ssl.endpoint.identification.algorithm"], "https")
        self.assertEqual(result["client.id"], data["client.id"])
        self.assertEqual(result["ssl.ca.location"], data["ssl.ca.location"])

    def test_role_debug_jaas_and_callback_settings_cannot_enter_connection_mapping(self):
        for name in ("group.id", "enable.auto.commit", "enable.auto.offset.store", "auto.offset.reset",
                     "acks", "enable.idempotence", "debug", "error_cb", "logger",
                     "socket.timeout.ms", "broker.address.family", "sasl.jaas.config",
                     "ssl.certificate.verify_cb", "schema.registry.url"):
            with self.subTest(name=name):
                with self.assertRaises(ConfluentCloudConfigError) as raised:
                    validate_kafka_connection_config({**config(), name: "SECRET_VALUE"})
                self.assertNotIn("SECRET_VALUE", str(raised.exception))

    def test_non_string_empty_control_and_whitespace_credentials_fail_safely(self):
        for value in (None, 123, True, b"SECRET_VALUE", [], "", "contains space", "secret\x00value"):
            with self.subTest(type=type(value).__name__):
                with self.assertRaises(ConfluentCloudConfigError) as raised:
                    validate_kafka_connection_config({**config(), "sasl.password": value})
                self.assertNotIn("SECRET_VALUE", str(raised.exception))


class DiagnosticTests(unittest.TestCase):
    def test_summary_excludes_credentials_and_masks_them_inside_client_id(self):
        data = config()
        data["client.id"] = "lesson-" + data["sasl.username"]
        data["ssl.ca.location"] = "/path/not/printed.pem"
        original = deepcopy(data)
        summary = safe_connection_summary(data)
        rendered = json.dumps(summary)
        self.assertNotIn("sasl.username", summary)
        self.assertNotIn("sasl.password", summary)
        self.assertNotIn(data["sasl.username"], rendered)
        self.assertNotIn(data["sasl.password"], rendered)
        self.assertNotIn(data["ssl.ca.location"], rendered)
        self.assertEqual(summary["client.id"], "lesson-[REDACTED]")
        self.assertTrue(summary["custom_ca_configured"])
        self.assertEqual(data, original)

    def test_error_redaction_masks_both_credentials_without_losing_the_stage(self):
        data = config()
        error = RuntimeError(f"authentication: user={data['sasl.username']}, password={data['sasl.password']}")
        result = redact_kafka_error(error, data)
        self.assertEqual(result, "authentication: user=[REDACTED], password=[REDACTED]")

    def test_error_redaction_handles_repr_json_and_url_escaping(self):
        secret = 'slash\\quote"and+equal='
        data = {**config(), "sasl.password": secret}
        variants = [secret, repr(secret)[1:-1], json.dumps(secret)[1:-1], quote(secret, safe="")]
        result = redact_kafka_error(" | ".join(variants), data)
        self.assertEqual(result, " | ".join("[REDACTED]" for _ in variants))

    def test_overlapping_credentials_are_redacted_longest_first(self):
        data = {"sasl.username": "credential", "sasl.password": "credential-longer"}
        self.assertEqual(redact_kafka_error("credential-longer credential", data),
                         "[REDACTED] [REDACTED]")

    def test_redaction_handles_an_unvalidated_or_absent_mapping(self):
        self.assertEqual(redact_kafka_error("connection failed", None), "connection failed")
        self.assertEqual(redact_kafka_error("connection failed", {"sasl.password": None}), "connection failed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
