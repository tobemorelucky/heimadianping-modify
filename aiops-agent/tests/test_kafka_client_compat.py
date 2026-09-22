"""Exercise the installed kafka-python API without a live Broker."""

from __future__ import annotations

import kafka
import kafka.admin
import pytest
from kafka.errors import (
    GroupAuthorizationFailedError,
    GroupIdNotFoundError,
    KafkaConfigurationError,
    KafkaTimeoutError,
    NoBrokersAvailable,
)

from mcp_tools.kafka_status import (
    KafkaPythonStatusReader,
    KafkaStatusRequest,
    collect_kafka_status,
)
from tool_contracts import ObservationStatus


TOPIC = "hmdp.demo.order"
GROUP = "hmdp-demo-group"


def observation(reader):
    return collect_kafka_status(
        KafkaStatusRequest(),
        reader=reader,
        default_topic=TOPIC,
        default_consumer_group=GROUP,
        lag_threshold=1,
    )


def test_installed_kafka_client_is_created_with_supported_readonly_options(monkeypatch):
    calls = []
    installed_consumer = kafka.KafkaConsumer

    class FakeAdmin:
        def __init__(self, **options):
            calls.append(("admin_created", options))

        def list_consumer_group_offsets(self, group_id, *, partitions):
            calls.append(("list_offsets", group_id))
            return {partitions[0]: 2}

        def describe_consumer_groups(self, group_ids):
            calls.append(("describe_group", group_ids))
            return [{"state": "Stable", "members": [object()]}]

        def close(self):
            calls.append(("admin_closed", None))

    class FakeConsumer:
        def __init__(self, **options):
            calls.append(("consumer_created", options))

        def partitions_for_topic(self, topic):
            calls.append(("topic_partitions", topic))
            return {0}

        def end_offsets(self, partitions):
            calls.append(("end_offsets", tuple(partitions)))
            return {partitions[0]: 5}

        def close(self, *, autocommit):
            calls.append(("consumer_closed", autocommit))

    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", FakeAdmin)
    monkeypatch.setattr(kafka, "KafkaConsumer", FakeConsumer)

    result = observation(KafkaPythonStatusReader(["127.0.0.1:9092"], timeout_ms=3000))

    assert result.status is ObservationStatus.SUCCESS
    assert result.data["topic"] == TOPIC
    assert result.data["consumer_group"] == GROUP
    assert result.data["member_count"] == 1
    assert result.data["total_lag"] == 3
    admin_options = next(value for name, value in calls if name == "admin_created")
    consumer_options = next(value for name, value in calls if name == "consumer_created")
    assert "default_api_timeout_ms" not in admin_options
    assert "default_api_timeout_ms" not in consumer_options
    assert set(consumer_options) <= set(installed_consumer.DEFAULT_CONFIG)
    assert consumer_options["group_id"] is None
    assert consumer_options["enable_auto_commit"] is False
    assert consumer_options["request_timeout_ms"] == 3000
    assert consumer_options["api_version_auto_timeout_ms"] == 3000
    assert ("consumer_closed", False) in calls
    assert {name for name, _ in calls} == {
        "admin_created", "consumer_created", "topic_partitions", "end_offsets",
        "list_offsets", "describe_group", "consumer_closed", "admin_closed",
    }


def test_installed_consumer_accepts_every_supplied_option():
    options = {
        "bootstrap_servers", "client_id", "group_id", "enable_auto_commit",
        "request_timeout_ms", "api_version_auto_timeout_ms",
    }
    assert options <= set(kafka.KafkaConsumer.DEFAULT_CONFIG)
    assert "default_api_timeout_ms" not in kafka.KafkaConsumer.DEFAULT_CONFIG


@pytest.mark.parametrize(
    ("failure", "expected_status", "reachable", "retryable"),
    [
        (NoBrokersAvailable(), ObservationStatus.ERROR, False, True),
        (KafkaTimeoutError("timeout"), ObservationStatus.TIMEOUT, None, True),
        (GroupAuthorizationFailedError("denied"), ObservationStatus.ERROR, None, False),
        (KafkaConfigurationError("invalid config"), ObservationStatus.ERROR, None, False),
        (RuntimeError("offset API failed"), ObservationStatus.ERROR, None, False),
    ],
)
def test_read_failures_return_structured_observation(
    failure, expected_status, reachable, retryable,
):
    class FailingReader:
        def read(self, topic, consumer_group):
            raise failure

    result = observation(FailingReader())

    assert result.status is expected_status
    assert result.data["broker_reachable"] is reachable
    assert result.error is not None
    assert result.error.error_type == type(failure).__name__
    assert result.error.retryable is retryable


@pytest.mark.parametrize("missing", ["topic", "group"])
def test_missing_topic_or_group_returns_partial_not_exception(monkeypatch, missing):
    class FakeAdmin:
        def __init__(self, **options):
            pass

        def list_consumer_group_offsets(self, group_id, *, partitions):
            raise GroupIdNotFoundError()

        def close(self):
            pass

    class FakeConsumer:
        def __init__(self, **options):
            pass

        def partitions_for_topic(self, topic):
            return None if missing == "topic" else {0}

        def end_offsets(self, partitions):
            return {partitions[0]: 3}

        def close(self, *, autocommit):
            assert autocommit is False

    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", FakeAdmin)
    monkeypatch.setattr(kafka, "KafkaConsumer", FakeConsumer)

    result = observation(KafkaPythonStatusReader(["127.0.0.1:9092"], timeout_ms=3000))

    assert result.status is ObservationStatus.PARTIAL
    assert result.data["topic_exists"] is (missing == "group")
    assert result.data["offsets_complete"] is False
    if missing == "group":
        assert result.data["consumer_status"] == "missing"
