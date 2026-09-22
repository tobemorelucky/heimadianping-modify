"""Read-only Kafka consumer-group status collection."""

from __future__ import annotations

from contextlib import suppress
from typing import Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

from tool_contracts import (
    EvidenceCompleteness,
    ObservationStatus,
    ToolError,
    ToolObservation,
)


KAFKA_NAME_PATTERN = r"^[A-Za-z0-9._-]+$"


class KafkaStatusRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    topic: str | None = Field(
        default=None,
        min_length=1,
        max_length=249,
        pattern=KAFKA_NAME_PATTERN,
    )
    consumer_group: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        pattern=KAFKA_NAME_PATTERN,
    )


class KafkaTarget(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    topic: str = Field(min_length=1, max_length=249, pattern=KAFKA_NAME_PATTERN)
    consumer_group: str = Field(
        min_length=1,
        max_length=255,
        pattern=KAFKA_NAME_PATTERN,
    )


class KafkaPartitionSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    partition: int = Field(ge=0)
    committed_offset: int | None = Field(default=None, ge=0)
    end_offset: int = Field(ge=0)
    lag: int = Field(ge=0)


class KafkaStatusSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    topic: str
    consumer_group: str
    topic_exists: bool
    group_state: str = Field(min_length=1, max_length=64)
    member_count: int = Field(ge=0)
    offsets_complete: bool
    partitions: tuple[KafkaPartitionSnapshot, ...] = Field(default_factory=tuple)


class KafkaStatusReader(Protocol):
    def read(self, topic: str, consumer_group: str) -> KafkaStatusSnapshot: ...


class KafkaPythonStatusReader:
    """Query Kafka metadata and offsets without joining a consumer group."""

    def __init__(
        self,
        bootstrap_servers: Sequence[str],
        *,
        timeout_ms: int,
    ) -> None:
        self.bootstrap_servers = list(bootstrap_servers)
        self.timeout_ms = timeout_ms

    def read(self, topic: str, consumer_group: str) -> KafkaStatusSnapshot:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
        from kafka.errors import GroupIdNotFoundError

        admin = None
        consumer = None
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="hmdp-aiops-readonly-admin",
                request_timeout_ms=self.timeout_ms,
                api_version_auto_timeout_ms=self.timeout_ms,
            )
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                client_id="hmdp-aiops-readonly-offset-reader",
                group_id=None,
                enable_auto_commit=False,
                request_timeout_ms=self.timeout_ms,
                api_version_auto_timeout_ms=self.timeout_ms,
            )
            partition_ids = consumer.partitions_for_topic(topic)
            if not partition_ids:
                return KafkaStatusSnapshot(
                    topic=topic,
                    consumer_group=consumer_group,
                    topic_exists=False,
                    group_state="unknown",
                    member_count=0,
                    offsets_complete=False,
                )

            topic_partitions = [
                TopicPartition(topic, partition) for partition in sorted(partition_ids)
            ]
            end_offsets = consumer.end_offsets(topic_partitions)
            group_missing = False
            try:
                committed_offsets = admin.list_consumer_group_offsets(
                    consumer_group,
                    partitions=topic_partitions,
                )
            except GroupIdNotFoundError:
                committed_offsets = {}
                group_missing = True

            group_state = "missing" if group_missing else "unknown"
            member_count = 0
            if not group_missing:
                try:
                    descriptions = admin.describe_consumer_groups([consumer_group])
                    if descriptions:
                        description = descriptions[0]
                        group_state = self._group_state(description)
                        member_count = self._member_count(description)
                except GroupIdNotFoundError:
                    group_state = "missing"

            snapshots: list[KafkaPartitionSnapshot] = []
            offsets_complete = True
            for partition in topic_partitions:
                metadata = committed_offsets.get(partition)
                committed = self._committed_offset(metadata)
                end_offset = int(end_offsets[partition])
                if committed is None:
                    offsets_complete = False
                    lag = end_offset
                else:
                    lag = max(0, end_offset - committed)
                snapshots.append(
                    KafkaPartitionSnapshot(
                        partition=partition.partition,
                        committed_offset=committed,
                        end_offset=end_offset,
                        lag=lag,
                    )
                )

            return KafkaStatusSnapshot(
                topic=topic,
                consumer_group=consumer_group,
                topic_exists=True,
                group_state=group_state,
                member_count=member_count,
                offsets_complete=offsets_complete,
                partitions=tuple(snapshots),
            )
        finally:
            if consumer is not None:
                with suppress(Exception):
                    consumer.close(autocommit=False)
            if admin is not None:
                with suppress(Exception):
                    admin.close()

    @staticmethod
    def _committed_offset(metadata: object) -> int | None:
        if metadata is None:
            return None
        raw_offset = getattr(metadata, "offset", metadata)
        try:
            offset = int(raw_offset)
        except (TypeError, ValueError):
            return None
        return offset if offset >= 0 else None

    @staticmethod
    def _group_state(description: object) -> str:
        if isinstance(description, dict):
            value = description.get("state", "unknown")
        else:
            value = getattr(description, "state", "unknown")
        normalized = str(getattr(value, "value", value)).strip().lower()
        return normalized or "unknown"

    @staticmethod
    def _member_count(description: object) -> int:
        if isinstance(description, dict):
            members = description.get("members", [])
        else:
            members = getattr(description, "members", [])
        return len(members or [])


def collect_kafka_status(
    request: KafkaStatusRequest,
    *,
    reader: KafkaStatusReader,
    default_topic: str,
    default_consumer_group: str,
    lag_threshold: int,
    max_partitions: int = 100,
) -> ToolObservation:
    """Build one bounded Evidence envelope from a read-only Kafka snapshot."""

    target = KafkaTarget(
        topic=request.topic or default_topic,
        consumer_group=request.consumer_group or default_consumer_group,
    )
    source = "hmdp.kafka.consumer_group"
    try:
        snapshot = reader.read(target.topic, target.consumer_group)
    except Exception as exc:
        error_type = type(exc).__name__
        timeout = isinstance(exc, TimeoutError) or error_type in {
            "KafkaTimeoutError",
            "RequestTimedOutError",
        }
        broker_unreachable = isinstance(exc, ConnectionError) or error_type in {
            "NoBrokersAvailable",
            "NodeNotReadyError",
            "KafkaConnectionError",
        }
        permission_denied = error_type in {
            "TopicAuthorizationFailedError",
            "GroupAuthorizationFailedError",
            "ClusterAuthorizationFailedError",
        }
        message = str(exc).strip() or type(exc).__name__
        message = message[:500]
        return ToolObservation(
            status=ObservationStatus.TIMEOUT if timeout else ObservationStatus.ERROR,
            kind="kafka_consumer_status",
            source=source,
            source_tool="get_kafka_status",
            summary=f"Kafka status collection failed: {message}",
            completeness=EvidenceCompleteness.UNKNOWN,
            raw_ref=None,
            data={
                "topic": target.topic,
                "consumer_group": target.consumer_group,
                "broker_reachable": False if broker_unreachable else None,
                "lag_status": "unknown",
            },
            error=ToolError(
                error_type=error_type,
                message=message,
                retryable=(
                    timeout
                    or broker_unreachable
                    or (not permission_denied and bool(getattr(exc, "retriable", False)))
                ),
            ),
        )

    total_lag = sum(partition.lag for partition in snapshot.partitions)
    ranked_partitions = sorted(
        snapshot.partitions,
        key=lambda partition: (partition.lag, partition.partition),
        reverse=True,
    )
    selected_partitions = ranked_partitions[:max_partitions]
    truncated = len(ranked_partitions) > max_partitions
    enough_data = snapshot.topic_exists and snapshot.offsets_complete and not truncated
    completeness = (
        EvidenceCompleteness.COMPLETE
        if enough_data
        else EvidenceCompleteness.PARTIAL
    )
    status = (
        ObservationStatus.SUCCESS
        if completeness is EvidenceCompleteness.COMPLETE
        else ObservationStatus.PARTIAL
    )
    lag_status = (
        "unknown"
        if not snapshot.topic_exists or not snapshot.offsets_complete
        else "abnormal" if total_lag > lag_threshold else "normal"
    )
    if not snapshot.topic_exists:
        summary = f"Kafka topic {target.topic} was not found or is not readable."
    elif lag_status == "unknown":
        summary = (
            f"Kafka group {target.consumer_group} has incomplete offset data for "
            f"topic {target.topic}."
        )
    else:
        summary = (
            f"Kafka group {target.consumer_group} state={snapshot.group_state}, "
            f"members={snapshot.member_count}, total_lag={total_lag}, "
            f"lag_status={lag_status}."
        )

    return ToolObservation(
        status=status,
        kind="kafka_consumer_status",
        source=source,
        source_tool="get_kafka_status",
        summary=summary,
        completeness=completeness,
        raw_ref=(
            f"kafka://{target.topic}/{target.consumer_group}/offsets"
            if snapshot.topic_exists
            else None
        ),
        data={
            "broker_reachable": True,
            "topic": target.topic,
            "consumer_group": target.consumer_group,
            "topic_exists": snapshot.topic_exists,
            "consumer_status": snapshot.group_state,
            "member_count": snapshot.member_count,
            "partition_count": len(snapshot.partitions),
            "partitions_returned": len(selected_partitions),
            "partitions_truncated": truncated,
            "offsets_complete": snapshot.offsets_complete,
            "total_lag": total_lag,
            "lag_threshold": lag_threshold,
            "lag_status": lag_status,
            "partitions": [
                partition.model_dump(mode="json")
                for partition in selected_partitions
            ],
        },
    )
