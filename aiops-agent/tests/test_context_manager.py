"""Bounded Context Manager behavior for application log Evidence."""

from datetime import datetime, timedelta, timezone

from context.manager import ContextLimits, ContextManager
from context.packet import ContextStage
from llm.mock import MockLLMProvider
from runtime.evidence import Evidence
from runtime.reflection import ReflectionDecision, ReflectionEngine
from tool_contracts import EvidenceCompleteness, ObservationStatus


def log_evidence(
    index: int,
    *,
    message: str,
    collected_at: datetime,
) -> Evidence:
    return Evidence(
        evidence_id=f"evi_context_{index:04d}",
        incident_id="inc_context_manager",
        step_id=f"step_context_{index}",
        status=ObservationStatus.SUCCESS,
        kind="log",
        source="hmdp.spring_boot.runtime_logs",
        source_tool="search_application_logs",
        collected_at=collected_at,
        summary="Application log search returned 1 event(s) with completeness=complete.",
        completeness=EvidenceCompleteness.COMPLETE,
        raw_ref="logs://hmdp/target/runtime/spring-boot",
        data={
            "events": [
                {
                    "path": "target/runtime/spring-boot.error.log",
                    "line_number": index + 1,
                    "message": message,
                }
            ],
            "warnings": [],
        },
    )


def test_repeated_logs_are_deduplicated_and_context_size_is_bounded():
    start = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
    evidence = [
        log_evidence(
            index,
            message=(
                f"2026-09-19T10:{index:02d}:00Z ERROR order 12345 failed "
                + "x" * 500
            ),
            collected_at=start + timedelta(minutes=index),
        )
        for index in range(20)
    ]
    manager = ContextManager(
        ContextLimits(
            max_evidence_cards=4,
            max_samples_per_card=1,
            max_sample_chars=80,
        )
    )

    packet = manager.build(evidence, stage=ContextStage.PLANNER)

    assert len(packet.evidence_cards) <= 4
    assert packet.budget.selected_evidence_count == 1
    assert packet.budget.excluded_evidence_count == 19
    assert all(
        len(sample) <= 80
        for card in packet.evidence_cards
        for sample in card.samples
    )
    assert packet.budget.estimated_chars < 1_000
    assert all(item.reason.startswith("duplicate_fingerprint") for item in packet.excluded)


def test_reflection_context_always_keeps_latest_evidence():
    start = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
    evidence = [
        log_evidence(
            index,
            message=f"2026-09-19T10:{index:02d}:00Z ERROR order 123 failed",
            collected_at=start + timedelta(minutes=index),
        )
        for index in range(12)
    ]
    latest = evidence[-1]
    manager = ContextManager(ContextLimits(max_evidence_cards=2))

    packet = manager.build(
        evidence,
        stage=ContextStage.REFLECTION,
        latest_evidence_id=latest.evidence_id,
    )

    assert packet.latest_observation is not None
    assert packet.latest_observation.evidence_id == latest.evidence_id
    assert latest.evidence_id in packet.selected_evidence_ids
    assert packet.budget.selected_evidence_count <= 2


def test_reflection_reads_latest_context_card_without_raw_evidence_data():
    now = datetime.now(timezone.utc)
    evidence = [
        log_evidence(
            1,
            message="INFO application healthy",
            collected_at=now - timedelta(minutes=1),
        ),
        log_evidence(
            2,
            message="ERROR order creation failed",
            collected_at=now,
        ),
    ]
    packet = ContextManager().build(
        evidence,
        stage=ContextStage.REFLECTION,
        latest_evidence_id=evidence[-1].evidence_id,
    )
    provider = MockLLMProvider()

    result = ReflectionEngine(provider).evaluate("订单创建失败", packet)

    assert result.decision is ReflectionDecision.REPORT
    assert packet.latest_observation is not None
    assert "ERROR order creation failed" in packet.latest_observation.samples
    reflection_input = provider.calls[0]["input"]
    assert set(reflection_input) == {"current_hypothesis", "context"}
    assert "data" not in reflection_input["context"]["latest_observation"]
