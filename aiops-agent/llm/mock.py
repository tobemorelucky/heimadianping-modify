"""Deterministic structured provider used when no external LLM is configured."""

from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Mapping, Sequence

from llm.provider import StructuredLLMProvider


class MockLLMProvider(StructuredLLMProvider):
    """Exercise the LLM contract without credentials or network access."""

    def __init__(
        self,
        scripted_responses: Mapping[str, Sequence[str | Mapping[str, Any]]] | None = None,
    ) -> None:
        self._responses: dict[str, deque[str | Mapping[str, Any]]] = defaultdict(deque)
        for operation, responses in (scripted_responses or {}).items():
            self._responses[operation].extend(responses)
        self.calls: list[dict[str, Any]] = []

    def _complete(
        self,
        *,
        operation: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        response_schema: dict[str, Any],
    ) -> str:
        self.calls.append(
            {
                "operation": operation,
                "input": input_payload,
                "response_schema": response_schema,
            }
        )
        if self._responses[operation]:
            response = self._responses[operation].popleft()
        else:
            response = self._default_response(operation, input_payload)
        return response if isinstance(response, str) else json.dumps(response, ensure_ascii=False)

    @staticmethod
    def _default_response(operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        if operation == "plan":
            incident_id = payload["incident"]["incident_id"]
            incident_text = " ".join(
                str(payload["incident"].get(field, ""))
                for field in ("title", "description")
            ).casefold()
            context = payload.get("context", {})
            context_cards = list(context.get("evidence_cards", []))
            latest = context.get("latest_observation")
            if isinstance(latest, dict):
                context_cards.append(latest)
            already_checked_kafka = any(
                card.get("kind") == "kafka_consumer_status"
                for card in context_cards
                if isinstance(card, dict)
            )
            already_checked_business_metrics = any(
                card.get("kind") == "business_metrics"
                for card in context_cards
                if isinstance(card, dict)
            )
            already_checked_mysql = any(
                card.get("kind") == "mysql_health"
                for card in context_cards if isinstance(card, dict)
            )
            mysql_signal = any(
                keyword in incident_text
                for keyword in ("order-persistence-failure-v1", "mysql persistence", "mysql 持久化")
            )
            business_signal = any(
                keyword in incident_text
                for keyword in ("business metrics", "business pipeline", "业务指标", "业务链路")
            )
            downstream_order_degradation = any(
                card.get("interpretation")
                == "supports_downstream_order_creation_degradation"
                for card in context_cards
                if isinstance(card, dict)
            )
            if (business_signal or mysql_signal) and not already_checked_business_metrics:
                return {
                    "objective": "比较秒杀请求、Lua 准入、Kafka 发送和订单创建计数。",
                    "hypothesis": (
                        "Kafka Consumer Failure" if mysql_signal
                        else "秒杀业务链路可能在某个阶段出现转化下降。"
                    ),
                    "tool_name": "get_business_metrics",
                    "arguments": {"incident_id": incident_id},
                }
            kafka_signal = any(
                keyword in incident_text
                for keyword in ("kafka", "lag", "consumer", "消息", "消费", "积压")
            )
            if (
                (kafka_signal or downstream_order_degradation or mysql_signal)
                and not already_checked_kafka
            ):
                return {
                    "objective": "检查 Kafka 消费者组状态和消息积压。",
                    "hypothesis": (
                        "Kafka Consumer Failure" if mysql_signal
                        else "订单延迟可能由 Kafka 消费停滞或 lag 异常导致。"
                    ),
                    "tool_name": "get_kafka_status",
                    "arguments": {},
                }
            if mysql_signal and already_checked_kafka and not already_checked_mysql:
                return {
                    "objective": "验证 hmdp-consumer 的 MySQL 连接测试及连接池状态。",
                    "hypothesis": "MySQL Persistence Failure",
                    "tool_name": "get_mysql_health",
                    "arguments": {},
                }
            return {
                "objective": "检查 Spring Boot 应用日志中的错误和异常信号。",
                "hypothesis": "订单延迟可能在应用日志中留下错误或超时证据。",
                "tool_name": "search_application_logs",
                "arguments": {
                    "incident_id": incident_id,
                    "log_paths": [
                        "target/runtime/spring-boot.error.log",
                        "target/runtime/spring-boot.out.log",
                    ],
                    "max_lines": 200,
                },
            }

        if operation == "reflect":
            observation = payload["context"]["latest_observation"]
            current_hypothesis = payload.get("current_hypothesis", "")
            status = observation.get("status")
            if status in {"error", "timeout"}:
                hypothesis_status = (
                    "insufficient"
                    if observation.get("kind") == "kafka_consumer_status"
                    else "inconclusive"
                )
                return {
                    "decision": "inconclusive",
                    "hypothesis": "工具失败，当前证据不足以确认根因。",
                    "hypothesis_status": hypothesis_status,
                    "reason": (
                        f"工具返回 {status} Observation："
                        f"{observation.get('summary', 'unknown error')}"
                    ),
                    "confidence": 0.1,
                }

            facts = [str(fact) for fact in observation.get("facts", [])]
            samples = [str(sample) for sample in observation.get("samples", [])]
            if observation.get("kind") == "business_metrics":
                interpretation = str(observation.get("interpretation", ""))
                if interpretation == "supports_downstream_order_creation_degradation":
                    return {
                        "decision": "replan",
                        "hypothesis": (
                            "Kafka Consumer Failure" if current_hypothesis == "Kafka Consumer Failure"
                            else "异常位于 Kafka 发送之后、订单创建完成之前。"
                        ),
                        "hypothesis_status": (
                            "insufficient" if current_hypothesis == "Kafka Consumer Failure"
                            else "supports"
                        ),
                        "reason": (
                            "业务指标显示请求、Lua 准入和 Kafka 发送计数一致，"
                            "但订单创建成功率下降；应继续检查 Consumer 状态和应用日志。"
                        ),
                        "confidence": (
                            0.35 if current_hypothesis == "Kafka Consumer Failure" else 0.88
                        ),
                    }
                if interpretation == "contradicts_business_pipeline_degradation":
                    return {
                        "decision": "report",
                        "hypothesis": "当前业务指标不支持秒杀链路转化下降假设。",
                        "hypothesis_status": "contradicts",
                        "reason": "请求、Lua 准入、Kafka 发送与订单创建计数保持一致。",
                        "confidence": 0.85,
                    }
                if interpretation in {
                    "supports_lua_admission_drop",
                    "supports_kafka_publish_drop",
                }:
                    return {
                        "decision": "replan",
                        "hypothesis": "业务指标显示秒杀链路上游阶段转化下降。",
                        "hypothesis_status": "supports",
                        "reason": "需要使用应用日志获取独立的错误证据。",
                        "confidence": 0.75,
                    }
                return {
                    "decision": "inconclusive",
                    "hypothesis": "业务指标不足以定位异常阶段。",
                    "hypothesis_status": "insufficient",
                    "reason": "业务指标缺失、部分可用或没有足够请求活动。",
                    "confidence": 0.25,
                }
            if observation.get("kind") == "kafka_consumer_status":
                fact_text = " ".join(facts).casefold()
                if "lag_status=abnormal" in fact_text:
                    return {
                        "decision": "report",
                        "hypothesis": "Kafka 消费积压与当前订单延迟相关。",
                        "hypothesis_status": "supports",
                        "reason": "Kafka Evidence 显示 total lag 超过只读诊断阈值。",
                        "confidence": 0.9,
                    }
                if (
                    "lag_status=normal" in fact_text
                    and "consumer_status=stable" in fact_text
                    and not "member_count=0" in fact_text
                ):
                    return {
                        "decision": "replan",
                        "hypothesis": (
                            "Kafka Consumer Failure" if current_hypothesis == "Kafka Consumer Failure"
                            else "Kafka 消费停滞假设被当前状态反驳。"
                        ),
                        "hypothesis_status": "contradicts",
                        "reason": "Kafka Evidence 显示消费者组稳定、有成员且 lag 正常。",
                        "confidence": (
                            0.1 if current_hypothesis == "Kafka Consumer Failure" else 0.85
                        ),
                    }
                return {
                    "decision": "inconclusive",
                    "hypothesis": "Kafka Evidence 不足以支持或反驳当前假设。",
                    "hypothesis_status": "insufficient",
                    "reason": "Kafka 状态或 offset 数据不完整。",
                    "confidence": 0.25,
                }

            if observation.get("kind") == "mysql_health":
                fact_text = " ".join(facts).casefold()
                prior_cards = payload["context"].get("evidence_cards", [])
                business_supported = any(
                    card.get("kind") == "business_metrics"
                    and card.get("interpretation") == "supports_downstream_order_creation_degradation"
                    for card in prior_cards if isinstance(card, dict)
                )
                kafka_contradicted = any(
                    card.get("kind") == "kafka_consumer_status"
                    and card.get("interpretation") == "contradicts_kafka_consumer_unavailable_hypothesis"
                    for card in prior_cards if isinstance(card, dict)
                )
                if (
                    business_supported
                    and kafka_contradicted
                    and "source_role=hmdp-consumer" in fact_text
                    and any(
                        failure_class.casefold() in fact_text
                        for failure_class in (
                            "DATABASE_UNAVAILABLE",
                            "CONNECTION_TIMEOUT",
                            "POOL_EXHAUSTED",
                        )
                    )
                ):
                    return {
                        "decision": "report",
                        "hypothesis": "MySQL Persistence Failure",
                        "hypothesis_status": "supports",
                        "reason": "Consumer MySQL 连接测试失败；此前业务转化下降且 Kafka 消费状态正常。",
                        "confidence": 0.93,
                    }
                return {
                    "decision": "inconclusive",
                    "hypothesis": current_hypothesis,
                    "hypothesis_status": "insufficient",
                    "reason": "Consumer MySQL 状态正常或关键字段不可用，不能确认数据库根因。",
                    "confidence": 0.2,
                }

            error_count_match = next(
                (
                    re.search(r"Detected (\d+) error-like", fact)
                    for fact in facts
                    if "error-like" in fact
                ),
                None,
            )
            error_samples = [
                sample
                for sample in samples
                if re.search(
                    r"\b(error|exception|failed|failure)\b|timed\s+out",
                    sample,
                    flags=re.IGNORECASE,
                )
            ]
            error_count = (
                int(error_count_match.group(1))
                if error_count_match is not None
                else len(error_samples)
            )
            if error_count:
                first_message = error_samples[0][:500] if error_samples else facts[-1]
                return {
                    "decision": "report",
                    "hypothesis": "应用日志中的错误事件与当前故障相关。",
                    "hypothesis_status": "verified",
                    "reason": f"发现 {error_count} 条错误类日志；首条为：{first_message}",
                    "confidence": 0.85,
                }
            return {
                "decision": "inconclusive",
                "hypothesis": "当前应用日志未提供足够的根因证据。",
                "hypothesis_status": "inconclusive",
                "reason": (
                    "日志搜索完成，但没有发现可验证根因的错误事件；"
                    f"数据完整性为 {observation.get('completeness', 'unknown')}。"
                ),
                "confidence": 0.3,
            }

        if operation == "report":
            reflection = payload["reflection"]
            verified = reflection["hypothesis_status"] in {"verified", "supports"}
            return {
                "status": "confirmed" if verified else "inconclusive",
                "title": "秒杀订单延迟诊断报告",
                "conclusion": reflection["reason"],
                "root_cause": reflection["hypothesis"] if verified else None,
                "confidence": reflection["confidence"],
            }

        raise ValueError(f"unsupported mock LLM operation: {operation}")
