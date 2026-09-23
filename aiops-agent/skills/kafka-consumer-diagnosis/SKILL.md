+++
skill_id = "kafka-consumer-diagnosis"
name = "Kafka Consumer Diagnosis"
description = "面向 HMDP 秒杀订单异步创建链路的 Kafka Consumer 只读诊断流程。"
category = "messaging"
trigger_conditions = ["kafka", "consumer", "lag", "消息积压", "消费异常", "秒杀订单延迟", "订单创建缓慢", "业务链路", "业务指标"]
version = "1.0"
+++

# Kafka Consumer Diagnosis

## 适用场景

- 秒杀订单延迟或异步订单创建缓慢。
- Kafka lag 增长。
- Consumer 不可用、成员消失或消费状态异常。

## 调查流程

1. 如果 Incident 描述业务链路转化下降，先使用 `get_business_metrics` 比较秒杀请求、Lua 准入、Kafka 发送和订单创建计数。
2. 使用 `get_kafka_status` 检查目标 Consumer Group 的状态、活跃成员数、total lag、分区 lag 和 offset 完整性。
3. 如果 Kafka Consumer 有成员且 lag 正常，应反驳 Kafka 故障假设；当订单创建仍下降时，使用 `get_mysql_health` 核验 hmdp-consumer 的连接测试。仅在该只读观测指明 MySQL 异常时支持数据库持久化假设。
4. 使用 `search_application_logs` 查找 Consumer 停止、订单创建失败、异常或超时的独立证据。
5. 根据多来源 Evidence 决定报告、重新规划或给出不确定结论。

## 业务链路判断

- 请求数量、Lua 准入成功数量和 Kafka 发送数量基本一致，但订单创建成功数量明显下降：异常范围位于 Kafka 发送之后，应优先调查 Consumer 与 MySQL 健康；不能仅凭指标断言 MySQL 是根因。
- Lua 准入成功明显低于请求数量：优先调查准入阶段日志，不应直接归因 Kafka。
- Kafka 发送明显低于 Lua 准入成功：优先调查消息发布日志。
- 各阶段计数一致：业务指标反驳当前链路转化下降假设，应检查时间窗口或其他症状。

## 约束

- Skill 只提供调查指导，不是 Evidence。
- 最终判断必须引用当前 Incident 实际采集到的 Evidence。
- Kafka 状态正常时必须允许证伪 Kafka 假设，不能因选择了本 Skill 而预设根因。
- 只能选择 Tool Registry 已授权的只读工具；不得提交 offset、生产或重放消息，也不得重启 Consumer。
