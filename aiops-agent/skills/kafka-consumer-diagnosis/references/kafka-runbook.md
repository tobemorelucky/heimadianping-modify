# HMDP Kafka Consumer 只读判断参考

本参考文档用于解释观测，不是故障事实。

- `member_count == 0` 且 lag 持续高于阈值：支持 Consumer 不可用或停止消费的假设。
- 存在活跃成员、lag 正常且 offset 完整：反驳 Consumer 停止假设，应调查应用日志或其他依赖。
- Broker 不可达、offset 缺失或 Observation 为 `partial/error/timeout`：证据不足，不能直接确认 Consumer 故障。
- 日志中的 `consumer stopped unexpectedly` 可以作为独立支持证据，但仍应和 Consumer Group 状态及 lag 交叉验证。
- 业务指标中请求、Lua 准入和 Kafka 发送保持一致而订单创建下降，只能把异常范围收敛到下游；仍需 Kafka 状态、日志以及未来的 MySQL Evidence 才能确认具体根因。

所有结论必须保留 Evidence ID、采集时间和完整性信息。该 Runbook 不包含执行、重启、offset 修改或消息重放步骤。
