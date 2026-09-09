# Kafka 秒杀 Retry/DLT 测试记录

## 1. 测试目标

验证正常消费、临时异常恢复、有限重试耗尽路由 DLT、失败转发保护和 DLT 确认边界，同时确认既有 Redis Stream 与项目回归测试不受影响。

## 2. 自动化用例

| 场景 | 测试类/方法 | 预期 |
| --- | --- | --- |
| 正常消费 | `VoucherOrderKafkaConsumerTest.shouldCreateOrderBeforeAcknowledgingMessage` | 事务服务先执行，随后 ACK |
| 业务异常 | `shouldNotAcknowledgeWhenOrderServiceFails` | 异常继续抛出，不 ACK |
| 临时异常恢复 | `shouldAcknowledgeRetryMessageAfterTransientFailureRecovers` | Retry 首次失败不 ACK，再次成功后 ACK |
| 主消息失败 | `KafkaConsumerRetryRoutingTest.shouldRouteMainFailureToRetryTopic` | 保持分区并转发 Retry Topic |
| Retry 耗尽 | `shouldRouteExhaustedRetryToDlt` | 保持分区并转发 DLT，固定退避参数为 1 秒/1 次 |
| 转发失败 | `shouldFailRecoveryWhenForwardingFails` | 恢复器抛错，不能把源消息视为已恢复 |
| DLT 处理 | `VoucherOrderDltConsumerTest.shouldAcknowledgeAfterRecordingFailedOrder` | 读取异常原因，记录后 ACK |

## 3. 定向测试

执行命令：

```bash
mvn '-Dtest=VoucherOrderKafkaConsumerTest,KafkaConsumerRetryRoutingTest,VoucherOrderDltConsumerTest' test
```

结果：

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## 4. 完整回归测试

执行命令：

```bash
mvn test
```

结果：

```text
Tests run: 22, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

本机 Redis 与 MySQL 可用，Spring 上下文、数据库和 Redis 既有测试均通过。本机 Kafka `127.0.0.1:9092` 未启动，测试期间监听容器产生预期的 broker 重连警告；因此完整回归证明代码编译、Bean 装配、单元行为和既有功能未回归，不代表真实 Kafka 三 Topic 端到端链路已完成验证。

## 5. 尚未覆盖的环境验证

当前自动化测试使用 Mockito 验证消费边界与 Topic 路由，不替代真实 Kafka 集群测试。上线前还应使用三个真实 Topic 验证：

1. 暂停 MySQL，观察主消息进入 Retry Topic。
2. 在第二次 Retry 前恢复 MySQL，验证订单成功且 Retry offset 提交。
3. 保持 MySQL 故障，验证消息最终进入 DLT。
4. 临时禁止 Retry/DLT 写权限，验证源 offset 不推进。
5. 重启 Consumer，验证未确认消息恢复消费。
6. 检查 DLT header 是否保留原 Topic、partition、offset 与异常信息。

## 6. 已知限制

- 本阶段不引入嵌入式 Kafka 测试依赖，也不修改生产 Topic 管理方式。
- 参数校验错误与业务异常当前统一进入有限重试；永久不可重试异常可在后续版本增加异常分类，直接进入 DLT。
- DLT 当前按要求写日志；需依赖集中日志平台才能形成长期检索和告警能力。
