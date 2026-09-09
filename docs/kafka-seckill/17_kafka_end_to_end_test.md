# Kafka 秒杀端到端测试记录

## 1. 测试目标

验证 Kafka 模式下以下链路可以闭环：

```text
用户上下文
  ↓
VoucherOrderServiceImpl
  ↓
Redis Lua 准入（测试中隔离模拟成功）
  ↓
真实 VoucherOrderKafkaProducer
  ↓
本地 KRaft Kafka broker
  ↓
VoucherOrderKafkaConsumer
  ↓
VoucherOrderTransactionalService 调用
  ↓
手动 ACK
```

测试没有写入正式数据库：事务服务使用 Mock 验证调用参数和调用边界，从而避免生成测试订单。Kafka 传输使用真实本地 broker 和独立测试 Topic。

## 2. 测试代码

### 2.1 模式分流单元测试

文件：`src/test/java/com/hmdp/service/impl/VoucherOrderServiceImplMessageModeTest.java`

覆盖：

- `kafka` 模式将第四个 Lua 参数设置为 `kafka`，并发送完整订单消息；
- `redis` 模式将第四个 Lua 参数设置为 `redis`，且不会调用 Kafka Producer；
- 两种模式均保留订单 ID、用户 ID 和优惠券 ID。

### 2.2 Consumer 测试

文件：`src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java`

覆盖：

- 消息转换为 `VoucherOrder`；
- 先调用事务服务，再手动 ACK；
- 参数错误或事务失败时不 ACK；
- Retry Topic 恢复成功后确认 offset。

### 2.3 真实 Kafka 联调

文件：`src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java`

新增 `shouldCompleteBusinessSwitchFlowThroughKafka()`：

- 连接 `127.0.0.1:9092`；
- 使用独立 Topic `hmdp.seckill.order.business-switch-test.v1`；
- 通过正式 `VoucherOrderServiceImpl` 和 `VoucherOrderKafkaProducer` 发送；
- 从真实 broker 读取 `ConsumerRecord`；
- 交给正式 `VoucherOrderKafkaConsumer`；
- 验证订单事务服务参数和 ACK；
- 使用随机测试 Consumer Group，不污染正式消费组 offset。

环境测试默认跳过，显式启用方式：

```powershell
$env:KAFKA_INTEGRATION_TEST_ENABLED='true'
mvn '-Dtest=KafkaLocalIntegrationTest' test
```

## 3. 测试结果

### 3.1 切换相关定向测试

```text
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

包含入口双模式测试和 Kafka Consumer 测试。

### 3.2 全部独立秒杀/Kafka 单元测试

```text
Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

包含 Producer/Consumer 路由、DLT、Kafka Consumer、Stream Consumer、事务服务和入口模式分流。

### 3.3 真实本地 Kafka 测试

环境：`hmdp-kafka`，`apache/kafka:3.9.2`，KRaft 单节点，端口 `9092`。

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

两个用例分别验证基础 JSON 收发和正式业务切换链路。broker 健康检查返回 1 个可用节点；业务切换测试 Topic 创建、发送和消费均成功。

### 3.4 完整 `mvn test`

2026-09-08 恢复 MySQL、Redis 和 Kafka 后重新执行完整命令：

```text
Tests run: 25, Failures: 0, Errors: 0, Skipped: 1
BUILD SUCCESS
```

唯一跳过项是受 `KAFKA_INTEGRATION_TEST_ENABLED` 控制的真实环境测试，属于预期行为。Spring 上下文、MySQL 查询、Redis/Redisson、正式 Kafka Listener 装配、数据库唯一索引检查及全部独立单元测试均通过。随后显式启用真实 Kafka 环境测试，2 个用例再次全部通过。

2026-09-07 首次完整运行曾因本机 MySQL 连接停在 `HikariPool-1 - Starting...` 而中断；本次复测确认它是电脑/外部环境问题，不是代码回归。

## 4. 验证结论

- 默认 Kafka 模式生效；
- Lua 的 Redis 库存校验、一人一单和订单 ID 生成保持不变；
- Kafka 消息经过真实 broker 成功到达 Consumer；
- Consumer 正确转换订单并调用事务服务；
- 事务服务成功后才手动 ACK；
- Redis Stream 回退路径仍可用且未被删除；
- 完整 Maven 回归和显式真实 Kafka 联调均已通过。
