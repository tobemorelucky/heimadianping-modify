# Kafka 秒杀 Consumer 测试记录

## 1. 测试目标

验证 Phase 2 Consumer 满足以下边界：

- 有效消息调用订单事务服务。
- DTO 字段正确映射到 `VoucherOrder`。
- 订单服务调用发生在 ACK 之前。
- 字段缺失或空消息不调用订单服务、不提交 offset。
- 订单事务异常不提交 offset，并将异常继续抛给重试机制。
- 现有 Redis Stream 与项目回归测试不受影响。

## 2. 新增测试类

```text
src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java
```

测试使用 JUnit 5、Mockito 和 `ConsumerRecord`，不依赖真实 Kafka broker。

## 3. Consumer 单元测试

### 3.1 `shouldCreateOrderBeforeAcknowledgingMessage`

测试方法：

1. 构造字段完整的 `VoucherOrderMessage`。
2. 构造 Topic 为 `hmdp.seckill.order.create.v1` 的 `ConsumerRecord`。
3. 调用 Consumer 监听方法。
4. 使用 Mockito `InOrder` 验证先调用事务服务，再调用 `acknowledgment.acknowledge()`。
5. 捕获 `VoucherOrder`，核对四个字段映射。

结果：通过。

### 3.2 `shouldRejectInvalidMessageWithoutAcknowledging`

测试方法：

1. 构造缺少 `userId` 的消息。
2. 验证监听方法抛出 `IllegalArgumentException`。
3. 验证事务服务从未调用。
4. 验证 ACK 从未调用。

结果：通过。

### 3.3 `shouldRejectNullMessageWithoutAcknowledging`

测试方法：

1. 构造 value 为 `null` 的 Kafka 记录，模拟反序列化失败或空消息。
2. 验证监听方法抛出 `IllegalArgumentException`。
3. 验证事务服务和 ACK 均未调用。

结果：通过。

### 3.4 `shouldNotAcknowledgeWhenOrderServiceFails`

测试方法：

1. 配置事务服务抛出数据库不可用异常。
2. 验证监听方法记录并继续抛出同类异常。
3. 验证订单事务服务确实被调用。
4. 验证 ACK 从未调用。

结果：通过。

## 4. 幂等相关测试

继续复用现有 `VoucherOrderTransactionalServiceTest`：

- `shouldTreatExistingOrderAsIdempotentSuccess`：已有 `(userId, voucherId)` 订单时不重复扣库存、不重复插单。
- `shouldDecreaseStockAndInsertOrder`：库存扣减成功后插入同一订单。
- `shouldRejectOrderWhenDatabaseStockCannotBeDecreased`：库存扣减失败时不插单。
- `shouldDeclareTransactionBoundary`：公开订单创建方法保留 `@Transactional`。

Consumer 将事务服务的正常返回视为可 ACK，包括新建订单和幂等重复两种情况。

## 5. 执行命令与结果

### 5.1 定向测试

执行：

```powershell
mvn "-Dtest=VoucherOrderKafkaConsumerTest,VoucherOrderTransactionalServiceTest" test
```

首次新增 3 个 Consumer 用例时结果：

```text
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

随后补充空消息用例和 `ErrorHandlingDeserializer`，并通过完整测试覆盖最终版本。

### 5.2 完整回归测试

执行：

```powershell
mvn test
```

最终结果：

```text
Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

其中包括：

- Kafka Consumer 新增测试：4 个。
- 订单事务服务测试：4 个。
- Redis Stream Consumer 测试：2 个。
- 数据库唯一索引测试：1 个。
- 项目原有其他测试：6 个。

## 6. 测试环境

- Spring Boot：2.3.12.RELEASE。
- Spring Kafka：2.5.14.RELEASE。
- Kafka Client：2.5.1。
- MySQL：本地 `127.0.0.1:3306` 可用。
- Redis：本地 `127.0.0.1:6379` 可用。
- Kafka：本地 `127.0.0.1:9092` 未启动。

Kafka 未启动时监听容器会记录 broker 重连警告，但不会阻止当前 Spring 上下文测试完成。单元测试通过直接调用监听方法验证 Consumer 边界。

## 7. 当前问题与限制

1. 本次没有真实 Kafka broker，因此没有验证真实反序列化 header、分区分配、offset 提交和重启后的重复投递。
2. 当前失败重试为 1 秒固定退避、无限次数；永久坏消息会阻塞所属分区。
3. 尚未实现 Retry Topic、DLT 和失败订单运营处理。
4. `seckill.message.mode` 尚未接入，Kafka Producer 也尚未进入秒杀请求流程。
5. Kafka Consumer 当前会随应用启动尝试连接 broker；正式灰度前仍需要独立启停开关。

## 8. 后续测试建议

- 使用嵌入式 Kafka 或 Testcontainers 验证真实生产和消费。
- 验证事务成功后 offset 已提交，事务失败后 offset 未提交。
- 模拟数据库提交后、offset 提交前宕机，验证重复消息只生成一单。
- 验证畸形 JSON 不会推进 offset。
- 在引入 Retry Topic/DLT 后验证最大次数、退避和失败消息可追踪性。
