# Kafka 秒杀实现第一阶段

## 1. 阶段目标

本阶段只完成 Kafka 基础设施接入：增加依赖、Producer/Consumer 配置、订单消息对象、消息发送组件，以及只打印消息的 Consumer。

当前 Redis Stream 秒杀链路保持不变：

- 未修改 `VoucherOrderServiceImpl`。
- 未修改 `VoucherOrderStreamConsumer`。
- 未修改 `VoucherOrderTransactionalService`。
- 未修改 `seckill.lua` 和 Redis Stream key。
- Kafka Producer 尚未被秒杀业务调用。
- Kafka Consumer 不调用任何订单业务服务，也不写数据库。

## 2. 新增文件

| 文件 | 说明 |
|---|---|
| `src/main/java/com/hmdp/config/kafka/KafkaProducerConfig.java` | 创建订单消息 `ProducerFactory` 和专用 `KafkaTemplate` |
| `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java` | 创建订单消息 `ConsumerFactory` 和监听容器工厂 |
| `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java` | 定义 `orderId`、`userId`、`voucherId`、`createTime` |
| `src/main/java/com/hmdp/kafka/producer/VoucherOrderKafkaProducer.java` | 提供 `sendOrderMessage()`，使用 `userId` 作为 key，记录成功/失败日志 |
| `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java` | 监听订单 Topic 并打印消息与 Kafka 元数据 |
| `docs/kafka-seckill/06_kafka_implementation_phase1.md` | 本阶段实施说明 |

## 3. 修改文件

| 文件 | 修改内容 |
|---|---|
| `pom.xml` | 增加 `org.springframework.kafka:spring-kafka`，版本由 Spring Boot 2.3.12 BOM 管理 |
| `src/main/resources/application.yaml` | 增加 Kafka broker、序列化、确认、重试、消费组、offset 与 Topic 配置 |
| `docs/kafka-seckill/development_log.md` | 记录本阶段实现和测试结果 |

## 4. 配置说明

### 4.1 连接与 Topic

| 配置 | 默认值 | 环境变量覆盖 | 说明 |
|---|---|---|---|
| `spring.kafka.bootstrap-servers` | `127.0.0.1:9092` | `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker 地址，多个地址使用逗号分隔 |
| `hmdp.kafka.topics.voucher-order` | `hmdp.seckill.order.create.v1` | `KAFKA_VOUCHER_ORDER_TOPIC` | Phase 1 监听和手动测试使用的 Topic |
| `spring.kafka.consumer.group-id` | `hmdp-seckill-order-create-v1` | 暂无单独环境变量 | 秒杀订单消费者组 |

### 4.2 Producer

| 配置 | 值 | 说明 |
|---|---|---|
| key serializer | `StringSerializer` | `userId` 转换为字符串作为消息 key |
| value serializer | `JsonSerializer` | `VoucherOrderMessage` 序列化为 JSON |
| `acks` | `all` | 等待 ISR 副本确认；生产环境还需配合 Topic 副本与 ISR 配置 |
| `retries` | `3` | Phase 1 的基础客户端重试次数 |

`sendOrderMessage()` 返回 `ListenableFuture`。同步异常会记录日志后继续抛给调用方；异步发送成功和失败通过 callback 记录。当前阶段没有将此方法接入秒杀入口。

### 4.3 Consumer

| 配置 | 值 | 说明 |
|---|---|---|
| key deserializer | `StringDeserializer` | 反序列化字符串 `userId` key |
| value deserializer | `JsonDeserializer<VoucherOrderMessage>` | 仅信任 `com.hmdp.kafka.message` 包 |
| `enable-auto-commit` | `false` | 禁止 Kafka 客户端定时自动提交 |
| `ack-mode` | `record` | 监听方法正常返回后按单条记录提交 offset |
| `auto-offset-reset` | `earliest` | 消费组无历史 offset 时从最早可用消息开始 |

Phase 1 Consumer 仅输出日志。正式订单业务接入前，需要按可靠性设计升级为显式业务成功后的手动确认与重试/DLT 流程。

## 5. 启动方式

### 5.1 环境依赖

启动应用前应确保：

- MySQL 已启动，默认连接 `127.0.0.1:3306/hmdp`。
- Redis 已启动，默认连接 `127.0.0.1:6379`；现有 Redis Stream Consumer 仍然保留并会启动。
- Kafka 已启动，默认连接 `127.0.0.1:9092`。
- 已创建 Topic `hmdp.seckill.order.create.v1`。

本地单 broker 示例：

```powershell
kafka-topics.bat --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic hmdp.seckill.order.create.v1 --partitions 12 --replication-factor 1
```

生产环境应使用架构设计中的 3 副本配置，不能照搬本地单副本示例。

如 broker 地址不同，可先设置环境变量再启动：

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:9092'
$env:KAFKA_VOUCHER_ORDER_TOPIC='hmdp.seckill.order.create.v1'
mvn spring-boot:run
```

Kafka 暂时不可用时监听容器会后台重连；由于 Kafka 尚未接入业务入口，这不会改变当前 Redis Stream 秒杀流程。

## 6. 测试方式

### 6.1 项目自动化测试

确保测试环境中的 MySQL 与 Redis 可用后执行：

```powershell
mvn test
```

本阶段实际结果：

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

第一次执行时本机 Redis 6379 未启动，原有 Spring 上下文测试因 `RedissonClient` 连接拒绝而失败；启动无数据卷的临时 Redis 后重新执行，13 个测试全部通过。临时容器已在测试完成后停止并自动删除。

测试期间本机 Kafka 9092 未启动，Consumer 记录了 broker 重连警告，但应用上下文、代码编译和既有测试均正常。这次 `mvn test` 不等同于 Kafka broker 端到端验证。

### 6.2 Kafka 监听验证

启动 Kafka 与应用后，可使用 Kafka 控制台 Producer 向 Topic 写入一条 JSON：

```powershell
kafka-console-producer.bat --bootstrap-server 127.0.0.1:9092 --topic hmdp.seckill.order.create.v1 --property parse.key=true --property key.separator=:
```

输入示例：

```text
1024:{"orderId":10001,"userId":1024,"voucherId":88,"createTime":"2026-09-06T16:00:00"}
```

预期应用日志包含 key、订单字段、Topic、partition 和 offset，并且数据库订单表没有新增记录。

### 6.3 Producer 验证

当前业务没有调用 Producer。后续可以在独立集成测试中构造 `VoucherOrderMessage` 并调用 `sendOrderMessage()`，验证：

- Kafka 消息 key 等于 `userId` 的字符串值。
- 成功日志包含 topic、partition 和 offset。
- broker 不可用时 Future 异常完成并记录失败日志。
- 测试不经过秒杀接口，避免提前改变核心业务流程。

## 7. 当前状态与下一阶段边界

Kafka 基础 Bean 和监听器已可装配，代码与现有测试兼容。下一阶段在修改秒杀业务前，应先准备可重复的 Kafka 集成测试环境，再设计 Redis Stream 与 Kafka 的灰度切换；不得直接删除现有 Stream 消费链路。
