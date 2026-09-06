# 开发日志

## 2026-09-06

### 操作

完成 Kafka 秒杀异步订单架构设计，覆盖整体链路、Topic 与消息协议、Producer、Consumer、手动 offset、幂等、重试、DLT、服务重启恢复和 Redis Stream 迁移方案。

### 修改文件

无 Java、配置、SQL 或 Lua 文件修改。

### 生成文档

- `docs/kafka-seckill/01_kafka_architecture_design.md`
- `docs/kafka-seckill/02_topic_and_message_design.md`
- `docs/kafka-seckill/03_consumer_design.md`
- `docs/kafka-seckill/04_reliability_design.md`
- `docs/kafka-seckill/04_interview_questions.md`
- `docs/kafka-seckill/05_migration_plan.md`
- `docs/kafka-seckill/development_log.md`

### 设计结论

- 目标链路采用 Redis Lua 预扣、Kafka 异步投递、Consumer Group 消费、MySQL 事务落库。
- 为解决 Lua 与 Kafka 的跨系统间隙，设计轻量 Redis Outbox；它不使用 Redis Stream，也不承担消息队列职责。
- 主 Topic 初始为 12 分区，使用 `userId` 作为 key，降低热门优惠券的单分区热点。
- 系统采用至少一次投递与消费，通过订单主键、用户与优惠券唯一索引以及本地事务实现业务结果等效一次。
- Redis Stream 按影子验证、单写切换、观察期和最终清理四个阶段退出。

### 测试结果

本阶段按要求只进行架构设计，未修改代码，因此未运行项目自动化测试。已完成文档结构、文件清单、关键流程和故障路径的静态核对；代码实施阶段必须执行迁移计划中定义的单元、集成、并发与故障演练。

### 当前状态

Kafka 秒杀架构设计完成，等待进入代码实施与验证阶段。

## 2026-09-06（Kafka 实现第一阶段）

### 操作

完成 Kafka 基础设施接入，增加 Spring Kafka 依赖、Producer/Consumer 配置、订单消息对象、异步 Producer，以及只打印消息的 Consumer 空实现。

### 修改内容

- 使用 Spring Boot BOM 管理 Spring Kafka 2.5.14.RELEASE，底层 Kafka Client 为 2.5.1。
- 配置默认 broker `127.0.0.1:9092`，支持通过 `KAFKA_BOOTSTRAP_SERVERS` 覆盖。
- 配置字符串 key、JSON 消息、`acks=all`、3 次 Producer 重试。
- Consumer Group 为 `hmdp-seckill-order-create-v1`，关闭自动提交并使用记录级确认。
- Topic 默认为 `hmdp.seckill.order.create.v1`，支持通过 `KAFKA_VOUCHER_ORDER_TOPIC` 覆盖。
- `sendOrderMessage()` 使用 `userId` 作为消息 key，并记录同步异常、异步成功与异步失败日志。
- Consumer 仅监听并记录消息，不调用订单事务服务。

### 文件列表

- `pom.xml`
- `src/main/resources/application.yaml`
- `src/main/java/com/hmdp/config/kafka/KafkaProducerConfig.java`
- `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java`
- `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java`
- `src/main/java/com/hmdp/kafka/producer/VoucherOrderKafkaProducer.java`
- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java`
- `docs/kafka-seckill/06_kafka_implementation_phase1.md`
- `docs/kafka-seckill/development_log.md`

### 保留内容

Redis Stream Consumer、Lua 脚本、秒杀入口和订单事务服务均未删除、未修改。Kafka Producer 尚未接入秒杀核心流程。

### 测试结果

- 首次 `mvn test`：主代码和测试代码编译成功；因本机 Redis 6379 未启动，5 个原有 Spring 上下文测试连接 Redisson 失败，另外 8 个测试通过。
- 使用无数据卷的临时 `redis:7-alpine` 容器提供 Redis 后重新执行 `mvn test`。
- 最终结果：`Tests run: 13, Failures: 0, Errors: 0, Skipped: 0`，`BUILD SUCCESS`。
- 测试完成后临时 Redis 容器已停止并自动删除。
- 本机未启动 Kafka broker；监听容器按预期记录重连警告，因此本次结果验证了编译、Spring 装配和现有回归测试，不代表 Kafka 端到端收发已经通过。

### 当前状态

Kafka 基础设施接入完成，现有 Redis Stream 秒杀链路保持运行方式不变。
