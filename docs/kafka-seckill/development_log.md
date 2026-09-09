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

## 2026-09-06（Kafka Consumer Phase 2）

### 操作

将 Kafka Consumer 从仅打印消息升级为真实订单消费者，增加参数校验、订单实体转换、事务服务调用、手动 offset 确认、异常日志和简单重试机制。

### 修改内容

- `VoucherOrderKafkaConsumer` 注入并调用 `VoucherOrderTransactionalService`。
- 消息字段完整后才进入订单事务服务。
- 事务服务正常返回后才调用 `Acknowledgment.acknowledge()`。
- 校验或事务失败时记录日志、继续抛出异常且不提交 offset。
- Listener ACK 模式改为 `MANUAL_IMMEDIATE`，自动提交继续关闭。
- 使用 `ErrorHandlingDeserializer` 包装 key/value 反序列化器，空消息不会被确认。
- 使用 `SeekToCurrentErrorHandler` 和 1 秒固定退避在原分区无限重试，未引入 Retry Topic。
- 继续复用 `(user_id, voucher_id)` 唯一索引和现有事务服务处理重复消费。

### 文件列表

- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java`
- `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java`
- `src/main/resources/application.yaml`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java`
- `docs/kafka-seckill/09_kafka_consumer_implementation.md`
- `docs/kafka-seckill/10_kafka_consumer_test.md`
- `docs/kafka-seckill/development_log.md`

### 保留内容

- 未修改 `VoucherOrderKafkaProducer` 和 `KafkaProducerConfig`。
- 未修改 `VoucherOrderServiceImpl`、`seckill.lua` 或 `VoucherOrderTransactionalService`。
- 未修改或删除 `VoucherOrderStreamConsumer` 及 Redis Stream 相关资源。
- 未增加或切换 `seckill.message.mode`。

### 测试结果

- 定向测试：`VoucherOrderKafkaConsumerTest` 与 `VoucherOrderTransactionalServiceTest` 共 7 个测试通过。
- 最终完整测试：`Tests run: 17, Failures: 0, Errors: 0, Skipped: 0`。
- Maven 结果：`BUILD SUCCESS`。
- 本地 Kafka broker 未启动，因此本次覆盖单元测试、Spring 装配和既有回归，不包含真实 Kafka 端到端 offset 验证。

### 当前问题

当前无限重试会阻塞失败消息所在分区。这是在暂不实现 Retry Topic/DLT 时，为保证失败消息不被空提交所采用的阶段性策略。

### 当前状态

Kafka Consumer 已具备真实订单消费能力；Producer 和秒杀消息模式仍未切换，现有 Redis Stream 链路保持不变。

## 2026-09-07（Kafka Consumer Phase 2.5）

### 操作

将 Kafka Consumer 的原分区无限重试改造为主 Topic、Retry Topic、DLT 三段式有限失败处理，并增加可靠转发、DLT 日志消费者和自动化测试。

### 修改原因

Phase 2 的无限重试会让永久失败消息持续占用消费线程并阻塞所在分区。本阶段为失败消息建立有限次数和明确终态，同时保证 Retry/DLT 转发失败时不错误提交源 offset。

### 修改文件

- `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java`
- `src/main/java/com/hmdp/config/kafka/ReliableDeadLetterPublishingRecoverer.java`
- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java`
- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumer.java`
- `src/main/resources/application.yaml`
- `src/test/java/com/hmdp/config/kafka/KafkaConsumerRetryRoutingTest.java`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumerTest.java`
- `docs/kafka-seckill/11_kafka_retry_dlt_design.md`
- `docs/kafka-seckill/12_kafka_retry_dlt_implementation.md`
- `docs/kafka-seckill/13_kafka_retry_test.md`
- `docs/kafka-seckill/14_kafka_retry_interview.md`
- `docs/kafka-seckill/development_log.md`

### 兼容说明

项目当前 Spring Kafka 2.5.14 不包含 `DefaultErrorHandler`，因此使用同版本的 `SeekToCurrentErrorHandler + FixedBackOff + DeadLetterPublishingRecoverer` 实现等价语义，未冒险升级 Spring Boot 依赖链。新增可靠恢复器等待转发 broker 结果后才允许提交源 offset。

### 测试结果

- 定向测试：`Tests run: 9, Failures: 0, Errors: 0, Skipped: 0`，`BUILD SUCCESS`。
- 完整回归：`Tests run: 22, Failures: 0, Errors: 0, Skipped: 0`，`BUILD SUCCESS`。
- 本机 Kafka `127.0.0.1:9092` 未启动，监听容器出现预期重连告警；本次未执行真实三 Topic 的端到端 offset 与重启恢复验证。

### 保留内容

未修改 Kafka Producer、`VoucherOrderServiceImpl`、`seckill.lua`、Redis Stream 消费代码或 `seckill.message.mode`；现有 Redis Stream 秒杀入口未切换。

### 当前状态

Kafka Consumer 已具备有限 Retry/DLT 失败终态和转发失败保护；等待在真实 Kafka 环境完成三 Topic 故障演练。

## 2026-09-07（Kafka 本地环境部署）

### 操作

增加单节点 KRaft Kafka 本地开发环境和部署说明，不依赖 ZooKeeper。

### 修改文件

- `docker/kafka/docker-compose-kafka.yml`
- `docs/kafka-seckill/15_kafka_environment_setup.md`
- `docs/kafka-seckill/development_log.md`

### 配置说明

- 使用官方 `apache/kafka:3.9.2` JVM 镜像并固定版本。
- 容器名和主机名均为 `hmdp-kafka`。
- 单节点同时承担 broker、controller 角色，使用 KRaft quorum。
- 宿主机通过 `localhost:9092` 连接，Docker 网络内通过 `hmdp-kafka:19092` 连接。
- Kafka 数据写入命名卷 `hmdp-kafka-data`。
- 本地单节点内部 Topic 复制因子为 1，默认分区数为 12。

### 保留内容

未修改任何 Java、Lua、SQL 或 Spring Boot 业务配置；Redis Stream、Kafka Consumer/Producer 和秒杀入口均保持不变。

### 验证结果

执行：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml config
```

结果：退出码 `0`，Compose 成功解析；服务名、镜像、KRaft 参数、9092 端口映射和 `hmdp-kafka-data` 命名卷均正确展开。当前沙箱无法读取用户级 `C:\Users\he\.docker\config.json`，Docker 输出两条权限警告，但不影响 Compose 文件校验结果。

## 2026-09-07（Kafka Phase 3.0 联调准备）

### 操作

启动本地 KRaft Kafka，完成容器、broker API、Spring Kafka AdminClient、测试 Producer、独立 Consumer、JSON 消息和测试 offset 的真实联调。

### 修改文件

- `src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java`
- `docs/kafka-seckill/16_kafka_integration_test.md`
- `docs/kafka-seckill/development_log.md`

### 测试隔离

- 仅使用 `hmdp.seckill.order.integration-test.v1` 专用 Topic。
- 每次创建随机测试 Consumer Group。
- 不加载 Spring Boot 应用上下文和正式 `@KafkaListener`。
- 不调用订单服务、数据库或正式秒杀入口。
- 使用 `KAFKA_INTEGRATION_TEST_ENABLED=true` 显式启用，普通 `mvn test` 默认跳过该环境测试。

### 环境检查

- `docker compose config --quiet`：通过。
- `hmdp-kafka`：运行状态 `Up`，宿主机端口 `9092` 可用。
- `kafka-broker-api-versions.sh`：成功返回节点 1 的 API 能力。
- AdminClient：clusterId 为 `4L6g3nShT-eMCtK--X86sw`，broker 数量为 1。
- 联调 Topic：1 分区、复制因子 1、Leader/ISR 均为节点 1。

### 消息日志

```text
KAFKA_INTEGRATION_MESSAGE_SENT topic=hmdp.seckill.order.integration-test.v1, partition=0, offset=0, orderId=1788750496814, userId=920250907, voucherId=930250907
KAFKA_INTEGRATION_MESSAGE_CONSUMED topic=hmdp.seckill.order.integration-test.v1, partition=0, offset=0, key=920250907, orderId=1788750496814, userId=920250907, voucherId=930250907
```

### 测试结果

```text
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

### 保留内容

未修改 Java 业务代码、Lua、Spring Boot 正式配置、Kafka Producer/Consumer 业务实现、秒杀入口或 Redis Stream 模式。

### 当前状态

Kafka 本地环境与 Spring Kafka 独立收发联调通过，具备正式 Topic 创建和后续灰度联调前置条件。

## 2026-09-07（Kafka Phase 3 正式业务切换）

### 操作

将秒杀订单默认消息通道由 Redis Stream 切换为 Kafka，保留 Redis Lua 准入、订单号生成、一人一单、数据库唯一索引、订单事务和手动 ACK，并通过配置开关保留 Redis Stream 回退链路。

### 修改原因

Phase 1 至 Phase 3.0 已完成 Kafka 基础设施、真实 Consumer、有限重试/DLT 和本地环境联调，本阶段将正式秒杀入口接入已经验证的 Kafka 链路。

### 修改文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java`
- `src/main/resources/seckill.lua`
- `src/main/resources/application.yaml`
- `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java`
- `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java`
- `src/test/java/com/hmdp/service/impl/VoucherOrderServiceImplMessageModeTest.java`
- `src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java`
- `docs/kafka-seckill/16_kafka_business_switch.md`
- `docs/kafka-seckill/17_kafka_end_to_end_test.md`
- `docs/kafka-seckill/development_log.md`

### 流程变化

- 新增 `seckill.message.mode`，默认值为 `kafka`，支持环境变量 `SECKILL_MESSAGE_MODE` 覆盖为 `redis`。
- Lua 新增第四个消息模式参数；Kafka 模式不写 Stream，Redis 模式继续原子 `XADD stream.orders`。
- Kafka 模式在 Lua 准入成功后构造 `VoucherOrderMessage`，等待 Producer 取得 broker 确认后返回订单号。
- 现有 Kafka Consumer 将消息转换为 `VoucherOrder`，调用事务服务后手动 ACK。
- `VoucherOrderStreamConsumer` 保留，未删除。

### 测试结果

- 入口模式与 Consumer 定向测试：7 个通过，0 失败。
- Kafka/秒杀独立单元回归：17 个通过，0 失败。
- 本地 KRaft Kafka 真实联调：2 个通过，0 失败；覆盖正式入口、Producer、broker、Consumer、事务服务调用和 ACK。
- 完整 `mvn test`：Spring 上下文启动后阻塞于 MySQL `HikariPool-1 - Starting...`，约 16 分钟后 Surefire fork 异常退出。新增代码已编译，独立测试与真实 Kafka 测试均通过；需在 MySQL 可连接后重跑完整环境回归。

### 当前状态

Kafka 已成为秒杀订单默认消息链路；Redis Stream 灰度回退仍保留。当前简单直投方案仍存在 Redis 与 Kafka 跨系统非原子窗口，已在切换文档中记录，未按本阶段要求引入复杂 Outbox 或分布式事务。

## 2026-09-08（Phase 3 环境恢复与最终回归）

### 操作

电脑恢复后确认 Phase 3 工作区修改完整，重新启动本地 KRaft Kafka，并在 MySQL、Redis、Kafka 均可连接的条件下完成昨日未闭环的完整 Maven 回归和真实 Kafka 复测。

### 修改文件

- `docs/kafka-seckill/17_kafka_end_to_end_test.md`
- `docs/kafka-seckill/development_log.md`

本次续测没有修改 Java、Lua、SQL 或 Spring Boot 配置。

### 环境检查

- MySQL `127.0.0.1:3306`：TCP 连接成功；完整测试中 HikariPool 正常启动并完成查询。
- Redis `127.0.0.1:6379`：TCP 连接成功；Redisson 测试通过。
- Kafka `127.0.0.1:9092`：`hmdp-kafka` KRaft 容器恢复运行，端口连接成功。

### 测试结果

- 完整 `mvn test`：`Tests run: 25, Failures: 0, Errors: 0, Skipped: 1`，`BUILD SUCCESS`。
- 跳过项为默认关闭的真实 Kafka 环境测试，符合测试隔离设计。
- 显式设置 `KAFKA_INTEGRATION_TEST_ENABLED=true` 后运行 `KafkaLocalIntegrationTest`：`Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`，`BUILD SUCCESS`。
- 两个真实联调用例覆盖基础 JSON 收发，以及秒杀入口、Producer、broker、正式 Consumer、事务服务调用与手动 ACK 的业务闭环。

### 当前状态

Kafka Phase 3 正式业务切换的代码、文档、完整回归和真实 Kafka 联调全部完成。昨日 MySQL 初始化阻塞已确认属于临时环境问题并已闭环。
