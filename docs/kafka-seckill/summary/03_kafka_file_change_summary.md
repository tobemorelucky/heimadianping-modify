# Kafka 秒杀文件变更汇总

## 1. 新增文件

### 1.1 Kafka 生产代码

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/config/kafka/KafkaProducerConfig.java` | 配置订单消息 ProducerFactory 和 KafkaTemplate，使用字符串 key、JSON value、`acks` 与 Producer 重试参数。 |
| `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java` | 配置 JSON 反序列化、关闭自动提交、手动 ACK、主/Retry/DLT 监听容器和有限失败路由。 |
| `src/main/java/com/hmdp/config/kafka/ReliableDeadLetterPublishingRecoverer.java` | 等待 Retry/DLT 转发取得 broker 确认，防止转发失败时错误提交源 offset。 |
| `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java` | 定义 Kafka 订单事件 DTO：`orderId`、`userId`、`voucherId`、`createTime`。 |
| `src/main/java/com/hmdp/kafka/producer/VoucherOrderKafkaProducer.java` | 使用 `userId` 作为 key 发送秒杀订单消息，记录成功元数据和发送异常。 |
| `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java` | 监听主 Topic 和 Retry Topic，校验消息、转换订单、调用事务服务并在成功后手动 ACK。 |
| `src/main/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumer.java` | 监听 DLT，记录失败订单、异常原因、时间及 Kafka 位点，记录完成后 ACK。 |

### 1.2 测试与本地环境

| 文件 | 作用 |
| --- | --- |
| `src/test/java/com/hmdp/config/kafka/KafkaConsumerRetryRoutingTest.java` | 验证主 Topic 到 Retry、Retry 到 DLT，以及转发失败不错误恢复。 |
| `src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java` | 验证消息校验、订单转换、事务服务调用顺序和手动 ACK 边界。 |
| `src/test/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumerTest.java` | 验证 DLT 异常信息读取、失败日志语义和 ACK。 |
| `src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java` | 使用独立 Topic 和随机消费组验证真实 KRaft broker 收发，以及入口到正式 Consumer 的业务闭环。 |
| `src/test/java/com/hmdp/service/impl/VoucherOrderServiceImplMessageModeTest.java` | 验证 Kafka 默认模式、Lua 第四个参数、Producer 投递和 Redis Stream 回退。 |
| `docker/kafka/docker-compose-kafka.yml` | 提供 `apache/kafka:3.9.2` 单节点 KRaft 本地环境、9092 端口和 `hmdp-kafka-data` 持久化卷。 |

### 1.3 Kafka 阶段文档

| 文件 | 作用 |
| --- | --- |
| `docs/kafka-seckill/01_kafka_architecture_design.md` | 初始 Kafka 秒杀总体架构设计。 |
| `docs/kafka-seckill/02_topic_and_message_design.md` | Topic、分区、key 和消息结构设计。 |
| `docs/kafka-seckill/03_consumer_design.md` | 消费者组、手动 offset、事务和幂等设计。 |
| `docs/kafka-seckill/04_reliability_design.md` | 消息不丢、重复消费、失败和重启恢复设计。 |
| `docs/kafka-seckill/04_interview_questions.md` | 初始 Kafka 面试问题整理。 |
| `docs/kafka-seckill/05_migration_plan.md` | Redis Stream 向 Kafka 的分阶段迁移计划。 |
| `docs/kafka-seckill/06_kafka_implementation_phase1.md` | Kafka 依赖、配置、Producer 和空 Consumer 第一阶段记录。 |
| `docs/kafka-seckill/07_kafka_integration_design.md` | 正式集成前的现状、目标链路和灰度开关设计。 |
| `docs/kafka-seckill/08_kafka_switch_interview.md` | 业务切换相关面试说明。 |
| `docs/kafka-seckill/09_kafka_consumer_implementation.md` | 真实订单 Consumer、事务调用和 ACK 实现记录。 |
| `docs/kafka-seckill/10_kafka_consumer_test.md` | Consumer 单元测试范围与结果。 |
| `docs/kafka-seckill/11_kafka_retry_dlt_design.md` | 有限重试、Retry Topic 与 DLT 设计。 |
| `docs/kafka-seckill/12_kafka_retry_dlt_implementation.md` | Retry/DLT 实现与 Spring Kafka 2.5 兼容说明。 |
| `docs/kafka-seckill/13_kafka_retry_test.md` | 正常、重试、DLT 与转发失败测试记录。 |
| `docs/kafka-seckill/14_kafka_retry_interview.md` | Retry/DLT 面试回答。 |
| `docs/kafka-seckill/15_kafka_environment_setup.md` | KRaft Docker 环境、Topic 命令和排障说明。 |
| `docs/kafka-seckill/16_kafka_integration_test.md` | Kafka 与 Spring Boot 独立联调记录。 |
| `docs/kafka-seckill/16_kafka_business_switch.md` | Kafka 正式业务切换、灰度回退和当前边界。 |
| `docs/kafka-seckill/17_kafka_end_to_end_test.md` | 完整回归和真实 Kafka 端到端测试结果。 |
| `docs/kafka-seckill/development_log.md` | Kafka 各阶段详细开发日志。 |
| `docs/kafka-seckill/summary/01_kafka_final_architecture.md` | 最终架构、流程、可靠性和验证基线归档。 |
| `docs/kafka-seckill/summary/02_kafka_interview_summary.md` | 面向秋招的 Kafka 秒杀回答汇总。 |
| `docs/kafka-seckill/summary/03_kafka_file_change_summary.md` | Kafka 阶段文件变更清单。 |

## 2. 修改文件

| 文件 | 修改内容与作用 |
| --- | --- |
| `pom.xml` | 引入 `spring-kafka`，版本由 Spring Boot 2.3.12.RELEASE 依赖管理。 |
| `src/main/resources/application.yaml` | 增加 broker、序列化/反序列化、`acks=all`、Producer 重试、消费组、手动 ACK、三个 Topic 和 `seckill.message.mode` 配置；默认 Kafka。 |
| `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java` | Lua 准入成功后按模式选择 Kafka 或 Redis Stream；Kafka 模式构造消息、调用 Producer 并等待 broker 确认。 |
| `src/main/resources/seckill.lua` | 增加第四个 `messageMode` 参数；保留原子库存和一人一单逻辑，只在 Redis 模式执行 `XADD stream.orders`。 |
| `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java` | 作为 Kafka 与 Stream 共用的事务边界，按用户与优惠券幂等检查，并在同一事务中完成数据库库存扣减和订单插入；Kafka 阶段同步更新了通道无关注释。 |
| `docs/development-log.md` | 增加 Kafka 阶段完成与归档记录。 |

## 3. 前置秒杀基线文件

以下文件在 Kafka 改造前的秒杀加固阶段已创建，但构成 Kafka 最终方案的可靠性前提，不重复计为 Kafka 新增代码：

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java` | 提供独立 Spring 事务代理边界。 |
| `src/main/resources/db/migration/V20260906_01__add_voucher_order_unique_index.sql` | 为 `tb_voucher_order(user_id, voucher_id)` 增加唯一索引，作为重复消费最终防线。 |
| `src/main/java/com/hmdp/service/impl/VoucherOrderStreamConsumer.java` | 保留 Redis Stream 消费和回退能力，并与 Kafka Consumer 复用事务服务。 |
| `src/test/java/com/hmdp/service/impl/VoucherOrderTransactionalServiceTest.java` | 验证幂等、条件扣库存、插入失败和事务服务行为。 |
| `src/test/java/com/hmdp/service/impl/VoucherOrderStreamConsumerTest.java` | 验证 Redis Stream 成功后 ACK、失败不错误 ACK。 |
| `src/test/java/com/hmdp/service/impl/SeckillDatabaseSchemaTest.java` | 验证数据库基线中存在用户与优惠券唯一索引。 |

## 4. 删除文件

无。

Redis Stream Consumer、原 `stream.orders` 链路及其失败处理资源均未删除。当前通过配置开关实现单通道选择，便于灰度观察和故障回退。

## 5. 最终变更范围结论

Kafka 改造新增了消息基础设施、订单事件 DTO、Producer、真实 Consumer、Retry/DLT、独立测试和本地 KRaft 环境；修改范围集中在依赖、Kafka 配置、秒杀入口和 Lua 消息分流。Controller API、实体表结构主体、Mapper 接口和店铺等非秒杀业务没有因 Kafka 切换而改变。

