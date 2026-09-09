# Kafka 秒杀 Retry/DLT 实现记录

## 1. 实现结果

Kafka Consumer 已从“原分区无限重试”调整为“主 Topic 失败转 Retry、Retry 有限重试、耗尽进 DLT”。改造未触及 Redis Stream、秒杀入口、Lua、Kafka Producer 和消息模式。

## 2. 配置实现

`KafkaConsumerConfig` 增加三套监听容器职责：

- `voucherOrderKafkaListenerContainerFactory`：主 Topic，第一次失败即转发 Retry Topic。
- `voucherOrderRetryKafkaListenerContainerFactory`：Retry Topic，`FixedBackOff(1000ms, 1)`，即初次失败后再尝试一次，仍失败转发 DLT。
- `voucherOrderDltKafkaListenerContainerFactory`：DLT 日志消费者，手动确认。

主、Retry 错误处理器均开启：

```text
AckMode = MANUAL_IMMEDIATE
commitRecovered = true
ackAfterHandle = false
```

业务成功由监听器手动确认；失败转发成功由错误处理器提交源 offset。监听器自身永远不会在异常路径调用 ACK。

## 3. 可靠转发

新增 `ReliableDeadLetterPublishingRecoverer`，继承 Spring Kafka 2.5 的 `DeadLetterPublishingRecoverer`。它最多等待 10 秒获取 Kafka broker 发送结果：

- 发送成功：错误处理器可以提交源 offset。
- 发送异常：抛出异常，源 offset 不提交。
- 等待超时：抛出异常，源 offset 不提交。
- 线程中断：恢复中断标记并抛出异常。

这消除了默认异步转发“发送结果尚未确认，源记录却可能被视为已恢复”的窗口。

## 4. Consumer 实现

`VoucherOrderKafkaConsumer` 新增 Retry Topic 监听方法 `listenRetry()`，并与主监听方法共同复用以下消费逻辑：

1. 校验 `orderId/userId/voucherId/createTime`。
2. 转换为 `VoucherOrder`。
3. 调用 `VoucherOrderTransactionalService.createVoucherOrder()`。
4. 数据库事务成功或事务服务判定幂等成功后手动 ACK。
5. 任意运行时异常均记录 Kafka 元数据并继续抛出。

## 5. DLT 实现

新增 `VoucherOrderDltConsumer`，监听 `hmdp.seckill.order.dlt.v1`，记录：

- `orderId`
- `userId`
- `voucherId`
- Spring Kafka DLT 异常消息头
- 失败记录时间
- Topic、partition、offset

日志记录完成后手动确认 DLT offset。当前阶段不自动补单、不自动重放，避免永久性业务错误被再次放大。

## 6. 配置项

```yaml
hmdp:
  kafka:
    topics:
      voucher-order: hmdp.seckill.order.create.v1
      voucher-order-retry: hmdp.seckill.order.retry.v1
      voucher-order-dlt: hmdp.seckill.order.dlt.v1
    consumer:
      retry-group-id: hmdp-seckill-order-retry-v1
      dlt-group-id: hmdp-seckill-order-dlt-log-v1
```

可分别通过下列环境变量覆盖：

- `KAFKA_VOUCHER_ORDER_TOPIC`
- `KAFKA_VOUCHER_ORDER_RETRY_TOPIC`
- `KAFKA_VOUCHER_ORDER_DLT_TOPIC`
- `KAFKA_VOUCHER_ORDER_RETRY_GROUP`
- `KAFKA_VOUCHER_ORDER_DLT_GROUP`

## 7. 文件清单

### 新增

- `src/main/java/com/hmdp/config/kafka/ReliableDeadLetterPublishingRecoverer.java`
- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumer.java`
- `src/test/java/com/hmdp/config/kafka/KafkaConsumerRetryRoutingTest.java`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumerTest.java`
- `docs/kafka-seckill/11_kafka_retry_dlt_design.md`
- `docs/kafka-seckill/12_kafka_retry_dlt_implementation.md`
- `docs/kafka-seckill/13_kafka_retry_test.md`
- `docs/kafka-seckill/14_kafka_retry_interview.md`

### 修改

- `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java`
- `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java`
- `src/main/resources/application.yaml`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java`
- `docs/kafka-seckill/development_log.md`

## 8. 部署前置条件

部署前必须预创建三个 Topic，并确保分区数量一致、复制因子符合环境容灾要求。应用账号必须具备主 Topic 消费、Retry/DLT 生产与消费权限。Kafka Producer 业务代码未改，但错误恢复器复用了现有 `KafkaTemplate` 和现有 `acks=all` 配置。

