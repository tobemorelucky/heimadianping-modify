# Kafka 秒杀正式业务切换

## 1. 切换结论

秒杀订单消息的默认通道已由 Redis Stream 切换为 Kafka，同时保留 Redis Stream 代码和 `redis` 配置值作为灰度回退手段。改造没有改变 Controller 接口、Redis Lua 准入规则、订单号生成、数据库事务或消费者可靠性机制。

默认配置：

```yaml
seckill:
  message:
    mode: ${SECKILL_MESSAGE_MODE:kafka}
```

- 未设置环境变量时使用 `kafka`。
- 设置 `SECKILL_MESSAGE_MODE=redis` 后恢复 Redis Stream 投递。
- 其他值会在执行 Lua 前被拒绝，避免已经预扣库存但没有可用消息通道。

## 2. 切换后的流程

```text
用户秒杀请求
    ↓
VoucherOrderServiceImpl 生成 orderId
    ↓
seckill.lua 原子执行：库存校验 + 一人一单 + Redis 库存预扣
    ↓
根据 seckill.message.mode 分流
    ├── kafka：VoucherOrderKafkaProducer
    │               ↓
    │   hmdp.seckill.order.create.v1
    │               ↓
    │   VoucherOrderKafkaConsumer
    │               ↓
    │   VoucherOrderTransactionalService
    │               ↓
    │   MySQL 库存扣减 + 订单插入（同一事务）
    │               ↓
    │   手动 ACK
    │
    └── redis：Lua XADD stream.orders
                    ↓
        VoucherOrderStreamConsumer
                    ↓
        同一个 VoucherOrderTransactionalService
```

## 3. 关键实现

### 3.1 秒杀入口

`VoucherOrderServiceImpl.seckillVoucher()` 将消息模式作为 Lua 的第四个参数。Lua 返回成功后：

- Kafka 模式构造 `VoucherOrderMessage(orderId, userId, voucherId, createTime)`；
- 调用 `VoucherOrderKafkaProducer.sendOrderMessage()`；
- 最多等待 10 秒取得 broker 发送结果，再向用户返回已受理订单号；
- Redis 模式不调用 Kafka Producer，因为 Lua 已原子写入 `stream.orders`。

### 3.2 Lua 分流

Lua 始终保留以下原子操作：

1. 校验 Redis 库存；
2. 使用 Set 校验一人一单；
3. 预扣 Redis 库存；
4. 记录用户已购买标记。

只有 `messageMode == 'redis'` 时才执行 `XADD`。Kafka 模式由 Java 在 Lua 成功后投递 Kafka，因此不会同时写入两个消息通道。

### 3.3 Kafka Consumer 与事务

现有 `VoucherOrderKafkaConsumer` 已满足正式链路要求：

- 校验消息字段；
- 转换为 `VoucherOrder`；
- 调用 `VoucherOrderTransactionalService.createVoucherOrder()`；
- 事务服务成功返回后才手动 ACK；
- 失败不 ACK，并进入既有有限重试与 DLT 流程。

事务服务继续使用 `(user_id, voucher_id)` 查询及数据库唯一索引防重，并在一个数据库事务中完成条件扣库存和订单插入。

## 4. 改动文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java`
- `src/main/resources/seckill.lua`
- `src/main/resources/application.yaml`
- `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java`（更新阶段说明）
- `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java`（更新消费者无关说明）
- `src/test/java/com/hmdp/service/impl/VoucherOrderServiceImplMessageModeTest.java`
- `src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java`
- `docs/kafka-seckill/16_kafka_business_switch.md`
- `docs/kafka-seckill/17_kafka_end_to_end_test.md`
- `docs/kafka-seckill/development_log.md`

没有删除或停用 `VoucherOrderStreamConsumer`。

## 5. 灰度与回退

建议切换步骤：

1. 确认 Kafka 主 Topic、Retry Topic、DLT 及消费者均正常；
2. 默认以 `kafka` 启动一个实例并观察生产、消费和 DLT 指标；
3. 稳定后逐步扩大 Kafka 实例比例；
4. 需要回退时设置 `SECKILL_MESSAGE_MODE=redis` 并重启实例；
5. 回退观察期内保留 Kafka 与 Redis Stream Consumer，处理切换前已经进入各自通道的消息。

同一个实例在一次请求内只选择一个消息通道，不进行双写。

## 6. 当前边界

Redis Lua 与 Kafka 属于两个独立系统，直接投递无法形成跨系统原子事务。当前简单实现使用 `acks=all`、Producer 重试并同步等待 broker 结果，显著缩小“Redis 已预扣但 Kafka 未确认”的窗口，但不能在网络结果不确定时提供绝对原子性。

本阶段按“避免复杂机制”的要求没有新增 Outbox、Kafka 事务或补偿任务。上线时必须监控 Kafka 发送失败日志；若后续要求进一步消除该窗口，可再引入轻量 Outbox/补偿扫描，而不是在本阶段扩大改造范围。

## 7. 面试说明

可以这样说明本次切换：秒杀准入仍由 Lua 承担，因为库存判断和一人一单需要 Redis 原子性；Lua 成功后以 `userId` 为 key 投递 Kafka，实现削峰和同一用户消息的分区有序；Consumer 只在数据库本地事务完成后手动 ACK，重复消息由业务查询、唯一索引和订单主键共同兜底。Redis Stream 没有直接删除，而是通过配置开关保留可回退路径，避免一次性切换风险。

