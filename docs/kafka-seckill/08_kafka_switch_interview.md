# Kafka 秒杀正式切换面试回答

## 1. 为什么选择 Kafka？

秒杀流量具有瞬时峰值，数据库无法按入口 QPS 同步写入。Kafka 用持久化日志承接流量，让入口快速完成 Redis 资格判断后返回，再由 Consumer Group 按数据库可承受的速率异步创建订单。

选择 Kafka 的主要原因：

- 分区并行和高吞吐适合秒杀削峰。
- 多副本、ACK、ISR 提供明确的消息持久化保障。
- Consumer Group 便于水平扩展。
- offset 和消息保留支持重启恢复、故障重放和对账。
- 后续可让风控、通知、统计等消费者独立订阅，降低系统耦合。

Kafka 不是为了省略数据库事务，也不能自动保证端到端只执行一次。本方案仍需要 Redis Outbox、手动 offset、数据库唯一索引和本地事务。

## 2. 为什么不用 Redis Stream？

Redis Stream 并非不可用。当前项目已经实现 Consumer Group、Pending 恢复、正确 ACK、有限重试和失败 Stream，在中小规模场景中完全可以工作。

正式迁移 Kafka 是基于职责和扩展性：

- Redis 继续专注缓存、库存预扣和一人一单资格判断，避免缓存与消息积压竞争内存。
- Kafka 以磁盘日志和副本机制承载更长时间、更大规模的积压。
- Kafka 的分区扩展、消费组、消息重放、跨服务订阅和运维工具更成熟。
- 秒杀订单未来可能被多个独立业务消费，Kafka 更适合作为事件总线。

如果业务量较小、没有多消费者需求、团队也不具备 Kafka 运维能力，保留加固后的 Redis Stream 反而更简单。技术选型应由容量与演进需求决定，而不是认为 Kafka 天然比 Redis Stream 更正确。

## 3. Kafka 如何保证可靠性？

需要分四段回答。

### Redis 到 Producer

Redis Lua 和 Kafka 不在同一事务中。如果 Lua 扣完库存后应用宕机，简单调用一次 Producer 会丢失订单。因此 Lua 在预扣库存时原子写 Redis Outbox；broker 确认成功后清理，失败或结果未知时恢复任务重投相同订单事件。

### Producer 到 broker

- `acks=all`。
- 启用幂等 Producer。
- Topic 使用 3 副本、`min.insync.replicas=2`。
- ISR 不足时让发送失败并等待重投，不降级为单副本确认。
- 监控 Producer 错误、ISR 缩减和 Outbox 最老记录。

### broker 到 Consumer

- 关闭自动提交。
- 订单事务成功或确认是幂等重复后，手动提交 offset。
- 业务失败不提交；进入 Retry/DLT 时，先确认目标 Topic 写入成功，再提交来源 offset。

### Consumer 到 MySQL

数据库库存条件扣减和订单插入在同一个本地事务中。系统采用至少一次投递，允许消息重复，通过主键和唯一索引实现业务结果等效一次。

## 4. 如何避免重复订单？

使用多层幂等防线：

1. `orderId` 是订单主键，同一事件重投不能重复插入。
2. `(user_id, voucher_id)` 是数据库唯一索引，即使生成了不同订单号，同一用户也不能重复购买同一券。
3. Redis `seckill:order:{voucherId}` 集合在入口快速拒绝重复请求。
4. Kafka 使用 `userId` 作为 key，使同一用户事件落在同一分区并保持顺序。
5. 数据库库存扣减和订单插入同事务，唯一约束冲突会回滚本次库存扣减。

Kafka key 只能提供分区顺序，不是幂等保证；应用层先查询也无法替代数据库唯一索引。

## 5. 为什么不能在 Kafka 发送失败时立刻回补库存？

Producer 超时只代表客户端没有拿到明确结果，不代表 broker 一定没有写入。如果此时立刻恢复 Redis 库存，而 Kafka 实际已经存在订单事件，后续 Consumer 仍会创建订单，可能导致超卖。

正确做法是保留 Outbox并用相同 `orderId` 重投。只有事件进入最终失败状态、确认不会继续自动消费、数据库不存在订单后，才执行幂等库存补偿。

## 6. MySQL 已提交但 offset 提交失败怎么办？

Kafka 会再次投递该消息，这是至少一次消费的正常情况。Consumer 再次调用事务服务时会命中订单主键或 `(user_id, voucher_id)` 唯一约束，将其识别为幂等成功，然后重新提交 offset。最终数据库只保留一张订单，也只扣减一次库存。

## 7. 灰度时为什么不能直接删除 Redis Stream？

切流瞬间可能仍有：

- `stream.orders` 中尚未消费的新消息。
- Consumer Group Pending 中已投递但未 ACK 的消息。
- 失败 Stream 中等待处理的订单。
- 已进入请求线程但尚未执行 Lua 的 Redis 模式请求。

因此 `seckill.message.mode` 只控制新请求的投递路径，Stream Consumer 需要独立开关继续排空历史数据。正确顺序是先完成 Kafka 能力、再小流量灰度、再全量生产、再排空 Stream、最后单独发布删除旧代码。

## 8. 当前切换最大的风险是什么？

当前 Phase 1 Kafka Consumer 只打印消息，配置为监听方法成功后按记录确认。如果先把 Producer 接入真实秒杀流量，这个 Consumer 会把真实订单消息打印后提交 offset，却不创建数据库订单。

所以正式切换的 P0 门禁是：先将 Kafka Consumer 升级为调用 `VoucherOrderTransactionalService` 的正式实现，并完成手动确认、幂等和失败重试测试；在此之前保持 Kafka 业务消费者关闭。

## 9. 如何回答整体切换方案？

可以用下面这段话概括：

> 我们先保留现有 Redis Stream 链路，以 `seckill.message.mode` 控制新订单进入 Redis 还是 Kafka。Kafka 模式下，Lua 在预扣库存和记录一人一单的同时原子写 Redis Outbox，Producer 获得 broker 确认后清理；Consumer 手动提交 offset，并调用原有 MySQL 事务服务。重复消息由订单主键、用户与优惠券唯一索引以及事务回滚兜底。灰度时先启用正式 Kafka Consumer，再切少量入口流量，最后全量切 Kafka并排空 Stream；观察期结束后才删除 Redis Stream 代码。
