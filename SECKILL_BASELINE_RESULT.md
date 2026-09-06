# SECKILL BASELINE RESULT

## 修复前问题

- 从 `stream.orders` 消费，却错误 ACK `s1`，消息滞留 Pending。
- 锁失败、库存失败等分支返回后仍可能 ACK，存在静默丢单。
- 数据库扣库存和插订单没有有效事务，可能只扣库存不生成订单。
- `tb_voucher_order` 没有 `(user_id, voucher_id)` 唯一约束。
- 消费异常无限循环，无次数、退避和失败订单记录。
- 消费者组人工初始化，线程没有优雅关闭，日志上下文不足。

## 修复后效果

- ACK 统一指向 `stream.orders/g1`，并要求返回值为 1。
- 只有数据库事务/幂等成功，或失败订单可靠记录后才 ACK。
- 独立事务 Service 保证 MySQL 库存扣减与订单插入同时提交或回滚。
- 新安装 SQL 与增量 migration 均增加用户+优惠券唯一索引。
- 消费失败持久化计数，最多 3 次、每轮 500ms 退避；达到上限后原子写入 `stream.orders.failed` 并 ACK 原消息。
- Stream/group 自动创建；消费者名可通过 `SECKILL_CONSUMER_NAME` 配置。
- 新增关键业务日志、线程优雅关闭和 7 个专项测试。
- 未引入 Kafka，HTTP 接口和 Lua 准入流程保持不变。

## 测试结论

- 专项测试：7/7 通过，覆盖正确 ACK、ACK 失败、事务声明、订单幂等、库存扣减、库存不足和唯一索引位置。
- 全量测试已执行：13 个中 8 个通过；5 个既有 Spring 集成测试因本机 Redis 未启动而无法加载上下文。需在依赖齐全环境补跑。

## 上线前动作

1. 查询并清理重复 `(user_id, voucher_id)` 订单。
2. 执行 `V20260906_01__add_voucher_order_unique_index.sql`。
3. 多实例部署时配置唯一且稳定的 `SECKILL_CONSUMER_NAME`。
4. 在真实 Redis/MySQL 环境验证 Pending 恢复、失败 Stream、事务回滚和库存对账。

## 下一步 Kafka 改造准备

当前事务 Service、业务唯一索引、幂等处理、失败语义和日志上下文都可被 Kafka 消费者复用。Kafka 阶段应重点解决 Redis Lua 准入结果到 Kafka 发布之间的可靠桥接、事件版本、分区键、Retry/DLT、指标和对账，而不是重写数据库正确性逻辑。

