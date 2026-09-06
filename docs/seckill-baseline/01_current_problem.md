# 秒杀模块现状问题

## 1. 审计范围

本次加固保持现有主流程不变：

```text
HTTP 秒杀请求
  ↓
Redis Lua 原子准入与预扣库存
  ↓
XADD stream.orders
  ↓
Redis Stream 消费者异步落库
  ↓
MySQL 扣库存并创建订单
```

未引入 Kafka 或其他消息中间件。

## 2. ACK 确认问题

### 修复前流程

消费者从 `stream.orders` 读取消息，但正常消费和 Pending List 补偿分支都执行：

```java
stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
```

### 存在的问题

- 读取 Stream 是 `stream.orders`，ACK 却使用 `s1`，消息不会从正确消费者组的 Pending List 移除。
- 数据库方法中的锁失败、库存失败只记录日志并返回，外层仍继续 ACK，可能静默丢单。
- ACK 返回值没有校验；Redis 返回 0 时应用仍视为成功。
- Pending 处理发生异常后没有最大重试次数和退避，毒消息可能形成死循环。
- 消费者组依赖人工命令创建，组缺失时会持续报错。

### 正确语义

只有以下两种情况允许 ACK 原消息：

1. 数据库事务成功，或消息被识别为已存在订单的幂等成功。
2. 达到最大重试次数，并且失败订单已被可靠写入 `stream.orders.failed`。

其他任何情况都保留在 Pending List，等待重试。

## 3. 订单事务问题

### 修复前

`VoucherOrderServiceImpl` 的异步私有方法依次执行：

1. 查询用户是否已经购买。
2. 条件扣减 `tb_seckill_voucher.stock`。
3. 插入 `tb_voucher_order`。

该私有方法没有有效的 Spring 事务边界。即便直接在私有方法上增加 `@Transactional`，同类内部调用和私有方法也不会经过 Spring AOP 代理。

### 风险

```text
库存 UPDATE 已提交
  ↓
订单 INSERT 异常
  ↓
数据库库存减少，但订单不存在
```

Redis 分布式锁只能控制并发，不能替代数据库事务。

## 4. 用户重复购买问题

### 修复前

应用先查询 `(user_id, voucher_id)` 是否存在，再插入订单。订单表只有主键 `id`，没有业务唯一约束。

### 风险

- “先查再插”存在并发窗口。
- Redis Stream 属于至少一次消费，ACK 失败或进程重启会产生重复处理。
- Redis 资格 Set 是入口优化，不是数据库最终约束。

最终防线必须是数据库唯一索引：

```sql
UNIQUE (user_id, voucher_id)
```

## 5. 消费失败问题

### 修复前

- 所有异常进入无限 Pending 循环。
- 没有失败次数。
- 没有退避时间。
- 没有失败订单记录、最大重试或人工处理入口。
- 失败原因只存在应用日志，无法按订单和原消息 ID 稳定追踪。

### 业务后果

- 单线程消费者可能被一条毒消息永久阻塞。
- 频繁重试产生大量日志与 Redis/MySQL 压力。
- 运维无法区分临时故障、永久数据问题和数据库库存不一致。

## 6. 可运维性问题

- 消费者名固定为 `c1`，没有部署配置说明。
- ExecutorService 没有显式关闭流程。
- Stream/group 不会自动初始化。
- 关键日志缺少 `recordId/orderId/userId/voucherId/retryCount` 的统一上下文。
- 无失败 Stream 处理手册和数据库迁移脚本。

## 7. 本次不扩大的边界

以下问题需要后续独立迭代，不在本次最小加固中改变：

- Redis 预扣成功而 MySQL 最终失败后的自动库存/资格补偿；本次先通过失败 Stream 留痕，避免静默丢失。
- 秒杀开始/结束时间在 Lua 中的原子校验。
- 跨消费者自动认领长期闲置 Pending 消息；当前要求消费者名稳定，必要时由运维执行 `XAUTOCLAIM/XCLAIM`。
- 消费吞吐仍为单线程；应先压测再决定分片/并发。
- Kafka 改造。

