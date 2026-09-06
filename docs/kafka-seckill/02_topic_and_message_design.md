# Kafka Topic、Producer 与消息设计

## 1. Topic 规划

### 1.1 Topic 清单

| 用途 | Topic 名称 | 初始分区 | 副本因子 | 建议保留期 |
|---|---|---:|---:|---:|
| 创建秒杀订单主事件 | `hmdp.seckill.order.create.v1` | 12 | 3 | 3 天 |
| 第一级延迟重试 | `hmdp.seckill.order.create.retry.1.v1` | 12 | 3 | 3 天 |
| 第二级延迟重试 | `hmdp.seckill.order.create.retry.2.v1` | 12 | 3 | 3 天 |
| 最终失败事件 | `hmdp.seckill.order.create.dlt.v1` | 12 | 3 | 14 天或按审计要求调整 |

重试 Topic 用于把暂时失败的消息从主分区移开，避免一条故障消息长期阻塞同一分区。延迟可从 5 秒、30 秒、5 分钟等起始值中按实现复杂度选取；最终值必须通过故障演练确认。

### 1.2 命名规则

Topic 使用 `<系统>.<领域>.<动作>.<版本>`：

- `hmdp`：系统边界。
- `seckill.order`：秒杀订单领域。
- `create`：事件意图。
- `v1`：协议大版本，不兼容变更必须创建新版本 Topic 或采用受控双读。

### 1.3 分区数量为什么初始选择 12

12 是初始生产基线，原因是：

- 可提供最多 12 个同组有效消费并发，足以支持小规模水平扩展。
- 相比过少分区，能更好地将热门优惠券下不同用户的订单分散处理。
- 相比一次创建过多分区，能控制 broker 文件句柄、复制流量和 rebalance 成本。

上线前需根据下式校验：

```text
所需分区数 >= 峰值入口成功 QPS / 单分区实测稳定消费 QPS
```

最终还需满足消费者总数据库写入吞吐不超过 MySQL 可承受能力。Kafka 扩大并发不能解决数据库瓶颈。

增加分区后，同一个 key 对应的分区映射可能变化，因此扩容切换期间不能假设跨旧、新分区仍具备严格顺序。应尽量提前规划分区，并在扩容时依靠数据库幂等保证正确性。

## 2. 消息 key 设计

### 2.1 选择 `userId`

主 Topic 和重试 Topic均使用字符串形式的 `userId` 作为 key。

选择原因：

1. 同一用户的秒杀订单事件进入同一分区，保持用户维度的顺序。
2. 秒杀热点集中在少数 `voucherId`。若使用 `voucherId` 作为 key，一个热门券的全部事件会落入单分区，形成明显热点，失去多分区并行处理能力。
3. 核心业务约束是“同一用户对同一优惠券只能下一单”。用户维度串行能降低同一用户并发请求造成的竞争。
4. 优惠券总库存不要求消息按券严格有序；数据库使用条件更新 `stock > 0` 保证不会超卖。

取舍：同一高频用户的事件会串行，但正常用户的秒杀频率有限。`userId` 只负责分区和顺序，不是幂等键；幂等仍依赖 `orderId` 和数据库唯一索引。

### 2.2 key 的稳定性

- 必须使用统一编码后的十进制字符串，不能在不同 Producer 中混用 Long 与 JSON key 序列化。
- 重试 Topic 重投时必须保留原始 `userId` key。
- `userId` 不得为空；空 key 会轮询分区并破坏用户维度顺序。

## 3. 消息格式

建议新增消息 DTO：`SeckillOrderCreateMessage`。

```json
{
  "schemaVersion": 1,
  "eventId": "310847248731553793",
  "orderId": 310847248731553793,
  "userId": 1024,
  "voucherId": 88,
  "occurredAt": "2026-09-06T10:15:30.123Z",
  "traceId": "89c1c1b725744f599327aa3f31d58e7c",
  "source": "seckill-api"
}
```

字段定义：

| 字段 | 必填 | 说明 |
|---|---|---|
| `schemaVersion` | 是 | 消息协议版本，初始为 `1` |
| `eventId` | 是 | 事件唯一标识；本场景可与 `orderId` 使用同一全局唯一值 |
| `orderId` | 是 | 订单主键，也是跨重投稳定的核心幂等标识 |
| `userId` | 是 | 用户标识，同时作为 Kafka key |
| `voucherId` | 是 | 秒杀优惠券标识 |
| `occurredAt` | 是 | Redis 受理事件的 UTC 时间，ISO-8601 格式 |
| `traceId` | 是 | 全链路追踪标识 |
| `source` | 是 | 事件来源，固定为 `seckill-api` |

消息不包含手机号、昵称等个人信息，也不携带可从业务主键稳定查询的冗余对象。

### 3.1 Header 设计

建议使用 Kafka headers 传输运行元数据：

- `eventType=SeckillOrderCreate`
- `schemaVersion=1`
- `retryCount=0`
- `originalTopic`、`originalPartition`、`originalOffset`：进入重试或 DLT 时记录来源。
- `failureClass`：进入 DLT 时记录异常分类，不写敏感堆栈。

业务真相字段放在消息 body 中；header 主要用于路由、重试和诊断。

### 3.2 兼容性规则

- 新增可选字段必须提供默认行为，消费者应忽略未知字段。
- 不直接删除、重命名或改变现有字段语义。
- 不兼容变更提升大版本，并采用新 Topic 或灰度双读。
- 同一 `eventId/orderId` 重投时 body 的业务字段必须完全一致；发现同 ID 不同内容时进入 DLT，不能覆盖订单。

## 4. Producer 设计

### 4.1 发送时机

1. Controller 调用现有 `VoucherOrderServiceImpl.seckillVoucher()`。
2. 服务生成全局唯一 `orderId`，构建不可变消息。
3. Lua 原子完成资格校验、Redis 库存预扣、一人一单标记和 Outbox 写入。
4. Lua 返回成功后，Producer 立即异步发送消息。
5. 收到 broker 成功确认后，幂等清理对应 Outbox 记录。
6. API 返回订单号和“已受理”状态；发送尚未确认时由 Outbox 负责恢复。

Lua 返回库存不足或重复购买时不得发送 Kafka 消息。

### 4.2 Producer 关键配置

| 配置 | 建议 | 目的 |
|---|---|---|
| `acks` | `all` | 等待所有 ISR 副本确认 |
| `enable.idempotence` | `true` | 抑制 Producer 网络重试产生的部分重复 |
| `retries` | 足够大，由 `delivery.timeout.ms` 限制总时长 | 应对暂时网络或 leader 切换 |
| `max.in.flight.requests.per.connection` | 不大于 5 | 满足幂等 Producer 要求并控制乱序风险 |
| `delivery.timeout.ms` | 明确设置 | 为业务定义一次发送等待上限 |
| `compression.type` | `lz4` 或 `zstd`，压测后选定 | 降低网络与存储压力 |

Topic 同时建议配置 `replication.factor=3`、`min.insync.replicas=2`，并禁止非同步副本强行选主。配置名称与可用值应以最终选定 Kafka 版本为准。

### 4.3 发送结果处理

成功回调：

- 记录 `eventId/orderId/topic/partition/offset/latency`。
- 执行 Outbox 幂等清理。
- 清理失败不影响 Kafka 已成功的事实，由恢复任务重投并依赖消费幂等。

失败回调：

- 记录结构化错误、指标和 traceId。
- 不删除 Outbox，不立即回补 Redis 库存。
- 对可重试异常可由 Kafka 客户端内置重试先处理，超出交付时间后留给 Outbox 恢复任务。
- 序列化错误、消息超限等确定性错误应告警并标记人工处理，盲目重试不会恢复。

结果未知：

- 网络超时可能发生在 broker 已写入但确认未返回之后。
- 必须使用相同 `eventId/orderId` 重投，接受 Kafka 中可能出现重复事件。
- 不能通过回滚 Redis 库存解决，因为回滚可能与已经存在的 Kafka 消息并发，导致超卖或重复资格。

### 4.4 Outbox 恢复任务

- 定时扫描 `seckill:kafka:outbox:pending` 中超过发送确认阈值的记录。
- 根据 `orderId` 从 Hash 读取完整消息，使用相同 key 和 payload 重投。
- 多实例恢复任务通过短租约/抢占标记避免无意义的并发风暴；即使重复抢占，消费者幂等仍是最终防线。
- 使用指数退避与抖动，设置单轮最大扫描量，避免 Kafka 恢复时瞬间重放压垮系统。
- 对长期无法发送的记录保留并高优先级告警，不静默删除。

## 5. Topic 运维要求

- Topic 由部署脚本或基础设施配置预创建；生产环境不依赖自动创建。
- 变更分区、副本、保留期必须评审并记录。
- 对主 Topic、重试 Topic、DLT 分别监控写入速率、失败率、消费延迟和磁盘占用。
- ACL 最小化：API 服务只写主 Topic；订单消费者读主 Topic并按需要写重试/DLT；人工补偿工具只授予受控权限。
