# Redis Stream 到 Kafka 迁移计划

## 1. 当前代码基线

当前秒杀链路已经具备以下可复用能力：

- `VoucherOrderServiceImpl` 生成订单号并执行 `seckill.lua`。
- `seckill.lua` 原子校验 Redis 库存、一人一单、库存预扣，并通过 `XADD` 写入 `stream.orders`。
- `VoucherOrderStreamConsumer` 使用 Redis Stream Consumer Group、Pending 恢复、ACK、有限重试和失败 Stream。
- `VoucherOrderTransactionalService` 在 Spring 事务中完成数据库库存条件扣减和订单插入。
- 数据库已有 `(user_id, voucher_id)` 唯一索引，负责一人一单最终兜底。

Kafka 迁移应复用现有事务服务与数据库约束，不重写已经正确的订单核心逻辑。

## 2. 目标代码对应关系

以下均为后续实施清单，本设计阶段不修改这些文件。

### 2.1 最终需要删除的 Redis Stream 内容

| 当前内容 | 最终动作 | 删除前条件 |
|---|---|---|
| `src/main/java/com/hmdp/service/impl/VoucherOrderStreamConsumer.java` | 删除 Redis Stream 消费者 | Kafka 消费稳定且 Stream PEL 清零 |
| `src/main/resources/seckill-record-failure.lua` | 删除失败 Stream 写入/ACK 脚本 | 不再有 Redis Stream 消费者 |
| `RedisConstants` 中 Stream key、group、failed stream、retry 等常量 | 删除对应常量 | 代码引用全部移除 |
| `application.yaml` 中 `hmdp.seckill.consumer-name` | 删除配置 | Stream 消费者下线 |
| `VoucherOrderStreamConsumerTest` 等 Stream 专属测试 | 删除或替换为 Kafka 测试 | Kafka 等价测试通过 |
| Redis 中 `stream.orders`、失败 Stream、重试 Hash | 运维清理 | PEL、lag、失败记录完成核对并过回滚观察期 |

生产 Redis key 的删除必须在迁移完成后由明确的运维变更执行，不能随应用首次发布自动删除。

### 2.2 需要新增

建议后续新增的职责组件：

| 组件 | 建议名称 | 职责 |
|---|---|---|
| Kafka 依赖 | `pom.xml` 中 Spring Kafka 依赖 | Producer/Consumer 客户端集成 |
| Producer 配置 | `KafkaProducerConfig` | 序列化、可靠性、发送模板 |
| Consumer 配置 | `KafkaConsumerConfig` | 消费组、手动确认、并发、错误处理 |
| 消息 DTO | `SeckillOrderCreateMessage` | 固定消息协议与版本 |
| Producer | `SeckillOrderKafkaProducer` | 发送、回调、指标、Outbox 清理 |
| Consumer | `SeckillOrderKafkaConsumer` | 校验、事务调用、确认、重试/DLT |
| Outbox 存储 | `SeckillKafkaOutboxRepository` | 写入后的读取、幂等清理 |
| Outbox 恢复 | `SeckillKafkaOutboxRecoveryJob` | 扫描与限流重投 |
| Outbox Lua | 写入整合在 `seckill.lua`；另增清理/抢占脚本 | 保证 Redis 操作原子性 |
| 失败处理 | Retry/DLT 发布与失败订单查询组件 | 有界重试、隔离、运营处置 |
| 可观测性 | 指标、结构化日志、告警规则 | 追踪投递与订单结果 |
| 集成测试 | Kafka/Redis/MySQL Testcontainers 测试 | 验证重启、重复、异常路径 |

### 2.3 需要修改

| 文件/组件 | 后续修改方向 |
|---|---|
| `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java` | 构建消息；Lua 成功后调用 Kafka Producer；接口语义改为“已受理” |
| `src/main/resources/seckill.lua` | 用 Hash + ZSet Outbox 写入替换 `XADD stream.orders` |
| `RedisConstants` | 新增 Outbox 数据、待发送索引、抢占/状态 key；迁移完成后移除 Stream 常量 |
| `application.yaml` | 增加 Kafka broker、Producer、Consumer、Topic 与重试参数；敏感信息走环境配置 |
| `VoucherOrderTransactionalService` | 保留事务边界，建议返回明确业务处理结果以支持幂等确认与异常分类 |
| Controller/API 返回模型 | 明确“已受理”与“已落库”的差异，必要时增加订单状态查询 |
| 部署与监控配置 | Topic 创建、ACL、仪表盘、告警、容量参数 |

现有数据库唯一索引继续保留，不应因 Kafka key 或 Redis 校验而删除。

## 3. 分阶段迁移

### 阶段 0：准备与冻结基线

- 固化当前 Redis Stream 指标：入口成功数、消费成功数、Pending、失败 Stream、数据库订单数。
- 确认唯一索引和事务回滚测试有效。
- 创建 Kafka Topic、ACL、监控和告警。
- 增加消息契约测试、幂等测试、故障注入测试。
- 引入切换配置，例如 `seckill.messaging.mode=redis-stream|shadow-kafka|kafka`。

退出条件：Kafka 环境、可靠性配置、监控和测试环境均就绪；当前 Stream 无未知失败记录。

### 阶段 1：Kafka 影子生产

- Lua 暂时保留当前 Stream 写入，同时原子写入 Kafka Outbox。
- Kafka Producer 投递事件。
- Kafka 影子消费者只校验协议、计数和延迟，不写 MySQL。
- 以 `eventId/orderId` 对比 Redis Stream 与 Kafka 的事件完整性。

此阶段会暂时存在双写，但 MySQL 仍只由 Redis Stream 消费者写入，避免两个主消费者同时执行业务。

退出条件：Kafka 事件数量、字段、延迟和 Outbox 清理达到预定阈值；重启与发送故障演练通过。

### 阶段 2：启用 Kafka 订单消费者

- 先部署处于暂停状态的 Kafka 订单消费者。
- 选择低流量窗口，停止 Redis Stream 消费者获取新消息。
- 处理并核对 Stream PEL，记录切换水位。
- 启用 Kafka 消费者的 MySQL 写入。
- 入口模式切换为 Kafka 主链路。

计划上应只有一个订单写入主消费者。短暂重叠即使被数据库幂等兜底，也不应成为常态，因为会制造额外事务冲突和难以解释的监控噪声。

退出条件：Kafka Consumer lag 稳定、数据库订单正常、幂等与错误率无异常，Stream 不再新增消息。

### 阶段 3：观察与可回滚运行

- 保留 Redis Stream 代码和历史 key，但保持消费者关闭。
- 持续核对 Redis 受理、Kafka 生产、Kafka 消费、MySQL 订单和 DLT。
- 演练 Consumer 重启、Kafka 暂时不可用、数据库短时故障和积压恢复。
- 观察至少一个完整业务周期，实际时长由发布风险策略确定。

退出条件：无未解释丢单、重复落库或长期 Outbox/DLT；回滚窗口结束。

### 阶段 4：移除 Redis Stream

- 将 `seckill.lua` 中 `XADD` 永久替换为 Outbox 写入。
- 删除 Stream Consumer、失败脚本、专属常量、配置和测试。
- 归档并核对 PEL、失败 Stream 与重试记录。
- 经过备份和审批后清理生产 Redis Stream key。
- 更新架构文档、运维手册和开发日志。

退出条件：代码和运行时均不依赖 Redis Stream，Kafka 可靠性验收全部通过。

## 4. 回滚方案

### 4.1 观察期内回滚

1. 停止 Kafka Consumer 拉取新记录并记录各分区 offset。
2. 禁止 Outbox 恢复任务继续无界发送，但保留所有数据。
3. 将入口模式切回 Redis Stream。
4. 恢复 Stream Consumer。
5. 以 `orderId` 对账 Kafka 已处理订单和 Stream 待处理订单；数据库唯一索引兜底重复。

回滚不能删除 Kafka Topic、offset 或 Outbox，以便事后审计。

### 4.2 移除 Stream 后

阶段 4 后若要回滚，需要重新发布保留的上一稳定版本，并恢复兼容脚本与配置，成本明显更高。因此只有在观察期验收完成后才执行物理删除。

## 5. 测试计划

### 5.1 单元与契约测试

- 消息序列化/反序列化和 schema 兼容。
- `userId` key 稳定。
- 异常分类、重试次数、DLT headers。
- Outbox 清理与重复清理幂等。
- 事务服务返回结果与异常映射。

### 5.2 集成测试

- Redis Lua、Kafka、Consumer、MySQL 完整链路。
- 同用户同券并发，只生成一单。
- Producer 确认丢失，重复事件只生成一单。
- 数据库事务回滚后 offset 不提交。
- 数据库提交后 Consumer 终止，恢复后不重复扣库存。
- Retry/DLT 写入失败时来源 offset 不提交。
- Consumer 重启、Rebalance、broker leader 切换与积压恢复。

### 5.3 压测

- 峰值 QPS、持续 QPS 和突发流量三类模型。
- 测量端到端订单创建延迟、Kafka lag、单分区吞吐、数据库 P99 和锁等待。
- 逐级提高消费者并发，找到 MySQL 饱和点，而不是默认拉满 12 个消费者。
- 验证恢复任务与 Retry Topic 回流的限速效果。

## 6. 发布门禁

- 无自动提交 offset。
- Topic 为 3 副本且 ISR 策略符合设计。
- Outbox 恢复、幂等、重试和 DLT 故障演练通过。
- 唯一索引存在并经过并发测试。
- 监控、告警、失败订单负责人和重放手册均已准备。
- Redis Stream PEL 与失败记录完成核对后，才允许最终删除旧链路。
