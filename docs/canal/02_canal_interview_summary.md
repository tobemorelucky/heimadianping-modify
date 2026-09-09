# Canal 缓存一致性秋招面试总结

## 1. 为什么需要 Canal？

### 推荐回答

> 项目原来在更新店铺数据库后由业务代码删除 `cache:shop:{id}`。这种方式只能覆盖走该方法的更新，而且 MySQL 事务和 Redis 删除无法原子提交：Redis 删除失败、并发读回填旧值、直接 SQL 改库都会造成旧缓存。Canal 订阅 MySQL 提交后的 binlog，无论变更来自哪个应用或脚本，都能产生店铺变更事件，再由业务 Cache Invalidator 幂等删除 Redis key，实现可恢复的最终一致。

### 项目落点

- 监听 `tb_shop` 的 INSERT、UPDATE、DELETE。
- INSERT 删除可能存在的空值缓存。
- UPDATE 删除详情正缓存。
- DELETE 删除残留缓存。
- Redis 删除成功后 ACK Canal batch。
- 删除失败 rollback，恢复后重新消费。
- 下一次读请求继续使用现有 CacheClient 从 MySQL 回源。

### 边界

Canal 不是分布式事务，也不能保证 MySQL 与 Redis 在同一时刻强一致。它提供的是基于提交日志、可重放的最终一致性基础。

## 2. 为什么不用代码双删？

### 什么是代码双删

常见方案是：

```text
删除缓存
  ↓
更新数据库
  ↓
等待一段固定时间
  ↓
再次删除缓存
```

第二次删除试图清理并发读在更新窗口中回填的旧值。

### 推荐回答

> 延迟双删可以缩小部分并发窗口，但它依赖业务代码路径和人为选择的延迟时间。等待多久很难准确：太短覆盖不了慢查询，太长扩大不一致窗口；进程在第二次删除前崩溃，任务就丢了；直接 SQL 或其他服务改库也不会执行双删。Canal 事件来自 MySQL 提交后的 binlog，有持久化位点并支持未确认事件重放，因此覆盖面和可恢复性更好。

### 为什么仍保留应用内删除

Canal 有传输和消费延迟。推荐组合不是“只用 Canal”，而是：

```text
事务提交后本地快速删除
          +
Canal 提交日志独立兜底删除
```

两次 `DEL` 是幂等的。应用内删除降低正常写后的可见延迟，Canal 覆盖删除失败、绕过应用写入和服务崩溃，不需要固定 sleep。

## 3. binlog 是什么？

### 推荐回答

> binlog 是 MySQL Server 层记录数据变更的二进制日志，主要用于主从复制和时间点恢复。事务提交后，INSERT、UPDATE、DELETE 会按配置写入 binlog。Canal 模拟 MySQL replica 的复制协议，从主库获取 binlog 并解析为业务可以消费的行变更事件。

### 三种格式

| 格式 | 记录内容 | 对 CDC 的影响 |
| --- | --- | --- |
| `STATEMENT` | 原始 SQL | 解析业务行变化困难，受函数和执行环境影响。 |
| `ROW` | 每一行的 before/after 变化 | 最适合 Canal 这类 CDC。 |
| `MIXED` | MySQL 在两者间选择 | 行事件语义不如统一 ROW 清晰。 |

本项目应使用 ROW，并确保行镜像足够完整，以便 DELETE 读取旧 ID、UPDATE 读取新旧列。

### binlog 与事务日志的区别

- binlog 是 MySQL Server 层的逻辑变更日志，可被复制/CDC 使用。
- redo log 是 InnoDB 存储引擎层的物理恢复日志，主要保证崩溃恢复和持久性。
- undo log 保存旧版本，用于事务回滚和 MVCC。

不要把 Canal 描述为直接解析 redo log。

## 4. Canal 原理是什么？

### 推荐回答

> Canal Server 模拟一个 MySQL replica。它向 MySQL 主库注册复制身份，请求从指定 binlog 文件和 position 开始同步；MySQL dump 线程把日志发送给 Canal；Canal 解析 binlog，按 destination 和 filter 暴露 Entry。Java Consumer 拉取 batch，解析 `tb_shop` 的行事件，删除对应 Redis key，成功后 ACK，失败则 rollback，之后可以重放。

### 流程图

```text
MySQL transaction commit
          ↓
       ROW binlog
          ↓ replication protocol
      Canal Server
          ↓ Entry / batch
      Canal Consumer
          ↓ shopId
   Cache Invalidator
          ↓
DEL cache:shop:{shopId}
          ↓
ACK batch
```

### Canal Server 与业务 Consumer 的分工

- Canal Server：连接 MySQL、拉取并解析 binlog、维护 destination 和消费位置。
- 业务 Consumer：理解 `tb_shop`、提取 ID、删除缓存、决定 ACK/rollback、记录业务指标。

## 5. 如何保证事件不丢？

### 推荐回答

> Consumer 使用 `getWithoutAck` 拉取批次，不能收到消息就 ACK。只有目标 batch 中的 Redis 缓存全部删除成功后才 ACK；如果删除失败或进程异常，就 rollback 或保持未确认，重连后重新消费。MySQL binlog 保留时间要覆盖最长故障窗口，Canal 位点也必须持久化。

关键检查：

- 先处理，后 ACK。
- ACK 前崩溃会重复消费，但不能丢事件。
- Redis 删除失败不推进位点。
- 监控最后成功位点和事件延迟。
- binlog 不能在消费者恢复前被 MySQL 清理。

## 6. 如何处理重复事件？

### 推荐回答

> Canal 链路采用至少一次处理语义更稳妥，因此 ACK 响应丢失或处理后崩溃可能导致重复事件。缓存失效使用 Redis `DEL`，删除一个已经不存在的 key 仍然可以视为成功，天然幂等；同一 batch 中还可以先对 shopId 去重。因此不需要为了详情缓存删除额外建设复杂幂等表。

如果未来同一个事件用于发送通知、扣款或其他非幂等操作，就必须使用事件 ID 或 binlog 位点做持久化幂等，不能照搬 `DEL` 的结论。

## 7. Redis 删除失败怎么办？

### 推荐回答

> Redis 删除失败时不 ACK 当前 Canal batch，记录 batchId、shopId、binlog file/position 和异常，执行 rollback 并退避重试。Redis 恢复后重新执行同一批 `DEL`。连续失败需要告警，但不能为了让消费位点继续前进而静默跳过，否则旧缓存会保留到 TTL 到期。

## 8. Canal 断开怎么办？

### 推荐回答

> Consumer 记录最后一次成功 ACK 的批次和事件时间，关闭失效连接后按有上限的退避策略重连并重新订阅。恢复后从 Canal 已确认位点继续拉取。除了监控连接状态，还要监控消费延迟，因为 TCP 连接正常并不代表 binlog 在持续推进。

## 9. 为什么删除缓存而不是更新缓存？

### 推荐回答

> 删除缓存更简单且幂等，Consumer 只需要 shopId，不需要复制 Shop JSON 序列化、TTL 和字段转换规则。下一次读请求会从权威 MySQL 读取并通过原 CacheClient 重建；冷数据更新后也不会被强制加载到 Redis。直接更新缓存容易因字段镜像不完整或转换差异写入错误数据。

代价是删除后的第一次查询会回源 MySQL，热点数据需要再结合互斥锁或逻辑过期策略控制击穿。本项目当前详情链路启用的是 pass-through，互斥锁和逻辑过期实现存在但未启用。

## 10. 当前项目存在哪些一致性窗口？

可以结合代码回答：

1. `ShopServiceImpl.update` 在数据库事务内先更新、再删 Redis、最后提交，并发读可能在提交前回填旧值。
2. Redis 删除失败时没有可恢复的重试来源，正缓存可能旧 30 分钟。
3. 直接 SQL 或其他服务更新不会触发 Java 删除逻辑。
4. 店铺新增前如果已缓存空值，新增后可能继续返回不存在，最长 2 分钟。
5. 当前 Canal 尚未实现，本阶段只是完成设计，不能把目标架构描述成已经上线。

## 11. 一分钟项目回答

> 黑马点评的店铺详情使用 `cache:shop:{id}`，正缓存 30 分钟，空值缓存 2 分钟。原更新逻辑在 MySQL 更新后直接删 Redis，但 MySQL 和 Redis 不能原子提交，而且直接改库不会经过这段代码。我设计使用 Canal 监听 `tb_shop` 的 ROW binlog，业务 Consumer 用 `getWithoutAck` 拉取事件，提取 shopId 后调用统一 Cache Invalidator 删除 Redis；全部成功才 ACK，失败 rollback 重试。重复事件通过幂等 DEL 处理。应用内仍保留事务提交后的快速删除，Canal 负责提交日志级兜底，下一次读请求由现有 CacheClient 从 MySQL 重新加载。这个方案实现最终一致，不是分布式强一致。
