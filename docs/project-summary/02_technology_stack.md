# HM-DianPing Plus 技术栈

## 1. 基础技术栈

| 技术 | 当前版本/形态 | 用途 | 解决的问题 |
| --- | --- | --- | --- |
| Java | Java 8 编译目标 | 项目主要开发语言 | 提供稳定的服务端运行基础 |
| Spring Boot | 2.3.12.RELEASE | 应用启动、依赖管理、配置和 Bean 生命周期 | 降低 Web 应用与中间件集成成本 |
| Spring MVC | Boot Starter Web | HTTP API、参数绑定、统一响应 | 分离接口接入与业务逻辑 |
| MyBatis-Plus | 3.4.3 | Mapper、条件查询、分页与基础 CRUD | 减少重复数据访问代码 |
| MySQL | 本地实测 8.0.30 | 权威业务库、事务、库存条件更新、唯一约束 | 保证核心数据持久化和事务一致性 |
| Maven | 项目构建工具 | 依赖、编译、测试和打包 | 统一构建与验证入口 |
| Lombok | Maven 依赖 | 日志、构造器和数据对象样板代码 | 减少重复 Java 代码 |

## 2. Redis

| 使用场景 | 实现 | 解决的问题 |
| --- | --- | --- |
| 店铺详情缓存 | `cache:shop:{id}` + Cache Aside | 降低 MySQL 热点读压力 |
| 缓存穿透 | 空值缓存，短 TTL | 避免不存在店铺持续打到数据库 |
| 登录状态 | token/user Redis key | 支持分布式会话 |
| 商户地理位置 | Redis GEO | 支持按距离查询 |
| 全局 ID | `RedisIdWorker` | 生成分布式订单 ID |
| 秒杀准入 | Lua 原子判断和预扣库存 | 避免高并发超卖与入口重复下单 |
| 一人一单 | `seckill:order:{voucherId}` Set | 在进入异步队列前快速拒绝重复请求 |
| Stream 回退 | `stream.orders` | Kafka 模式故障时保留灰度回退能力 |
| 缓存失效目标 | Redis DEL | 接收 Canal 事件后幂等删除旧店铺缓存 |

Redis 适合低延迟状态和缓存，不作为订单最终事实来源。秒杀最终库存与订单仍由 MySQL 事务确认。

## 3. Kafka

| 能力 | 当前实现 | 解决的问题 |
| --- | --- | --- |
| 异步削峰 | 主 Topic `hmdp.seckill.order.create.v1` | 避免秒杀请求直接同步压垮 MySQL |
| 分区顺序 | `userId` 作为消息 key | 保持同一用户事件的分区内顺序 |
| 可靠发送 | `acks=all`、Producer 重试、等待 broker 结果 | 降低消息发送丢失风险 |
| 手动确认 | Consumer 事务完成后 ACK | 防止先提交 offset 后订单落库失败 |
| 有限失败处理 | Retry Topic + FixedBackOff | 避免无限重试阻塞主分区 |
| 失败终态 | DLT + 失败日志 | 保留人工排查和补偿入口 |
| 重复消费 | 业务幂等查询 + 数据库唯一索引 | 将至少一次交付收敛为业务等效一次 |
| 本地环境 | Kafka 3.9.2 KRaft | 无 ZooKeeper 的独立开发环境 |

## 4. Elasticsearch

| 能力 | 当前实现 | 解决的问题 |
| --- | --- | --- |
| 搜索索引 | `shop_index` | 将搜索读模型与事务主库分离 |
| 文本查询 | `match name` | 支持分词匹配，替代正常链路 MySQL LIKE |
| 相关性排序 | `_score desc`、`id asc` | 返回更符合关键词相关性的结果 |
| 轻量文档 | ES 返回有序店铺 ID | 避免搜索文档承担全部业务展示字段 |
| 字段补齐 | MySQL `selectBatchIds` | 保持 ShopDTO 信息完整和接口兼容 |
| 异常降级 | MySQL LIKE 分页 | ES 不可用时维持基础搜索能力 |
| 初始化 | Bulk 全量同步 | 首次建立 MySQL 到 ES 的搜索数据基线 |
| 本地环境 | Elasticsearch/Kibana 8.19.21 | 提供搜索开发、DSL 调试和观测环境 |

Elasticsearch 是可重建的搜索读模型，不承担业务事务或主数据写入。当前使用 standard analyzer，中文 IK 和实时增量同步仍是后续项。

## 5. Canal

| 能力 | 当前实现 | 解决的问题 |
| --- | --- | --- |
| 变更捕获 | 读取 MySQL ROW binlog | 覆盖直接 SQL、其他服务写库等应用外变更 |
| 原生 Client | Canal Client/Protocol 1.1.8 | 在 Spring Boot 内简单消费，不引入 Adapter |
| 表过滤 | `hmdp.tb_shop` | 只处理目标店铺表，降低误消费 |
| 事件解析 | INSERT/UPDATE/DELETE 行镜像 | 从数据库事实中提取 shop id |
| 缓存删除 | `DEL cache:shop:{id}` | 消除数据库提交后残留的详情旧缓存 |
| 消费确认 | `getWithoutAck` + ACK/rollback | Redis 删除失败时不错误推进消费位点 |
| 幂等 | batch 内去重 + Redis DEL | 安全处理重复事件 |
| 本地环境 | Canal Server 1.1.8 | 提供独立网络、健康检查和持久化位点 |

Canal 负责捕获和交付变化，业务 Consumer 负责解释事件并执行缓存规则。

## 6. 事务与可靠性组合

| 问题 | 解决组合 |
| --- | --- |
| 秒杀超卖 | Redis Lua 原子预扣 + MySQL `stock > 0` 条件扣减 |
| 一人多单 | Redis Set + 事务服务幂等检查 + MySQL 联合唯一索引 |
| 消息重复 | Consumer 幂等 + 数据库约束 |
| 消息消费失败 | 不 ACK + Retry Topic + DLT |
| ES 故障 | 捕获异常并降级 MySQL LIKE |
| 店铺缓存旧数据 | 应用更新后删除 + Canal binlog 再删除 |
| Canal 重复投递 | Redis DEL 幂等 |
| Canal/Redis 暂不可用 | rollback、固定退避、重连重放 |

## 7. 技术选型边界

- Kafka 不替代 Redis Lua：前者负责异步削峰，后者负责请求入口原子准入。
- ES 不替代 MySQL：ES 负责检索，MySQL 负责权威数据和事务。
- Canal 不负责业务规则：它只解析并交付 binlog，缓存 key 和 ACK 条件由应用决定。
- Redis 不作为订单主库：Redis 状态用于高性能判断，最终结果必须落入 MySQL。
- 当前不引入 Outbox、Canal Adapter、复杂搜索或分布式事务框架，保持项目规模下的可理解性。
