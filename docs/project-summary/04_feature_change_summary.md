# HM-DianPing Plus 功能改造汇总

## 1. 总体对比

| 能力 | 原黑马点评基线 | HM-DianPing Plus 当前实现 | 实现效果 |
|---|---|---|---|
| 秒杀异步下单 | Redis Lua + Redis Stream 课程实现 | Redis Lua + Kafka + 事务订单服务 + 手动 ACK + Retry/DLT | 入口削峰、可靠消费、数据库幂等；保留 Redis Stream 回退 |
| 商户名称搜索 | MySQL LIKE | ES match 检索 + MySQL 批量补充 + 顺序恢复 + 降级 | 支持分词和相关性排序，接口协议不变 |
| 店铺缓存一致性 | 业务更新逻辑主动删除 Redis | MySQL binlog + Canal + 缓存失效服务 | 覆盖旁路写库，失败不 ACK，重复事件幂等 |

本表所列能力已经进入当前项目基线；AI 运营助手、ES 实时增量同步和更完整的跨系统补偿仍属于后续阶段。

## 2. Kafka 秒杀改造

### 2.1 主要新增文件

| 文件 | 作用 |
|---|---|
| `src/main/java/com/hmdp/config/kafka/KafkaProducerConfig.java` | Producer 序列化、ACK、重试等基础配置 |
| `src/main/java/com/hmdp/config/kafka/KafkaConsumerConfig.java` | Consumer、手动确认、有限重试和错误处理配置 |
| `src/main/java/com/hmdp/config/kafka/ReliableDeadLetterPublishingRecoverer.java` | 将超过重试次数的消息可靠路由到 DLT |
| `src/main/java/com/hmdp/kafka/message/VoucherOrderMessage.java` | 秒杀订单事件 DTO |
| `src/main/java/com/hmdp/kafka/producer/VoucherOrderKafkaProducer.java` | 使用 `userId` 作为 key 发送订单消息 |
| `src/main/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumer.java` | 校验消息、调用事务订单服务并手动 ACK |
| `src/main/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumer.java` | 记录最终失败消息及异常上下文 |
| `docker/kafka/docker-compose-kafka.yml` | KRaft 模式本地 Kafka 环境 |

### 2.2 主要修改文件

| 文件 | 改造内容 |
|---|---|
| `pom.xml` | 引入 Spring Kafka 依赖 |
| `src/main/resources/application.yaml` | 增加 Kafka 与 `seckill.message.mode` 配置，默认模式为 Kafka |
| `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java` | Lua 成功后按配置选择 Kafka 或 Redis Stream 发布订单事件 |
| `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java` | 集中处理数据库库存条件扣减与订单创建事务 |
| `src/main/resources/seckill.lua` | 配合消息模式保留入口库存和一人一单原子判断 |

### 2.3 测试文件

- `src/test/java/com/hmdp/config/kafka/KafkaConsumerRetryRoutingTest.java`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderKafkaConsumerTest.java`
- `src/test/java/com/hmdp/kafka/consumer/VoucherOrderDltConsumerTest.java`
- `src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java`
- `src/test/java/com/hmdp/service/impl/VoucherOrderServiceImplMessageModeTest.java`
- `src/test/java/com/hmdp/service/impl/VoucherOrderTransactionalServiceTest.java`

### 2.4 实现效果与测试结果

- Redis Lua 继续承担入口库存资格校验、一人一单判断和订单 ID 生成前后的快速裁决。
- Kafka 负责异步削峰；Consumer 成功完成数据库事务后手动 ACK。
- 数据库 `(user_id, voucher_id)` 唯一索引承担最终幂等兜底。
- 持续失败消息经过有限重试后进入 DLT，避免无限重试阻塞分区。
- Kafka 阶段归档时完整测试为 25 个、0 失败、0 错误、1 个环境测试跳过；真实 Kafka 环境测试 2 个用例通过。
- 当前项目完整回归基线已增长为 43 个测试、0 失败、0 错误、3 个环境测试跳过。

### 2.5 当前边界

- Redis Stream 代码没有删除，仅作为 `redis` 模式回退链路。
- Lua 成功和 Kafka 发送不是原子事务，发送失败补偿和跨系统对账仍可继续增强。

## 3. Elasticsearch 商户搜索改造

### 3.1 主要新增文件

| 文件 | 作用 |
|---|---|
| `src/main/java/com/hmdp/config/elasticsearch/ElasticsearchConfig.java` | 创建并管理 ES Java Client |
| `src/main/java/com/hmdp/elasticsearch/document/ShopDocument.java` | `shop_index` 文档模型 |
| `src/main/java/com/hmdp/elasticsearch/service/ShopEsSyncService.java` | 创建索引、设置 mapping、全量同步店铺数据 |
| `src/main/java/com/hmdp/elasticsearch/service/ShopSearchService.java` | 商户搜索服务契约 |
| `src/main/java/com/hmdp/elasticsearch/service/impl/ShopSearchServiceImpl.java` | match 查询、MySQL 补充、排序恢复和异常降级 |
| `src/main/java/com/hmdp/dto/ShopDTO.java` | 搜索接口使用的店铺响应对象 |
| `src/main/resources/elasticsearch/shop-index-mapping.json` | `shop_index` mapping 定义 |
| `docker/elasticsearch/docker-compose-es.yml` | 独立的 Elasticsearch 8.x 与 Kibana 8.x 本地环境 |

### 3.2 主要修改文件

| 文件 | 改造内容 |
|---|---|
| `src/main/java/com/hmdp/controller/ShopController.java` | 仅将 `/shop/of/name` 委托给 `ShopSearchService`，其他店铺接口保持不变 |

ES 阶段复用了项目中已有的 ES 客户端依赖与连接配置基础，没有为了搜索切换改动 Redis、Kafka、Lua 或店铺其他业务流程。

### 3.3 测试文件

- `src/test/java/com/hmdp/elasticsearch/service/ShopEsSyncServiceTest.java`
- `src/test/java/com/hmdp/elasticsearch/integration/ElasticsearchInitialSyncIntegrationTest.java`
- `src/test/java/com/hmdp/elasticsearch/service/ShopSearchServiceImplTest.java`
- `src/test/java/com/hmdp/controller/ShopControllerSearchTest.java`

### 3.4 实现效果与测试结果

- 非空关键词使用 ES match 查询，获得分词能力与相关性排序。
- ES 返回有序 ID，MySQL 批量读取完整 Shop，服务层恢复 ES 顺序并转换 DTO。
- 空关键词保持原有逻辑；ES 异常自动回退到 MySQL LIKE。
- `/shop/of/name` 的请求参数和响应包装结构保持不变。
- ES 搜索阶段完整回归为 35 个测试、0 失败、0 错误、2 个环境测试跳过；当前总基线为 43/0/0/3。

### 3.5 当前边界

- 已实现首次全量同步，尚未实现 MySQL 到 ES 的持续增量同步。
- 当前名称字段使用标准 analyzer，未增加拼音、同义词或复杂搜索能力。
- MySQL 仍是业务主库，ES 只是可重建的搜索索引。

## 4. Canal 缓存一致性改造

### 4.1 主要新增文件

| 文件 | 作用 |
|---|---|
| `src/main/java/com/hmdp/canal/config/CanalProperties.java` | 绑定 Canal host、port、destination、订阅和启停配置 |
| `src/main/java/com/hmdp/canal/config/CanalConfig.java` | 装配 Canal Connector 和相关组件 |
| `src/main/java/com/hmdp/canal/client/CanalClient.java` | 生命周期托管、批量拉取、成功 ACK、失败 rollback 和断线重连 |
| `src/main/java/com/hmdp/canal/handler/ShopBinlogEventHandler.java` | 过滤 `hmdp.tb_shop` 并从三类行事件提取 shop id |
| `src/main/java/com/hmdp/canal/service/ShopCacheInvalidator.java` | 删除 `cache:shop:{id}` 并定义成功语义 |
| `docker/canal/docker-compose-canal.yml` | 独立 Canal Server、网络和持久化卷配置 |

### 4.2 主要修改文件

| 文件 | 改造内容 |
|---|---|
| `pom.xml` | 增加 `canal.client` 和 `canal.protocol` 依赖 |
| `src/main/resources/application.yaml` | 增加可由环境变量覆盖的 `canal.*` 配置 |

没有修改 `ShopController`、`ShopService`、Kafka、ES 搜索、Lua 或秒杀业务代码。

### 4.3 测试文件

- `src/test/java/com/hmdp/canal/client/CanalClientTest.java`
- `src/test/java/com/hmdp/canal/handler/ShopBinlogEventHandlerTest.java`
- `src/test/java/com/hmdp/canal/service/ShopCacheInvalidatorTest.java`
- `src/test/java/com/hmdp/canal/integration/CanalShopCacheIntegrationTest.java`

### 4.4 实现效果与测试结果

- Spring Boot Client 订阅 `hmdp.tb_shop` 的 INSERT、UPDATE、DELETE 事件。
- 同一 batch 内 shop id 去重，所有缓存删除成功后才 ACK；异常时 rollback。
- Redis DEL 返回 false 代表 key 已不存在，按幂等成功处理；null 或异常阻止 ACK。
- Canal 单元测试 7 个全部通过。
- 真实集成测试验证 MySQL 更新后 `cache:shop:1` 被删除并成功 ACK，测试数据随后恢复。
- 最新完整 `mvn test`：43 个测试，0 失败，0 错误，3 个环境测试跳过，`BUILD SUCCESS`。

### 4.5 当前边界

- 当前仅处理店铺详情缓存，不处理 GEO Key、其他聚合缓存或 ES 索引。
- Canal 是异步失效链路，不提供数据库与缓存的瞬时强一致。
- 当前环境仍需在正式部署时使用最小权限 Canal 专用数据库账号。

## 5. 删除文件

本轮三项架构改造没有删除原课程的 Redis Stream 秒杀实现或其他业务文件。保留旧链路便于灰度、回退和对照验证。

## 6. 阶段结论

项目已经形成三条清晰且相互解耦的基础链路：Kafka 承担秒杀异步订单事件，Elasticsearch 承担商户检索，Canal 以数据库变更事实驱动 Redis 店铺缓存失效。当前基线适合继续开发 AI 运营助手，但新增能力应复用现有服务边界，避免绕过 MySQL 主数据、搜索降级和消息幂等机制。
