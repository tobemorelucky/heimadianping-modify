# 开发日志

## 2026-09-06

### 日期

2026-09-06

### 修改文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java`
- `src/main/java/com/hmdp/utils/RedisConstants.java`
- `src/main/resources/application.yaml`
- `src/main/resources/db/hmdp.sql`

### 新增文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderStreamConsumer.java`
- `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java`
- `src/main/resources/seckill-record-failure.lua`
- `src/main/resources/db/migration/V20260906_01__add_voucher_order_unique_index.sql`
- 3 个秒杀专项测试类
- `docs/seckill-baseline/` 下 4 份专项文档
- `SECKILL_BASELINE_RESULT.md`

### 修改原因

修复 Redis Stream 错误 ACK、无有效数据库事务、缺少一人一券唯一约束、无限失败重试和失败订单不可追踪问题；保持现有 Redis Stream 秒杀架构，不引入 Kafka。

### 测试结果

- 秒杀专项测试：7/7 通过。
- 全量测试：13 个中 8 个通过，5 个既有 Spring 集成测试因本机 Redis `127.0.0.1:6379` 未启动而报错。
- Maven 编译成功；未发现新增逻辑的编译错误。

### 当前状态

秒杀基线加固实现完成。上线前需清理潜在重复订单并执行唯一索引 migration；具备 MySQL/Redis 的环境需补跑全量集成与故障恢复测试。

## 2026-09-08

### 日期

2026-09-08

### 操作

完成 Kafka 秒杀模块归档，整理最终架构、完整业务流程、Producer/Consumer、手动 ACK、幂等、有限 Retry/DLT、文件变更和秋招面试回答，为后续 Elasticsearch 开发建立稳定基线。

### 新增文件

- `docs/kafka-seckill/summary/01_kafka_final_architecture.md`
- `docs/kafka-seckill/summary/02_kafka_interview_summary.md`
- `docs/kafka-seckill/summary/03_kafka_file_change_summary.md`

### 修改文件

- `docs/development-log.md`

### 代码变更

无。本次归档没有修改 Java、Spring 配置、SQL 或 Lua 文件。

### 基线验证结果

- Kafka 是秒杀订单默认消息通道，Redis Stream 灰度回退保留。
- 完整 `mvn test`：25 个测试，0 失败，0 错误，1 个环境测试按设计跳过，`BUILD SUCCESS`。
- 显式真实 Kafka 联调：2 个测试，0 失败，0 错误，`BUILD SUCCESS`。
- 已验证 Redis Lua 准入、Kafka Producer、真实 broker、Consumer、事务服务、手动 ACK、Retry/DLT 和幂等链路。

### 当前状态

Kafka 秒杀阶段完成并归档。后续 Elasticsearch 搜索开发应以本次归档所描述的消息链路和测试结果为稳定基线，避免无关修改秒杀事务与消息确认边界。

## 2026-09-08（Elasticsearch 商户搜索设计）

### 日期

2026-09-08

### 操作

完成 Elasticsearch 商户搜索现状分析、目标架构、`shop_index` Mapping、MySQL 初始化同步、Canal 实时同步和面试回答设计。

### 新增文件

- `docs/elasticsearch/01_es_architecture_design.md`
- `docs/elasticsearch/02_es_index_design.md`
- `docs/elasticsearch/03_es_data_sync_design.md`
- `docs/elasticsearch/04_es_interview_summary.md`

### 修改文件

- `docs/development-log.md`

### 分析结论

- 当前 `/shop/of/name` 在 `ShopController` 中直接使用 MyBatis-Plus 生成 `name LIKE '%keyword%'`，没有中文分词、相关性排序或专用搜索 Service。
- 首期新增设计边界 `ShopSearchService -> Elasticsearch shop_index`，保持 `/shop/of/name` 的 URL、参数和返回包装不变。
- 店铺详情缓存、Redis GEO 类型查询、店铺写接口和 Kafka 秒杀模块保持原样。
- `shop_index` 使用 MySQL 店铺 ID 作为 `_id`，核心字段包含名称、类型、地址、描述、地理位置和评分；当前不存在的 `description` 不虚构数据。
- 数据同步先实施可重建的全量初始化，再通过 Canal binlog 实现增量最终一致，并使用重试、位点恢复和定期对账发现漂移。

### 代码与测试

本阶段按要求只进行分析和设计，没有修改 Java、配置、SQL 或 Lua，也没有引入 Elasticsearch 依赖。由于没有代码变更，本阶段未重新运行自动化测试；Kafka 阶段最近基线仍为完整测试 25 个无失败、真实 Kafka 联调 2 个无失败。

### 当前状态

Elasticsearch 商户搜索设计阶段完成，等待进入基础设施接入、索引创建和初始化同步实现阶段。

## 2026-09-08（Elasticsearch Phase 1 环境部署）

### 日期

2026-09-08

### 操作

为商户搜索新增 Elasticsearch 8.x 与 Kibana 8.x 的本地 Docker Compose 环境设计，配置独立容器、持久化卷、网络、端口和 Elasticsearch 健康检查，并补充环境启动及基础验证文档。

### 新增文件

- `docker/elasticsearch/docker-compose-es.yml`
- `docs/elasticsearch/05_es_environment_setup.md`

### 修改文件

- `docs/development-log.md`

### 配置说明

- Elasticsearch：`docker.elastic.co/elasticsearch/elasticsearch:8.19.21`。
- Kibana：`docker.elastic.co/kibana/kibana:8.19.21`。
- 容器名：`hmdp-elasticsearch`、`hmdp-kibana`。
- 数据卷：`hmdp-elasticsearch-data`、`hmdp-kibana-data`。
- 独立网络：`hmdp-es-network`。
- 端口仅绑定本机：Elasticsearch `127.0.0.1:9200`，Kibana `127.0.0.1:5601`。
- 本地单节点关闭 Elastic Security；该配置禁止直接用于生产。

### 验证结果

执行：

```powershell
docker compose -f docker/elasticsearch/docker-compose-es.yml config
```

结果：退出码 `0`，Compose 配置解析成功，服务、镜像、容器名、端口、健康检查、命名卷和独立网络均正确展开。本阶段按要求没有启动容器或验证运行时 API。

### 代码变更

无。未修改 Java 业务代码、`application.yaml`、SQL 或 Lua。

### 当前状态

Elasticsearch Phase 1 本地环境配置完成，等待用户启动环境后进入索引创建与初始化同步阶段。

## 2026-09-08（Elasticsearch Phase 2 数据模型与初始化同步）

### 日期

2026-09-08

### 操作

新增独立商户 ES 文档模型、Elasticsearch REST 连接配置、`shop_index` 显式 Mapping 和 MySQL 店铺全量 Bulk 同步服务；增加隔离单元测试与显式开启的真实环境集成测试。

### 新增文件

- `src/main/java/com/hmdp/elasticsearch/document/ShopDocument.java`
- `src/main/java/com/hmdp/config/elasticsearch/ElasticsearchConfig.java`
- `src/main/java/com/hmdp/elasticsearch/service/ShopEsSyncService.java`
- `src/main/resources/elasticsearch/shop-index-mapping.json`
- `src/test/java/com/hmdp/elasticsearch/service/ShopEsSyncServiceTest.java`
- `src/test/java/com/hmdp/elasticsearch/integration/ElasticsearchInitialSyncIntegrationTest.java`
- `docs/elasticsearch/06_es_document_design.md`
- `docs/elasticsearch/07_es_initial_sync.md`
- `docs/elasticsearch/08_es_sync_test.md`

### 修改文件

- `docs/development-log.md`

### 修改原因

- 用独立 `ShopDocument` 隔离数据库模型与搜索模型。
- 为首次全量同步提供可重复调用、分批执行和可验证的实现。
- 通过显式 Mapping、固定 `_id` 和 Bulk 明细错误检查，避免动态字段漂移、重复文档及部分失败误报成功。
- 保持 Java 8 / Spring Boot 2.3.12 兼容，不新增 Elasticsearch Java Client 依赖，使用现有 Spring Web REST 能力连接 ES 8。

### 测试结果

- `mvn '-Dtest=ShopEsSyncServiceTest' test`：3 个测试通过，0 失败，0 错误。
- `mvn test`：29 个测试，0 失败，0 错误，2 个环境型测试默认跳过，`BUILD SUCCESS`。
- 显式环境集成测试：3 个测试通过，0 失败，0 错误，`BUILD SUCCESS`。
- Elasticsearch 容器：`healthy`；集群状态：`green`。
- MySQL 查询 14 家店铺，Bulk 同步 14 条，`shop_index/_count=14`。
- Mapping 实测：`location=geo_point`、`name=text`。

### 影响范围

- 未修改 `ShopController`。
- 未修改 `ShopService` 及现有商户查询逻辑。
- 未修改 Redis、Kafka、SQL、Lua 和 `application.yaml`。
- 商户接口仍使用原查询链路，ES 当前仅作为初始化完成的数据基线。

### 当前状态

Elasticsearch Phase 2 已完成：数据模型、索引 Mapping、首次全量同步能力及真实环境验证均通过，可进入 ES 查询服务实现阶段。

## 2026-09-09（Elasticsearch Phase 3 搜索服务接入设计）

### 日期

2026-09-09

### 操作

分析当前 `/shop/of/name` 的 Controller、MyBatis-Plus Service 和 ShopMapper 查询链路，设计 `ShopSearchService -> Elasticsearch` 的搜索服务边界、match 查询、相关性排序、完整 Shop 字段补齐、异常降级及测试方案，并整理面试回答。

### 新增文件

- `docs/elasticsearch/06_es_search_service_design.md`
- `docs/elasticsearch/07_es_search_interview.md`

### 修改文件

- `docs/development-log.md`

### 设计结论

- 当前名称搜索逻辑直接位于 `ShopController`，通过 `IService.query()` 生成 `name LIKE '%keyword%'` 并由 `ShopMapper` 查询 MySQL。
- 下一实现阶段新增 `ShopSearchService`、`ShopSearchServiceImpl`，Controller 只负责参数接收和 `Result` 包装。
- 非空关键词对 `shop_index.name` 使用 `match`，由字段 analyzer 分析关键词并按 `_score desc` 排序；同分时按 ID 稳定排序。
- 空关键词使用 `match_all`，保持当前分页浏览全部店铺的行为。
- ES 返回有序店铺 ID，MySQL 仅按主键批量补齐完整 Shop 字段，再按 hits 顺序恢复；不再执行名称 LIKE。
- ES 连接、超时或响应异常时建议受控降级到原 MySQL LIKE，配套错误日志、指标、告警和降级并发限制。
- URL、`name`/`current` 参数、默认页码、每页 10 条以及 `Result.ok(List<Shop>)` 返回结构保持不变。

### 代码与测试

本阶段按要求只生成设计和面试文档，没有修改 Java 代码、配置、SQL、Redis、Kafka、Lua 或 Elasticsearch 索引。由于没有代码变更，本阶段未新增或运行实现测试；最近代码基线仍为 2026-09-09 完整 Maven 回归 29 个测试、0 失败、0 错误、2 个环境测试跳过。

### 当前状态

Elasticsearch Phase 3 搜索服务接入设计完成，等待进入 `ShopSearchService` 实现与 `/shop/of/name` 内部查询切换阶段。

## 2026-09-09（Elasticsearch Phase 3 搜索服务实现）

### 日期

2026-09-09

### 操作

实现商户名称 Elasticsearch 搜索服务，将 `/shop/of/name` 的非空关键词查询从 MySQL LIKE 切换到 ES match；增加有序 ID 解析、MySQL 完整 Shop 批量补齐、ES 排序恢复、ShopDTO 转换和异常降级，并完成隔离测试、接口契约测试与真实 ES 只读冒烟验证。

### 新增文件

- `src/main/java/com/hmdp/dto/ShopDTO.java`
- `src/main/java/com/hmdp/elasticsearch/service/ShopSearchService.java`
- `src/main/java/com/hmdp/elasticsearch/service/impl/ShopSearchServiceImpl.java`
- `src/test/java/com/hmdp/elasticsearch/service/ShopSearchServiceImplTest.java`
- `src/test/java/com/hmdp/controller/ShopControllerSearchTest.java`
- `docs/elasticsearch/08_es_search_implementation.md`
- `docs/elasticsearch/09_es_search_test.md`

### 修改文件

- `src/main/java/com/hmdp/controller/ShopController.java`
- `docs/development-log.md`

### 修改原因

- 将 HTTP Controller 中的数据访问细节迁移到独立搜索服务。
- 使用 ES match 和 `_score` 提供文本分析与相关性排序。
- 通过 ES 有序 ID + MySQL 主键批量查询保留完整 Shop 展示字段。
- 使用 ShopDTO 隔离数据库实体，同时保持原 JSON 字段协议。
- ES 异常时保留原 MySQL LIKE 降级能力，不影响搜索可用性。

### 测试结果

- `ShopSearchServiceImplTest`：5 个测试通过，覆盖正常搜索、无结果、ES 异常降级、排序保持和空关键词。
- `ShopControllerSearchTest`：1 个测试通过，验证路径、参数、Result 包装和主要 Shop 字段不变。
- 组合测试：6 个通过，0 失败，0 错误。
- 真实 ES 只读冒烟：关键词“火锅”命中 4 条，并按 `_score` 降序返回。
- 完整 `mvn test`：35 个测试，0 失败，0 错误，2 个环境测试跳过，`BUILD SUCCESS`。

### 影响范围

- 仅切换 `ShopController.queryShopByName`。
- `GET /shop/{id}`、`POST /shop`、`PUT /shop`、`GET /shop/of/type` 保持原样。
- 未修改 `IShopService`、`ShopServiceImpl`、ShopMapper 接口、Redis、Kafka、SQL、Lua 或应用配置。

### 当前状态

Elasticsearch Phase 3 搜索服务实现完成：名称搜索已接入 ES，协议兼容、完整字段补齐、排序恢复和异常降级均有自动化测试保护。

## 2026-09-09（Elasticsearch Phase 4 搜索模块归档）

### 日期

2026-09-09

### 操作

归档 Elasticsearch 商户搜索最终架构、索引设计、查询与降级流程、MySQL/ES 职责边界、一致性现状、秋招面试回答和完整文件变更清单。

### 新增文件

- `docs/elasticsearch/summary/01_es_final_architecture.md`
- `docs/elasticsearch/summary/02_es_interview_summary.md`
- `docs/elasticsearch/summary/03_es_file_change_summary.md`

### 修改文件

- `docs/development-log.md`

### 归档结论

- `/shop/of/name` 非空关键词已使用 ES match 和 `_score` 排序。
- ES 返回 ID，MySQL 按主键批量补充完整 Shop，最终转换为协议兼容的 ShopDTO。
- 空关键词保留原 MySQL 分页；ES 异常记录日志并降级到 MySQL LIKE。
- MySQL 仍是权威主库，ES 是可重建的搜索读模型。
- 当前已完成首次全量同步，但 Canal 实时增量同步尚未实现，文档没有将规划误写为现状。
- 最近完整测试基线为 35 个测试、0 失败、0 错误、2 个环境测试跳过。

### 代码与测试

本阶段按要求仅新增归档文档并更新开发日志，没有修改 Java、配置、SQL 或 Lua。归档阶段没有代码变更，因此没有重复运行测试，沿用 Phase 3 已通过的完整 Maven 测试基线。

### 当前状态

Elasticsearch 商户搜索阶段完成并已归档。后续可在当前稳定基线上进入 Canal 增量同步、中文分词优化或其他业务模块开发。

## 2026-09-09（Canal 缓存一致性设计）

### 日期

2026-09-09

### 操作

分析当前店铺详情缓存、空值缓存、缓存重建策略和店铺写入时序，设计 MySQL ROW binlog、Canal Consumer、统一缓存删除服务、显式 ACK/rollback、断线恢复、Redis 失败重试与重复事件幂等方案，并整理秋招面试回答。

### 新增文件

- `docs/canal/01_canal_architecture_design.md`
- `docs/canal/02_canal_interview_summary.md`

### 修改文件

- `docs/development-log.md`

### 当前缓存结论

- 店铺详情正缓存 key 为 `cache:shop:{id}`，TTL 30 分钟。
- 不存在店铺使用同一 key 写空字符串，TTL 2 分钟。
- 当前详情查询启用 Cache Aside/Pass Through；互斥锁和逻辑过期重建代码存在但未启用。
- `ShopServiceImpl.update` 在数据库事务内更新 MySQL 后直接删除缓存，无法覆盖删除失败、提交前旧值回填、直接 SQL 或其他服务写入。
- 新增店铺流程没有主动清除可能存在的空值缓存。

### 设计结论

- 目标链路为 `MySQL -> ROW binlog -> Canal -> ShopCanalConsumer -> ShopCacheInvalidationService -> Redis DEL`。
- Canal 负责复制协议、binlog 解析和事件交付；业务负责提取 shopId、删除业务缓存、ACK/rollback 和重新加载规则。
- Consumer 使用 `getWithoutAck`，Redis 删除全部成功后才 ACK；失败 rollback 并退避重试。
- Redis `DEL` 天然幂等，重复事件不会产生错误；同一 batch 内先对 ID 去重。
- 应用内事务提交后删除用于低延迟，Canal 删除用于覆盖失败和绕过应用的写入，不采用固定 sleep 的延迟双删。
- 首期只处理 `cache:shop:{id}`，不同时改造 Redis GEO、ES、Kafka 或秒杀链路。

### 代码与测试

本阶段按要求只生成设计文档，没有修改 Java、配置、SQL 或 Lua，也没有启动 Canal 或运行实现测试。最近代码基线仍为 Elasticsearch Phase 3 完整 Maven 回归：35 个测试、0 失败、0 错误、2 个环境测试跳过。

### 当前状态

Canal 缓存一致性设计完成，等待进入本地 Canal 环境部署、MySQL binlog 验证和缓存失效 Consumer 实现阶段。

## 2026-09-09（Canal Phase 2 开发环境部署）

### 日期

2026-09-09

### 操作

新增独立 Canal Server 1.1.8 Docker 开发环境；对当前 MySQL binlog 参数与账号现状进行只读检查；完成 Compose 静态校验、容器健康检查、11111 端口检查以及 `hmdp` destination 的 MySQL binlog dump 验证。

### 新增文件

- `docker/canal/docker-compose-canal.yml`
- `docs/canal/03_canal_environment_setup.md`

### 修改文件

- `docs/development-log.md`

### 配置说明

- 容器：`hmdp-canal`，镜像 `canal/canal-server:v1.1.8`。
- 独立网络：`hmdp-canal-network`。
- 持久化卷：`hmdp-canal-data`、`hmdp-canal-logs`。
- 本机端口：`127.0.0.1:11111`。
- destination：`hmdp`；只订阅 `hmdp.tb_shop`。
- MySQL 凭据仅通过运行时环境变量注入，未写入仓库。

### MySQL 只读检查

- MySQL 8.0.30。
- `log_bin=ON`。
- `binlog_format=ROW`。
- `binlog_row_image=FULL`。
- `server_id=1`。
- `SHOW MASTER STATUS` 可返回当前 binlog 文件与 position。
- 当前未发现 canal 专用账号；未执行 CREATE USER、GRANT 或其他 SQL 修改。

### 验证结果

- `docker compose -f docker/canal/docker-compose-canal.yml config`：退出码 0，通过。
- Compose 启动：镜像、网络、命名卷和容器创建成功。
- 容器状态：`hmdp-canal` 为 `Up (healthy)`。
- `Test-NetConnection 127.0.0.1 -Port 11111`：`True`。
- Canal Server 启动成功。
- `hmdp` instance 连接宿主机 MySQL、定位 binlog 并进入 dump。
- 最终订阅过滤器：`^hmdp\.tb_shop$`。

### 代码与数据影响

本阶段未修改 Java、application.yaml、SQL、Lua 或 Redis 逻辑，未执行数据库写操作，也未改变已有 Kafka、Elasticsearch 和 Redis 容器。由于没有 Java 代码变更，本阶段不运行 Maven 测试。

### 当前状态

Canal Phase 2 本地开发环境部署完成。进入业务实现前，应由数据库管理员创建最小权限 canal 专用账号，替换本次仅用于无 SQL 验证的临时数据源账号注入方式。

## 2026-09-09（Canal Phase 3 Spring Boot Consumer）

### 日期

2026-09-09

### 操作

接入 Canal 原生 Client，新增 Spring 生命周期托管的消费线程，解析 `hmdp.tb_shop` INSERT、UPDATE、DELETE 行事件，提取 shop id 并删除 `cache:shop:{id}`；实现成功 ACK、失败 rollback、断线退避重连和独立真实环境测试。

### 新增文件

- `src/main/java/com/hmdp/canal/config/CanalConfig.java`
- `src/main/java/com/hmdp/canal/config/CanalProperties.java`
- `src/main/java/com/hmdp/canal/client/CanalClient.java`
- `src/main/java/com/hmdp/canal/handler/ShopBinlogEventHandler.java`
- `src/main/java/com/hmdp/canal/service/ShopCacheInvalidator.java`
- `src/test/java/com/hmdp/canal/client/CanalClientTest.java`
- `src/test/java/com/hmdp/canal/handler/ShopBinlogEventHandlerTest.java`
- `src/test/java/com/hmdp/canal/service/ShopCacheInvalidatorTest.java`
- `src/test/java/com/hmdp/canal/integration/CanalShopCacheIntegrationTest.java`
- `docs/canal/04_canal_consumer_implementation.md`
- `docs/canal/05_canal_integration_test.md`

### 修改文件

- `pom.xml`：增加 `canal.client`、`canal.protocol` 1.1.8。
- `src/main/resources/application.yaml`：增加可由环境变量覆盖的 `canal.*` 配置。
- `docs/development-log.md`。

### 实现原因

原有店铺更新后直接删除缓存只能覆盖应用内正常写入，无法补偿 Redis 删除失败、直接 SQL 或其他服务写库。Canal Consumer 以 MySQL binlog 为事实来源，在数据库提交后异步删除店铺详情缓存。

### ACK与幂等

- `getWithoutAck` 拉取，全部事件解析和缓存删除成功后才 `ack(batchId)`。
- 解析或 Redis 删除失败时 `rollback(batchId)`，不错误确认。
- 空 batch 不 ACK。
- Redis DEL 返回 false 表示 key 不存在，按幂等成功；返回 null 或异常时阻止 ACK。
- 同一 batch 内先对 shop id 去重。

### 测试结果

- Canal 单元测试：7 个，0 失败，0 错误。
- 真实集成测试：1 个，0 失败；日志确认 shopId=1 的 `cache:shop:1` 删除成功并 ACK。
- 集成测试结束后：Redis key 不存在，MySQL 店名已恢复。
- 完整 `mvn test`：43 个测试，0 失败，0 错误，3 个环境测试跳过，`BUILD SUCCESS`。
- 完整回归使用 `-Dcanal.enabled=false`；真实 Canal 链路通过独立环境门控测试验证。

### 问题与处理

首次真实测试发现 Spring 订阅正则多一层转义，Client 虽连接成功但匹配不到 UPDATE。根据运行日志将其修正为 `hmdp\.tb_shop` 后重跑通过。失败轮次和成功轮次均通过 finally 恢复原店名并清理测试缓存。

### 环境清理

本机原 Redis 未运行，测试使用 `redis:7-alpine` 启动 `--rm` 临时容器；测试后已停止并自动删除。Canal Server 继续保持 `healthy`。

### 影响范围

仅新增 Canal 相关包、依赖、配置、测试和文档。未修改 Kafka、Elasticsearch、Lua、秒杀代码、ShopController 或 ShopService。

### 当前状态

Canal Phase 3 完成：MySQL `tb_shop` binlog 到 Redis 店铺详情缓存自动失效链路已实现，并通过单元测试、真实集成测试和完整回归。

## 2026-09-09（项目阶段性归档）

### 日期

2026-09-09

### 操作

基于当前实现和各阶段验证记录，完成 HM-DianPing Plus 整体架构归档，统一整理 Kafka 秒杀、Elasticsearch 商户搜索、Canal 缓存一致性三条核心链路，并形成面向后续 AI 助手开发和秋招复习的项目基线。

### 新增文件

- `docs/project-summary/01_system_architecture.md`
- `docs/project-summary/02_technology_stack.md`
- `docs/project-summary/03_interview_summary.md`
- `docs/project-summary/04_feature_change_summary.md`

### 修改文件

- `docs/development-log.md`

### 归档内容

- 系统分层、组件职责、三条核心业务链路和数据职责边界。
- Java、Spring Boot、MyBatis-Plus、MySQL、Redis、Kafka、Elasticsearch、Canal 等技术用途与解决的问题。
- 三分钟和三十秒项目介绍、负责工作、技术难点、解决方案及准确表达边界。
- 相比原黑马点评的新增文件、修改文件、测试文件、实现效果和当前限制。

### 验证结果

本阶段只新增和更新 Markdown 文档，未修改 Java、配置、SQL 或 Lua，因此未重复运行 Maven 测试。归档引用最近一次已验证的完整基线：43 个测试，0 失败，0 错误，3 个环境测试跳过，`BUILD SUCCESS`；Kafka、Elasticsearch、Canal 真实环境链路均有各自的独立验证记录。

### 当前状态

HM-DianPing Plus 阶段性归档完成。Kafka 秒杀、Elasticsearch 搜索和 Canal 店铺缓存失效已形成稳定开发基线；后续可在明确数据权限和服务边界的前提下进入 AI 运营助手设计与实现阶段。

## 2026-09-09（AI 运营助手设计）

### 日期

2026-09-09

### 操作

完成 HM-DianPing Plus 商家运营助手的首期架构设计、面试总结和功能边界定义。设计采用 Spring Boot 业务与权限边界、独立 FastAPI AI 服务、LangGraph 状态编排和 LLM Tool Calling，首期严格限定为只读分析助手。

### 新增文件

- `docs/ai-assistant/01_ai_architecture_design.md`
- `docs/ai-assistant/02_ai_interview_summary.md`
- `docs/ai-assistant/03_ai_feature_scope.md`

### 修改文件

- `docs/development-log.md`

### 设计内容

- 定义店铺经营概览、订单趋势、优惠券效果和热门数据洞察四类业务场景。
- 说明 Agent 与 RAG 的职责差异：首期实时结构化业务查询以 Tool-Calling Agent 为主，RAG 可作为后续知识工具补充。
- 设计 Spring Boot → FastAPI → LangGraph → LLM 主链路，以及 Tool Adapter 回调 Spring Boot 只读 API 的数据链路。
- 定义店铺信息查询、订单统计、优惠券分析、热门数据分析四个白名单工具的输入、输出和约束。
- 明确 Java/Python 采用内网 REST/JSON、短时服务认证、统一 requestId、有限重试和超时控制。
- 明确 Agent 不能直接访问或修改数据库，不提供店铺、订单、优惠券、库存等写操作。
- 列出 MVP 验收标准和不实现范围，避免影响 Kafka、Elasticsearch、Canal、Redis Lua 等既有链路。

### 验证结果

本阶段只新增和更新 Markdown 文档，未新增或修改 Java、Python、配置、SQL、Lua 及部署文件，因此不运行 Maven 测试。设计内容已与当前店铺、优惠券、订单模型及项目阶段性架构交叉核对。

### 当前状态

AI 运营助手设计阶段完成。下一阶段实施前需要先冻结四个 Tool 的字段 Schema、订单/优惠券统计口径、商户与店铺授权模型，以及独立 Python 服务的部署边界。

## 2026-09-09（AI Assistant Phase 1 基础框架）

### 日期

2026-09-09

### 操作

新增独立 `ai-assistant` Python 服务，完成 FastAPI `/chat`、LangGraph `START → LLM → END`、OpenAI API 兼容 LLM 客户端和环境变量配置基础链路。

### 新增文件

- `ai-assistant/app/__init__.py`
- `ai-assistant/app/main.py`
- `ai-assistant/app/config.py`
- `ai-assistant/app/agent/__init__.py`
- `ai-assistant/app/agent/state.py`
- `ai-assistant/app/agent/graph.py`
- `ai-assistant/app/tools/__init__.py`
- `ai-assistant/.env.example`
- `ai-assistant/.gitignore`
- `ai-assistant/requirements.txt`
- `ai-assistant/README.md`
- `docs/ai-assistant/04_ai_environment_setup.md`
- `docs/ai-assistant/05_ai_phase1_implementation.md`

### 修改文件

- `docs/development-log.md`

### 实现内容

- 从 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL` 读取配置，不硬编码或提交真实密钥。
- `/chat` 接受 `{ "message": "..." }` 并按 `{ "answer": "..." }` 返回成功结果。
- `AgentState` 仅包含 `message` 和 `answer`，无 Memory 或持久化。
- 使用 `StateGraph` 构建单个 LLM Node 的最小 Graph。
- 使用 `ChatOpenAI` 的自定义 `base_url` 支持 DeepSeek/OpenAI 兼容 API。
- 对缺失配置、非法输入和上游调用失败提供明确且不泄露密钥的错误响应。

### 影响范围

本阶段没有修改 Java、Spring Boot 配置、SQL 或 Lua；没有接入数据库、Redis、Kafka、Elasticsearch、Canal、向量数据库或 RAG。既有秒杀、搜索和缓存一致性链路不受影响。

### 验证结果

- Python 3.11.7 项目内 `.venv` 创建成功。
- `pip install -r requirements.txt` 成功；`pip check` 返回 `No broken requirements found`。
- 7 个 Python 源文件的 AST 语法检查通过。
- FastAPI 应用导入成功，`/chat` 路由存在。
- LangGraph 编译成功，节点为 `__start__`、`llm`、`__end__`。
- `uvicorn app.main:app --reload` 启动成功，应用监听 `127.0.0.1:8000`。
- `GET /openapi.json` 返回 200。
- 当前无 LLM 环境变量及本地 `.env`，真实 `/chat` 返回预期 503，并且只显示缺失变量名。
- 空白消息返回 422。
- 使用隔离假 LLM 验证 FastAPI → LangGraph → LLM Node → Response 完整链路，返回 200 和 `{"answer":"mock-deepseek-answer"}`。
- 因缺少真实 DeepSeek API 配置，真实外部回答验证未执行，不能标记为通过；配置凭据后可直接复测。
- 验证完成后已停止 Uvicorn，8000 端口未继续占用。

### 当前状态

AI Assistant Phase 1 基础框架、依赖安装和本地接口链路验证完成。唯一未完成项是真实 DeepSeek 外部调用，阻塞原因为当前环境没有 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL` 或本地 `.env`。

## 2026-09-09（AI Assistant Phase 2 Tool Calling）

### 日期

2026-09-09

### 操作

新增 AI 专用店铺基础信息接口和 Python `get_shop_info` HTTP Tool，将 LangGraph 升级为 `Agent → ToolNode → Agent` 循环，并完成 Java、Python、Graph 和真实 Java–Python Tool 边界验证。

### 新增文件

- `src/main/java/com/hmdp/controller/AiShopController.java`
- `src/main/java/com/hmdp/dto/AiShopInfoDTO.java`
- `src/test/java/com/hmdp/controller/AiShopControllerTest.java`
- `ai-assistant/app/tools/shop_tools.py`
- `ai-assistant/tests/__init__.py`
- `ai-assistant/tests/test_shop_tool.py`
- `ai-assistant/tests/test_agent_graph.py`
- `docs/ai-assistant/06_ai_tool_calling_design.md`
- `docs/ai-assistant/07_ai_phase2_implementation.md`

### 修改文件

- `src/main/java/com/hmdp/config/MvcConfig.java`
- `ai-assistant/app/config.py`
- `ai-assistant/app/agent/state.py`
- `ai-assistant/app/agent/graph.py`
- `ai-assistant/app/tools/__init__.py`
- `ai-assistant/app/main.py`
- `ai-assistant/.env.example`
- `ai-assistant/requirements.txt`
- `ai-assistant/README.md`
- `docs/ai-assistant/04_ai_environment_setup.md`
- `docs/development-log.md`

### 实现内容

- `GET /api/ai/shop/{id}` 复用 `IShopService`、`IShopTypeService`，只返回 id、name、type、address。
- 独立 DTO 避免把完整 Shop 实体暴露给 AI。
- `/api/ai/shop/**` 仅因字段与公开店铺详情等价而排除登录拦截；未开放其他 AI 路径。
- Python Tool 从 `SPRING_BOOT_BASE_URL` 读取地址，通过固定路径和 5 秒超时调用 Java。
- Python 对 Spring Result 进行校验并再次执行字段白名单过滤。
- LangGraph 使用 `ToolNode` 和 `tools_condition` 建立可终止的 Tool Calling 循环。
- `messages` 仅存在于单次请求内，没有 Checkpointer 或持久化 Memory。

### 测试结果

- Python：2 个 unittest，全部通过。
- Java专项：`AiShopControllerTest` 2 个测试，0 失败，0 错误，`BUILD SUCCESS`。
- 完整 Maven 首次因本机 Redis 未运行导致 5 个原有上下文测试错误；启动一次性 Redis、禁用 Canal Client 后重跑，45 个测试、0 失败、0 错误、3 个环境测试跳过，`BUILD SUCCESS`。
- 真实 Java 接口：店铺 1 返回 success=true、名称“103茶餐厅”。
- 真实 Python HTTP Tool：返回店铺 1 的 id、name、type“美食”和 address。
- 确定性假模型触发真实 Tool 后，LangGraph 完成两次 Agent 调用并得到包含真实 Tool Result 的 answer。
- 验证结束后 Spring Boot 和一次性 Redis 容器已停止。
- 当前仍无真实 DeepSeek 环境配置，因此“LLM 自主选择 Tool 并生成回答”未执行，不能标记为通过。

### 影响范围

没有修改已有 `ShopController`、Kafka、Elasticsearch、Canal、Redis Lua 或秒杀逻辑；没有增加数据库连接、RAG、向量库或 Agent Memory。

### 当前状态

AI Assistant Phase 2 的 Java 业务出口、Python Tool、LangGraph 调用循环和真实 HTTP 数据链路已完成。剩余外部联调项仅为配置有效 DeepSeek 凭据后验证模型原生 Tool Calling。

## 2026-09-09（AI Assistant Phase 2 真实 LLM 联调）

### 日期

2026-09-09

### 操作

在不修改代码的前提下，检查 AI 服务 LLM 配置读取机制，启动 Spring Boot 与 FastAPI，并使用“查询店铺1的信息”调用真实 `/chat`，验证 DeepSeek 是否驱动 LangGraph 触发 `get_shop_info`。

### 新增文件

- `docs/ai-assistant/08_ai_phase2_real_llm_test.md`

### 修改文件

- `docs/development-log.md`

### 配置检查

- `config.py` 使用 `load_dotenv(ai-assistant/.env)` 加载本地文件。
- `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL` 均使用 `os.getenv` 读取。
- 源码未硬编码 API Key。
- 本次检查时 `ai-assistant/.env` 不存在，当前进程中的三项 LLM 环境变量也均未设置。
- 未输出、创建或提交任何真实密钥。

### 运行验证

- 启动一次性 Redis 7 测试容器后，Spring Boot 在 8081 端口启动成功。
- `GET /api/ai/shop/1` 就绪检查返回 `success=true`。
- FastAPI/Uvicorn 在 8000 端口以 reload 模式启动成功。
- `POST /chat` 请求体为 `{"message":"查询店铺1的信息"}`。
- 返回 HTTP 503：缺少 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL`。
- 流程在创建 LLM Client 前的配置校验终止，DeepSeek 未被请求，Agent 未产生 Tool Call，`get_shop_info` 未由 Agent 触发。
- Spring Boot 就绪检查不能冒充 Agent Tool Calling 结果。

### 测试结论

真实 DeepSeek Tool Calling 联调未通过，阻塞原因是运行环境没有任何真实 LLM 配置，不是 Java/Python HTTP Tool 链路故障。配置有效且支持 Tool Calling 的 DeepSeek 模型后需要重新执行本测试。

### 环境清理

Spring Boot、FastAPI 和一次性 Redis 均已停止；没有遗留测试服务或提交密钥。

### 影响范围

本轮只新增和更新 Markdown 文档，没有修改 Java、Python、配置、SQL、Lua、Kafka、Elasticsearch 或 Canal 逻辑。

### 当前状态

配置机制与服务启动已验证；真实 LLM 自动选择 `get_shop_info` 和生成最终回答仍待本地注入 DeepSeek 配置后复测。

## 2026-09-09（AI Assistant Phase 2 真实 DeepSeek 联调通过）

### 日期

2026-09-09

### 操作

修正本地 `.env` 中复制错误的方舟 API Key，重新启动 Spring Boot 与 FastAPI，执行真实 DeepSeek Agent Tool Calling 端到端测试。

### 新增文件

- `docs/ai-assistant/09_ai_phase2_real_llm_test.md`

### 修改文件

- `ai-assistant/.env`（仅本地配置，已被 Git 忽略）
- `docs/development-log.md`

### 测试结果

- 方舟 `/ping`：HTTP 200。
- `deepseek-v4-pro-ga-260813` 最小 Chat Completions：HTTP 200。
- Spring Boot `GET /api/ai/shop/1`：返回店铺 1 的真实基础信息。
- FastAPI `POST /chat`：HTTP 200。
- LangGraph 真实轨迹：`HumanMessage → AIMessage(get_shop_info Tool Call) → ToolMessage(get_shop_info) → AIMessage`。
- 最终回答正确包含店铺名称“103茶餐厅”、类型“美食”和地址。
- API Key 未写入文档、日志或 Git 跟踪文件。

### 当前状态

AI Assistant Phase 2 真实 DeepSeek Tool Calling 联调完成，Java 只读业务出口、Python Tool、LangGraph 工具循环及真实 LLM 决策链路全部验证通过。

## 2026-09-10（AI Assistant Phase 3 业务分析工具扩展）

### 日期

2026-09-10

### 操作

新增订单统计和优惠券统计两个 AI 只读业务出口；在 Python AI 服务中实现对应 HTTP Tool，注册到既有 LangGraph ToolNode，并完成真实 DeepSeek 多 Tool Calling 联调。

### 新增文件

- `src/main/java/com/hmdp/controller/AiOrderStatisticsController.java`
- `src/main/java/com/hmdp/controller/AiCouponStatisticsController.java`
- `src/main/java/com/hmdp/dto/AiOrderStatisticsDTO.java`
- `src/main/java/com/hmdp/dto/AiCouponStatisticsDTO.java`
- `src/main/java/com/hmdp/service/AiBusinessStatisticsService.java`
- `src/main/java/com/hmdp/service/impl/AiBusinessStatisticsServiceImpl.java`
- `src/test/java/com/hmdp/controller/AiOrderStatisticsControllerTest.java`
- `src/test/java/com/hmdp/controller/AiCouponStatisticsControllerTest.java`
- `src/test/java/com/hmdp/service/impl/AiBusinessStatisticsServiceImplTest.java`
- `ai-assistant/app/tools/order_tools.py`
- `ai-assistant/app/tools/coupon_tools.py`
- `ai-assistant/tests/test_order_tool.py`
- `ai-assistant/tests/test_coupon_tool.py`
- `docs/ai-assistant/10_ai_phase3_business_tools_design.md`
- `docs/ai-assistant/11_ai_phase3_business_tools_test.md`

### 修改文件

- `src/main/java/com/hmdp/mapper/VoucherOrderMapper.java`
- `ai-assistant/app/tools/__init__.py`
- `ai-assistant/app/agent/graph.py`
- `ai-assistant/tests/test_agent_graph.py`
- `docs/development-log.md`

### 修改原因

- 将订单和优惠券运营指标继续封装在 Spring Boot 业务边界内，避免 Python 直连 MySQL。
- 使用 MySQL 聚合而不是把明细拉入 Controller，减少数据传输和职责混乱。
- 通过字段白名单和只读 Tool 保持 Agent 的最小权限。
- 对复合经营问题要求同时调用订单和优惠券工具，使分析依据完整且可验证。

### 测试结果

- Java 专项：7 个测试，0 失败，0 错误，`BUILD SUCCESS`。
- Python：4 个测试，0 失败，0 错误。
- 完整 Maven：52 个测试，0 失败，0 错误，3 个环境集成测试跳过，`BUILD SUCCESS`。
- 真实 Java 接口：店铺 1 的订单和优惠券统计均返回 `success=true`。
- 真实 DeepSeek 轨迹：自主调用 `get_order_statistics`、`get_coupon_statistics`，随后生成自然语言经营分析。
- `.env` 继续由 Git 忽略，API Key 未写入代码、文档或日志。

### 影响范围

仅增加 AI 专用只读接口、经营统计服务、Python Tool、测试和文档；未修改 Kafka、Elasticsearch、Canal、Redis Lua 或秒杀代码，未引入 RAG、向量数据库、Memory、MCP 或多 Agent。

### 当前状态

AI Assistant Phase 3 完成：订单统计、优惠券统计、LangGraph 多工具调用及真实 DeepSeek 端到端链路均已验证。

## 2026-09-10（AI Assistant 最终归档）

### 日期

2026-09-10

### 操作

整理 AI 运营助手最终架构、LangGraph 与 Tool Calling 流程、Java/Python 职责边界、秋招面试回答和完整文件变更清单。

### 修改文件

- `docs/development-log.md`

### 生成文档

- `docs/ai-assistant/summary/01_ai_final_architecture.md`
- `docs/ai-assistant/summary/02_ai_interview_summary.md`
- `docs/ai-assistant/summary/03_ai_file_change_summary.md`

### 归档基线

- 单一无状态 LangGraph Agent。
- 三个只读业务 Tool：店铺信息、最近 7 天订单统计、累计优惠券统计。
- Python 只通过 HTTP 调用 Spring Boot，不直接连接业务数据库或中间件。
- 真实 DeepSeek 单工具与多工具调用均已验证。
- Java、Python 自动化测试以及完整 Maven 测试已有通过记录。

### 影响范围

本轮仅新增和修改 Markdown 文档，没有修改 Java、Python、配置、SQL、Lua、Kafka、Elasticsearch、Canal 或 Redis 逻辑。

### 当前状态

AI Assistant 模块归档完成，可作为后续开发基线和秋招面试复习材料。

## 2026-09-10（项目最终工程化整理）

### 日期

2026-09-10

### 操作

统一本地 Docker 环境管理入口，增加 Spring Boot、FastAPI 和六个 Docker 组件的一键启动、停止及状态检查脚本，并补充运行时架构和新环境启动指南。

### 新增文件

- `docker-compose.yml`
- `scripts/start-all.ps1`
- `scripts/stop-all.ps1`
- `scripts/status.ps1`
- `docs/project-summary/05_project_runtime_architecture.md`
- `docs/runtime/01_startup_guide.md`

### 实现说明

- 根 Compose 新增 MySQL 8.0.30、Redis 7，并通过 `include` 复用已有 Kafka、Elasticsearch/Kibana、Canal Compose。
- 原 `docker/kafka/`、`docker/elasticsearch/`、`docker/canal/` 文件保持不变且仍可独立使用。
- MySQL 开启 ROW binlog 和 FULL row image，首次使用现有 `hmdp.sql` 初始化命名卷。
- 一键启动会将旧 Compose project 迁移到统一 project，迁移和停止均不使用 `-v`。
- 如果宿主机 3306 已占用，脚本使用稳定记录的 3307，并同步设置 Spring Boot 与 Canal 连接端口。
- Java/Python 进程 PID 和日志统一写入 `target/runtime`。
- 停止脚本只结束 PID 文件记录的应用进程，避免按端口误杀手动启动进程。

### 测试结果

- `docker compose -f docker-compose.yml config`：退出码 0，六个组件全部成功展开。
- 三个 PowerShell 脚本：AST 语法解析通过。
- `status.ps1`：实际执行成功，能够报告 Docker 可用性、受管进程和 8081/8000 端口状态。
- 首次真实根 Compose 启动完成 MySQL、Kibana 缺失镜像拉取，并发现旧 project 同名容器冲突；已据此增加保留数据卷的迁移步骤。
- 当前 Codex 沙箱对 Docker Desktop named pipe 的提升权限审核连续超时，修正后的完整启动、停止生命周期未能复跑，因此没有将该项标记为通过。需在本机 PowerShell 按启动指南完成最终验收。

### 影响范围

没有修改 Java 或 Python 业务代码、application.yaml、SQL、Lua、Kafka、Elasticsearch、Canal、Redis 或 AI Agent 实现；没有删除已有 Compose 或任何命名卷。

### 当前状态

统一运行入口、脚本和文档已完成；Compose 静态配置及脚本语法通过。完整实际启停仍待在具备 Docker named pipe 权限的本机终端执行验收。

## 2026-09-10（Nginx 前端纳入一键启停）

### 操作

将项目自带的 Nginx 静态前端加入统一启动、停止和状态检查流程。

### 修改文件

- `scripts/start-all.ps1`
- `scripts/stop-all.ps1`
- `scripts/status.ps1`
- `docs/runtime/01_startup_guide.md`
- `docs/development-log.md`

### 实现说明

- 启动前执行 Nginx 配置校验，启动后等待 8080 端口就绪。
- 保存 Nginx master PID，停止时优先使用 `nginx -s quit` 优雅退出。
- 状态脚本新增 Nginx 受管 PID 与 8080 端口检查。
- 前端入口统一为 `http://127.0.0.1:8080`，`/api` 继续反向代理至 Spring Boot 8081。

### 影响范围

仅修改工程化脚本与文档，未修改 Java、Python、SQL、Lua 或中间件业务逻辑。

## 2026-09-10（MySQL 初始化兼容与运行链路修复）

### 问题

原始 `hmdp.sql` 来自 MySQL 5.6，零日期默认值被 MySQL 8 的 `NO_ZERO_DATE` 拒绝，首次初始化在 `tb_seckill_voucher` 处中断。容器端口健康但正式库只有三张表，导致首页店铺类型和博客接口返回“服务器异常”。Canal 命名卷还保留了旧 3306 MySQL 的 binlog 位点，无法消费当前 3307 MySQL 事件。

### 修改文件

- `docker-compose.yml`
- `docs/runtime/01_startup_guide.md`
- `docs/development-log.md`

### 修复

- 保留原 SQL 和业务表逻辑，仅在本地 MySQL 8 关闭零日期检查。
- MySQL 健康检查增加 10 张项目表完整性校验。
- 修复前完成数据库备份，并通过隔离临时库验证原 SQL。
- 正式库只补充缺失的七张表，不覆盖已有博客、评论和关注表。
- 备份并重置 Canal 旧位点/TSDB，使其从当前 MySQL binlog 建立新位点。

### 验证

- 店铺类型、热门博客、店铺详情和 ES 店铺名称搜索接口返回成功。
- 本地静态资源引用 24 个，缺失 0 个。
- ES 集群绿色，`shop_index` 共 14 条店铺文档。
- Kafka 主 Topic、Retry、DLT 存在，正式消费者在线；Redis `PING` 返回 `PONG`。
- 三个 AI 专用 Spring Boot 接口与 FastAPI 文档返回 HTTP 200。
- Canal 更新事件触发 `cache:shop:1` 删除，随后恢复测试字段原值。
- 完整 `mvn test` 首次运行发现课程 Demo 测试污染真实 Redis；清理错误格式店铺缓存，并将四个外部副作用 Demo 测试标记为手工禁用。
- `HmDianPingApplicationTests` 整体标记为课程手工演示，避免标准测试为已禁用方法仍启动完整 Spring 上下文、Canal Client 和 Kafka Consumer。
- `RedissonTest` 同样属于连接真实 Redis 的课程手工演示；整体标记为手工禁用，避免标准测试顺带启动生产 Kafka Consumer 和 Canal Client。
- 修复后完整 `mvn test`：49 个测试，0 失败，0 错误，5 个环境/课程手工测试跳过，`BUILD SUCCESS`。
- 使用真实浏览器验证首页与店铺详情页：核心接口、CSS/JS、本地图片及店铺外链图片均返回 HTTP 200，浏览器控制台 0 错误、0 警告。
- 回归后 `cache:shop:1` 为当前穿透查询逻辑需要的原始 `Shop` JSON，不再包含逻辑过期包装层。

### 影响范围

没有修改 Java 业务代码、Python、SQL、Lua、Kafka、Elasticsearch、Redis 或 AI Agent 业务逻辑；仅调整 Java Demo 测试执行方式，业务数据卷未删除。

## 2026-09-12（Docker MySQL 默认端口统一）

### 操作

将根 Compose、根 Compose 下的 Canal 数据源以及 Spring Boot 本地默认数据源统一调整为宿主机 `3307`，解决本机 MySQL 已占用 `3306` 时直接执行 `docker compose up -d` 失败的问题。

### 修改文件

- `docker-compose.yml`
- `src/main/resources/application.yaml`
- `docs/runtime/01_startup_guide.md`
- `docs/development-log.md`

### 兼容性

- 只修改本地默认配置，没有修改 Java 业务逻辑。
- `MYSQL_HOST_PORT`、`CANAL_DB_PORT`、`SPRING_DATASOURCE_URL` 仍可显式覆盖默认值。
- 没有删除或重新创建任何命名卷。

### 验证结果

- `docker compose config` 通过，MySQL 映射为 `127.0.0.1:3307 -> 3306`，Canal 数据源为 `host.docker.internal:3307`。
- 未设置任何临时端口环境变量，直接执行 `docker compose up -d` 成功，退出码为 0。
- MySQL、Redis、Elasticsearch、Canal 均达到 healthy；Kafka、Kibana 正常运行。
- 旧命名卷的 Compose project 标签警告不影响复用，也没有执行带 `-v` 的操作。
- 完整 `mvn test`：49 个测试，0 失败，0 错误，5 个环境/课程手工测试跳过，`BUILD SUCCESS`。

## 2026-09-12（发布笔记图片目录修复）

### 操作

移除课程环境遗留的 `D:\\lesson\\...` 图片上传硬编码路径，将发布笔记图片默认保存到当前项目的 Nginx 前端目录 `nginx-1.18.0/html/hmdp/imgs`，保持现有 `/imgs/blogs/...` URL 协议不变。

### 修改文件

- `src/main/java/com/hmdp/utils/SystemConstants.java`
- `src/main/java/com/hmdp/controller/UploadController.java`
- `src/test/java/com/hmdp/controller/UploadControllerTest.java`
- `docs/runtime/01_startup_guide.md`
- `docs/development-log.md`

### 修复内容

- 默认上传目录根据项目工作目录生成，不再依赖开发者机器上的固定盘符。
- 支持通过 `HMDP_IMAGE_UPLOAD_DIR` 覆盖部署目录。
- 统一解析上传返回的 `/blogs/...` 与前端删除请求的 `/imgs/blogs/...`。
- 增加目录边界校验，拒绝解析到图片根目录之外的文件路径。
- 测试使用 JUnit 临时目录，不污染前端真实图片目录。

### 测试结果

- `UploadControllerTest`：上传和删除测试通过。
- 完整 `mvn test`：50 个测试，0 失败，0 错误，5 个环境/课程手工测试跳过，`BUILD SUCCESS`。
- 真实启动 Spring Boot 后上传临时 PNG 成功，文件位于项目 `imgs/blogs/...`；Nginx 对应 URL 返回 HTTP 200，内容长度一致；随后通过删除接口成功清理临时文件。

## 2026-09-14（秋招项目完整复盘）

### 操作

基于当前代码、各模块归档和完整开发日志，生成面向秋招项目介绍、技术复习和追问准备的综合复盘文档。

### 生成文档

- `docs/project-summary/06_autumn_recruitment_project_review.md`

### 文档内容

- 原项目问题、最终架构、核心业务和五类主要改造。
- Kafka 秒杀、ES 搜索、Canal 缓存一致性、AI Agent 和运行工程化的实现细节与流程。
- MySQL 初始化、Canal 位点与正则、Kafka 重试、Docker 端口、测试污染、AI 凭据类型和上传目录等真实失败案例。
- 最新测试基线、真实联调证据、准确表达边界、后续演进优先级和面试演示清单。

### 影响范围

本阶段只新增和更新 Markdown 文档，没有修改 Java、Python、配置、SQL、Lua 或运行数据。
