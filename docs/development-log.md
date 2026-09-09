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
