# Elasticsearch 文件变更汇总

## 1. 汇总范围

本文汇总 Elasticsearch 商户搜索从设计、环境、数据初始化到搜索接口切换阶段的文件。工作区中同时存在 Kafka 秒杀改造文件，以下清单不把 Kafka 文件计入 ES 阶段。

## 2. 新增生产代码与资源

| 文件 | 作用 |
| --- | --- |
| `src/main/java/com/hmdp/config/elasticsearch/ElasticsearchConfig.java` | 创建 ES 专用 `RestTemplate`，默认连接 `http://127.0.0.1:9200`，配置连接和读取超时。 |
| `src/main/java/com/hmdp/elasticsearch/document/ShopDocument.java` | `shop_index` 文档模型，包含 ID、名称、类型、区域、地址、描述和地理坐标。 |
| `src/main/java/com/hmdp/elasticsearch/service/ShopEsSyncService.java` | 创建 `shop_index`、读取 MySQL 全部店铺、转换文档并按 500 条执行 Bulk 首次同步。 |
| `src/main/java/com/hmdp/elasticsearch/service/ShopSearchService.java` | 定义名称搜索服务接口，隔离 Controller 和 ES 实现。 |
| `src/main/java/com/hmdp/elasticsearch/service/impl/ShopSearchServiceImpl.java` | 实现 match 查询、有序 ID 解析、MySQL 批量补齐、排序恢复、DTO 转换及 LIKE 降级。 |
| `src/main/java/com/hmdp/dto/ShopDTO.java` | 名称搜索响应 DTO；字段与原 Shop JSON 对齐，保持接口协议。 |
| `src/main/resources/elasticsearch/shop-index-mapping.json` | `shop_index` settings 和显式 Mapping，包含 `text`、`keyword`、`long`、`geo_point` 等类型。 |

## 3. 修改生产代码

| 文件 | 修改内容 | 未受影响范围 |
| --- | --- | --- |
| `src/main/java/com/hmdp/controller/ShopController.java` | `queryShopByName` 从 Controller 内 MySQL Wrapper 改为调用 `ShopSearchService.searchByName`。 | 详情、新增、更新、按类型和 Redis GEO 接口未改。 |

未修改：

- `src/main/java/com/hmdp/service/IShopService.java`
- `src/main/java/com/hmdp/service/impl/ShopServiceImpl.java`
- `src/main/java/com/hmdp/mapper/ShopMapper.java`
- `pom.xml`
- `src/main/resources/application.yaml`
- SQL 和 Lua 文件
- Redis 与 Kafka 业务代码

## 4. 测试文件

| 文件 | 作用 | 最近结果 |
| --- | --- | --- |
| `src/test/java/com/hmdp/elasticsearch/service/ShopEsSyncServiceTest.java` | Mock ES 和 Mapper，验证连接、索引创建、Mapping、坐标转换及 Bulk 同步。 | 3 个测试通过。 |
| `src/test/java/com/hmdp/elasticsearch/integration/ElasticsearchInitialSyncIntegrationTest.java` | 显式环境测试，真实连接 MySQL 与 ES，验证连接、索引和首次全量同步。 | 显式运行时 3 个通过；普通回归默认跳过。 |
| `src/test/java/com/hmdp/elasticsearch/service/ShopSearchServiceImplTest.java` | 验证 match、无结果、排序恢复、异常降级及空关键词原逻辑。 | 5 个测试通过。 |
| `src/test/java/com/hmdp/controller/ShopControllerSearchTest.java` | 验证 `/shop/of/name` 路径、参数、Result 包装和主要 ShopDTO 字段兼容。 | 1 个测试通过。 |

完整 Maven 基线：35 个测试，0 失败、0 错误、2 个环境测试跳过，`BUILD SUCCESS`。

## 5. Docker 环境文件

| 文件 | 作用 |
| --- | --- |
| `docker/elasticsearch/docker-compose-es.yml` | 定义 Elasticsearch 8.19.21、Kibana 8.19.21、独立网络、健康检查和持久化卷。 |

容器名为 `hmdp-elasticsearch`、`hmdp-kibana`；ES 数据卷为 `hmdp-elasticsearch-data`。本地环境关闭安全认证，仅用于开发，不可直接复制到生产。

## 6. 分析与实施文档

| 文件 | 作用 |
| --- | --- |
| `docs/elasticsearch/01_es_architecture_design.md` | 原 MySQL 搜索现状、ES 目标架构和业务边界。 |
| `docs/elasticsearch/02_es_index_design.md` | `shop_index` 字段与 Mapping 设计。 |
| `docs/elasticsearch/03_es_data_sync_design.md` | 首次全量同步和未来 Canal 增量同步设计。 |
| `docs/elasticsearch/04_es_interview_summary.md` | ES 设计阶段面试问题初稿。 |
| `docs/elasticsearch/05_es_environment_setup.md` | ES/Kibana Docker 启动、端口、健康检查和基础验证。 |
| `docs/elasticsearch/06_es_document_design.md` | 已实现 ShopDocument 与 Mapping 说明。 |
| `docs/elasticsearch/07_es_initial_sync.md` | ShopEsSyncService 全量同步流程和使用方式。 |
| `docs/elasticsearch/08_es_sync_test.md` | 初始化同步单元测试和真实环境测试记录。 |
| `docs/elasticsearch/06_es_search_service_design.md` | 搜索服务边界、查询、补齐、降级和测试设计。 |
| `docs/elasticsearch/07_es_search_interview.md` | 搜索接入阶段面试回答。 |
| `docs/elasticsearch/08_es_search_implementation.md` | ShopSearchService 实现和接口切换记录。 |
| `docs/elasticsearch/09_es_search_test.md` | 搜索单测、Controller 契约、真实 ES 冒烟和完整回归结果。 |

编号存在并行阶段文档，例如文档模型与搜索设计均从 `06` 开始；文件名能够区分主题，本次归档不重命名历史文件，避免破坏已有引用。

## 7. 本次归档文件

| 文件 | 作用 |
| --- | --- |
| `docs/elasticsearch/summary/01_es_final_architecture.md` | 最终架构、索引、查询、降级、职责和一致性现状。 |
| `docs/elasticsearch/summary/02_es_interview_summary.md` | 秋招高频问题、项目回答与边界。 |
| `docs/elasticsearch/summary/03_es_file_change_summary.md` | ES 阶段生产代码、测试、环境和文档清单。 |

## 8. 删除文件

无。Redis、Kafka 和原 MySQL 业务能力均保留。
