# Elasticsearch 初始化同步测试记录

## 1. 测试分层

### 隔离单元测试

文件：`src/test/java/com/hmdp/elasticsearch/service/ShopEsSyncServiceTest.java`

通过 `MockRestServiceServer` 和 Mock `ShopMapper` 验证：

1. Elasticsearch 根 API 连接判断。
2. `shop_index` 不存在时携带显式 Mapping 创建。
3. MySQL 店铺转换、`lat=y`/`lon=x` 坐标规则和 NDJSON Bulk 请求。

执行命令：

```powershell
mvn '-Dtest=ShopEsSyncServiceTest' test
```

结果：3 个测试通过，0 失败，0 错误，0 跳过，`BUILD SUCCESS`。

### 独立环境集成测试

文件：`src/test/java/com/hmdp/elasticsearch/integration/ElasticsearchInitialSyncIntegrationTest.java`

该测试默认不运行，只有设置 `ES_INTEGRATION_TEST_ENABLED=true` 才会连接本机 Elasticsearch 和 MySQL，依次验证：

1. ES 根 API 响应成功。
2. `shop_index` 创建成功或已存在。
3. `ShopMapper` 读取的全部店铺经 Bulk 写入后，可通过 `_count` 查询。

默认关闭的原因是它依赖外部环境并会写入本地 `shop_index`，不应让普通 `mvn test` 隐式改变外部状态。

本次已在本机真实 Elasticsearch 与 MySQL 环境完成联调，结果见本文末尾。

## 2. 全量回归

执行命令：

```powershell
mvn test
```

结果：29 个测试，0 失败，0 错误，2 个环境型测试跳过，`BUILD SUCCESS`。Kafka 未运行时应用上下文测试出现连接告警，但不影响测试结果，也不是本次 ES 代码引入的失败。

## 3. 验收标准

- 集群健康至少为 `yellow`（本地单节点、零副本的正常状态也可为 `green`）。
- `shop_index` 存在且 `location` 类型为 `geo_point`。
- 同步服务返回数量等于 MySQL `tb_shop` 查询数量。
- ES `_count` 不小于本次 MySQL 店铺数量；首次空索引环境应相等。
- Bulk 响应 `errors=false`。

## 4. 已知问题与后续事项

- `description` 在现有数据库实体中没有权威来源，本次不写入虚构内容。
- 当前采用内置 `standard` analyzer，不代表中文检索最终方案。
- 本阶段只建立数据模型和初始化能力，没有替换商户查询接口。

## 5. 真实环境结果

执行日期：2026-09-08。

环境状态：

- Elasticsearch 镜像：`8.19.21`
- 容器：`hmdp-elasticsearch`，状态 `healthy`
- 集群：`hmdp-es-local`，状态 `green`，节点数 1

执行命令：

```powershell
$env:ES_INTEGRATION_TEST_ENABLED='true'
mvn '-Dtest=ElasticsearchInitialSyncIntegrationTest' test
```

执行结果：

- 集成测试：3 个通过，0 失败，0 错误，0 跳过，`BUILD SUCCESS`。
- MySQL `tb_shop`：查询到 14 条店铺记录。
- 同步服务：`syncedCount=14`。
- Elasticsearch `shop_index/_count`：14 条文档。
- Mapping 验证：`name=text`，`location=geo_point`。

集成测试启动完整 Spring 上下文时，因为本次没有启动 Kafka，日志中出现 Kafka `127.0.0.1:9092` 连接告警；该告警与 ES 同步无关，没有造成测试失败或消息链路变更。
