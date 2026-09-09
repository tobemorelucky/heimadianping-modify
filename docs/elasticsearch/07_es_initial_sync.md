# Elasticsearch 首次全量同步

## 1. 同步流程

```text
显式调用 ShopEsSyncService.syncAllShops()
                  ↓
        HEAD /shop_index 检查索引
                  ↓
       不存在则加载 Mapping 并创建
                  ↓
       ShopMapper.selectList(null)
                  ↓
          MySQL 全部 tb_shop 数据
                  ↓
       Shop 转换为 ShopDocument
       （x→lon，y→lat）
                  ↓
       每 500 条组装一批 NDJSON
                  ↓
          POST /_bulk 批量写入
                  ↓
  校验 HTTP 状态及响应 errors=false
                  ↓
       POST /shop_index/_refresh
                  ↓
          返回成功同步数量
```

## 2. 连接配置

`ElasticsearchConfig` 创建名称为 `elasticsearchRestTemplate` 的专用客户端：

- 默认地址：`http://127.0.0.1:9200`
- 连接超时：3 秒
- 读取超时：30 秒
- 可选覆盖：启动参数 `--hmdp.elasticsearch.url=http://host:9200`

默认值写在配置类中，因此本阶段没有修改 `application.yaml`。

## 3. 索引初始化

`createShopIndex()` 先通过 HEAD 判断索引是否存在：

- 不存在：读取 `classpath:elasticsearch/shop-index-mapping.json` 并执行 `PUT /shop_index`。
- 已存在：按幂等成功处理，不破坏现有数据。

如需在非首次环境彻底重建 Mapping，应先备份或确认数据可重建，再显式删除旧索引；同步服务本身不会执行破坏性删除。

## 4. Bulk 写入策略

- 批大小固定为 500，控制请求体和 JVM 临时内存。
- 请求类型为 `application/x-ndjson`，每个 action 行和 document 行均以换行结束。
- `_id` 使用 MySQL 店铺 ID，因此同一数据重复同步会覆盖而不是新增重复文档。
- 不只判断 HTTP 200，同时读取 Bulk 顶层 `errors`；存在任一失败明细即抛出异常，避免部分失败被记录为成功。
- 全量同步结束统一 refresh，仅用于初始化后立即验收；未来 Canal 实时写入不应逐条 refresh。

## 5. 新增文件

- `src/main/java/com/hmdp/config/elasticsearch/ElasticsearchConfig.java`
- `src/main/java/com/hmdp/elasticsearch/service/ShopEsSyncService.java`
- `src/main/java/com/hmdp/elasticsearch/document/ShopDocument.java`
- `src/main/resources/elasticsearch/shop-index-mapping.json`

## 6. 使用方式

本阶段没有增加 Controller 或自动启动任务，防止普通应用启动时误触发全量重建。开发环境首次同步通过独立集成测试显式执行：

```powershell
$env:ES_INTEGRATION_TEST_ENABLED='true'
mvn '-Dtest=ElasticsearchInitialSyncIntegrationTest' test
```

执行前应确保 Elasticsearch、MySQL、Redis 可用。测试结束后可检查：

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:9200/shop_index/_count'
Invoke-RestMethod -Uri 'http://127.0.0.1:9200/shop_index/_mapping'
```

## 7. 当前业务影响

- `ShopController` 未修改。
- `ShopService` 与查询实现未修改。
- Redis、Kafka、SQL、Lua 未修改。
- 当前线上/本地商户查询仍走原 MySQL/Redis 逻辑；ES 只是建立可验收的数据基线。
