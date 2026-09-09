# Elasticsearch 商户文档设计

## 1. 实现边界

本阶段新增独立的 `ShopDocument`，但不替换 `ShopController`、`ShopService` 或现有 MySQL/Redis 查询链路。同步能力只由 `ShopEsSyncService` 和显式启用的集成测试调用。

项目仍以 Java 8、Spring Boot 2.3.12 为基线。为避免引入与该基线不匹配的新一代 Elasticsearch Java Client，本阶段复用 Spring Web 的 `RestTemplate` 调用 Elasticsearch 8 REST API，没有新增 Maven 依赖。

## 2. shop_index Mapping

| 文档字段 | ES 类型 | MySQL 来源 | 设计说明 |
| --- | --- | --- | --- |
| `id` | `long` | `tb_shop.id` | 同时作为 ES `_id`，重复同步覆盖同一文档 |
| `name` | `text` + `keyword` | `tb_shop.name` | `text` 用于检索，`keyword` 预留精确匹配/聚合 |
| `typeId` | `long` | `tb_shop.type_id` | 店铺类型精确过滤 |
| `area` | `text` + `keyword` | `tb_shop.area` | 兼顾文本检索和商圈过滤 |
| `address` | `text` | `tb_shop.address` | 地址全文检索 |
| `description` | `text` | 当前无权威字段 | 模型保留；首次同步不虚构内容，JSON 中省略空值 |
| `location` | `geo_point` | `tb_shop.y`/`tb_shop.x` | `lat=y`、`lon=x`，支持距离过滤和排序 |

Mapping 使用 `dynamic: strict`，防止拼写错误或意外字段被动态写入。当前官方基础镜像未安装 IK 插件，因此首期采用内置 `standard` analyzer；中文搜索上线前应确定 IK 或其他中文分析器，并通过新索引重建完成分析器迁移。

## 3. Java 模型

`ShopDocument` 与数据库实体 `Shop` 分离，避免 ES 字段设计反向污染数据库模型。嵌套 `Location` 使用 Elasticsearch 接受的对象形式：

```json
{
  "lat": 30.310633,
  "lon": 120.157780
}
```

`@JsonInclude(NON_NULL)` 用于省略当前没有数据来源的 `description` 和缺失坐标，避免把空字段误认为有效搜索数据。

## 4. 新增文件

- `src/main/java/com/hmdp/elasticsearch/document/ShopDocument.java`：ES 商户文档及地理坐标模型。
- `src/main/resources/elasticsearch/shop-index-mapping.json`：`shop_index` 的显式 settings 与 mapping。

## 5. 后续约束

- 搜索接口启用前必须先选定中文分词策略。
- Canal 增量同步继续使用店铺 ID 作为 `_id`，保证 upsert 幂等。
- 如果数据库增加店铺描述字段，应先明确数据来源，再补充 `Shop -> ShopDocument` 映射。
