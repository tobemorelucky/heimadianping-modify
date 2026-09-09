# Elasticsearch 商户索引设计

## 1. 索引定义

- 逻辑索引名：`shop_index`
- 文档 `_id`：使用 MySQL `tb_shop.id`
- 数据源：MySQL `tb_shop`，类型名称补充自 `tb_shop_type`
- 主数据原则：MySQL 为权威源，`shop_index` 可以随时全量重建
- 初期分片建议：单机开发环境 1 个 primary shard、0 个 replica；生产分片数需根据数据量和节点数压测确定，不直接照搬本地值

推荐使用独立的 `ShopDocument`，不要直接给 MyBatis `Shop` 实体叠加 Elasticsearch 注解。这样可以清晰处理字段重命名、类型名称补充和经纬度转换。

## 2. 核心 Mapping

以下为目标设计示例。中文环境建议在每个 Elasticsearch 节点安装相同版本的 IK 分词插件；索引创建前必须先验证 `ik_max_word` 和 `ik_smart` 可用。

```json
PUT /shop_index
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "long"
      },
      "name": {
        "type": "text",
        "analyzer": "ik_max_word",
        "search_analyzer": "ik_smart",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 128
          }
        }
      },
      "type": {
        "type": "keyword"
      },
      "typeId": {
        "type": "long"
      },
      "address": {
        "type": "text",
        "analyzer": "ik_max_word",
        "search_analyzer": "ik_smart",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "ik_max_word",
        "search_analyzer": "ik_smart"
      },
      "location": {
        "type": "geo_point"
      },
      "score": {
        "type": "integer"
      }
    }
  }
}
```

当前项目没有 Elasticsearch 依赖和 IK 插件配置，本段只是目标 Mapping，不表示已经执行。

## 3. 字段设计说明

| 字段 | ES 类型 | 数据来源 | 设计原因 |
| --- | --- | --- | --- |
| `id` | `long` | `tb_shop.id` | 与 MySQL 主键一致，并同时作为 ES `_id`，便于幂等 upsert、删除和按 ID 排障。无需参与全文分词。 |
| `name` | `text` + `keyword` 子字段 | `tb_shop.name` | `text` 支持中文分词和相关性检索；`name.keyword` 用于精确匹配、去重或稳定排序。名称应在多字段查询中获得最高 boost。 |
| `type` | `keyword` | `tb_shop_type.name` | 类型名称集合有限，主要用于精确过滤和聚合，不需要把“美食”“KTV”拆成多个 token；也可以在全文查询中作为精确词项提高相关性。 |
| `typeId` | `long` | `tb_shop.type_id` | 保留现有业务的类型 ID 语义，避免仅凭类型名称过滤。虽然任务核心字段为 `type`，实际兼容 `/shop/of/type` 或后续组合过滤仍需要 `typeId`。 |
| `address` | `text` + `keyword` 子字段 | `tb_shop.address` | `text` 支持道路、商场、区域等中文关键词检索；`.keyword` 可用于完全地址判断，不建议默认用于聚合大量高基数字段。 |
| `description` | `text` | 待确定权威来源 | 用于商户特色、主营业务和搜索召回。当前 `tb_shop` 没有该字段，首期不能虚构数据；可缺省为空，待数据库或运营内容建立权威字段后同步。 |
| `location` | `geo_point` | `tb_shop.x/y` | 支持距离过滤、`_geo_distance` 排序和附近搜索。写入格式必须是 `{"lat": y, "lon": x}`；当前 `x` 是经度，`y` 是纬度，不能颠倒。 |
| `score` | `integer` | `tb_shop.score` | 当前评分按实际值乘 10 保存，保留整数可避免同步时精度歧义，并支持排序或后续 function score。展示时仍按现有业务规则换算。 |

## 4. 当前模型差异

### 4.1 `type` 不在 `tb_shop`

`tb_shop` 只有 `type_id`，类型名称位于 `tb_shop_type.name`。初始化同步应通过联表或批量字典映射填充 `type`。Canal 阶段需要同时考虑：

- 店铺 `type_id` 变化时重建该店铺文档；
- 类型名称变化时批量更新使用该类型的店铺文档。

### 4.2 `description` 当前不存在

不能把拼接的名称和地址伪装成业务描述。首期可以让字段缺失或写入空字符串，但 Mapping 先保留该字段，等运营侧建立权威描述数据后再加入检索。若未来需要 AI 运营助手生成描述，仍应先审核并写入权威业务存储，再同步到 ES。

### 4.3 `location` 需要转换

数据库使用两个 Double 字段：`x` 为经度，`y` 为纬度；ES 使用一个 `geo_point`。同步转换规则固定为：

```json
{
  "location": {
    "lat": 30.316078,
    "lon": 120.149192
  }
}
```

## 5. 建议的扩展展示字段

为了让 `/shop/of/name` 继续直接返回现有商户信息，实际 `ShopDocument` 可在核心 Mapping 之外增加以下非全文字段：

| 字段 | 建议类型 | 用途 |
| --- | --- | --- |
| `images` | `keyword`，`index: false` | 返回店铺图片字符串，不参与搜索。 |
| `area` | `text` + `keyword` | 商圈检索和精确过滤。 |
| `avgPrice` | `long` | 价格范围过滤或排序。 |
| `sold` | `integer` | 销量展示和排序。 |
| `comments` | `integer` | 评论数展示。 |
| `openHours` | `keyword`，可 `index: false` | 营业时间展示。 |
| `updateTime` | `date` | 同步校验、排障和重建对账。 |

另一种方案是 ES 只返回 ID，再批量回查 MySQL/Redis 补齐详情。该方案会增加一次数据访问和顺序恢复逻辑。项目当前数据规模较小，实施阶段可按返回字段一致性和性能压测二选一；首期建议文档包含展示字段，减少搜索请求回源。

## 6. 查询与排序建议

### 6.1 多字段检索

```json
{
  "query": {
    "multi_match": {
      "query": "北京火锅",
      "fields": ["name^4", "type^2", "address^2", "description"],
      "type": "best_fields"
    }
  },
  "sort": [
    "_score",
    {"score": "desc"},
    {"id": "asc"}
  ]
}
```

名称设置最高权重，类型和地址其次，描述用于扩大召回。同分时使用业务评分和 ID，保证翻页顺序稳定。

### 6.2 分页

- 兼容现有浅分页时使用 `from + size`，每页仍为 10 条。
- 必须限制最大页数或最大 `from`，防止深分页消耗过多内存。
- 若未来需要无限滚动，改用 `search_after`，并保持 `score + id` 等稳定排序键。

## 7. Mapping 变更原则

字段类型和 analyzer 创建后不能直接无损修改。后续重大 Mapping 变更应创建版本化新索引，例如 `shop_index_v2`，执行全量重建和验证后再通过别名切换。首期若保持简单，可直接使用 `shop_index`；进入生产迭代后建议把 `shop_index` 作为读别名。

