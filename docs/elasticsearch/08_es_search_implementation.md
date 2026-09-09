# Elasticsearch Phase 3：商户搜索实现记录

## 1. 实现结果

`GET /shop/of/name` 的非空名称搜索已经从 Controller 内部的 MySQL `LIKE` 切换为 Elasticsearch `match` 查询。接口路径、请求参数、默认页码、每页数量和 `Result` 包装保持不变，其他店铺接口未调整。

最终链路：

```text
GET /shop/of/name?name=火锅&current=1
                  ↓
            ShopController
                  ↓
          ShopSearchService
                  ↓
POST /shop_index/_search（match name）
                  ↓
       ES 返回有序店铺 ID
                  ↓
  ShopMapper.selectBatchIds 批量查询
                  ↓
       按 ES ID 顺序恢复结果
                  ↓
         Shop 转换为 ShopDTO
                  ↓
       Result.ok(List<ShopDTO>)
```

## 2. 新增代码

### ShopDTO

文件：`src/main/java/com/hmdp/dto/ShopDTO.java`

项目原本没有 `ShopDTO`。本次新增的 DTO 完整保留原名称搜索返回的 Shop 字段：`id`、`name`、`typeId`、`images`、`area`、`address`、`x`、`y`、`avgPrice`、`sold`、`comments`、`score`、`openHours`、`createTime`、`updateTime`、`distance`。

因此 Java 内部从数据库实体切换到 DTO，但 JSON 字段协议不变，并避免 MyBatis 实体直接成为搜索接口契约。

### ShopSearchService

文件：`src/main/java/com/hmdp/elasticsearch/service/ShopSearchService.java`

提供：

```java
List<ShopDTO> searchByName(String keyword, Integer current);
```

服务接口不返回 `Result`，HTTP 包装仍由 Controller 负责。

### ShopSearchServiceImpl

文件：`src/main/java/com/hmdp/elasticsearch/service/impl/ShopSearchServiceImpl.java`

职责：

1. 将空值或小于 1 的页码规范为第 1 页。
2. 非空关键词构造 `match name` 查询。
3. 设置 `from`、`size=10`、`_score desc` 和 `id asc`。
4. 只从 ES `_source` 读取 `id`，降低搜索响应体积。
5. 使用 `LinkedHashSet` 保持 hits 顺序并去除异常重复 ID。
6. 使用 `ShopMapper.selectBatchIds` 一次性查询完整店铺数据。
7. 将数据库结果转换成 Map，再按 ES ID 顺序恢复相关性排序。
8. 将完整 `Shop` 转换为字段兼容的 `ShopDTO`。
9. ES 连接、HTTP、JSON 或响应格式异常时记录错误并降级到原 MySQL LIKE。

## 3. Elasticsearch 查询

实际请求结构：

```json
{
  "from": 0,
  "size": 10,
  "_source": ["id"],
  "query": {
    "match": {
      "name": {
        "query": "火锅"
      }
    }
  },
  "sort": [
    { "_score": { "order": "desc" } },
    { "id": { "order": "asc" } }
  ]
}
```

- `match` 使用 `name` 字段的 analyzer 处理关键词。
- `_score` 表达相关性。
- `id` 是同分情况下的稳定排序字段。
- MySQL 批量查询不保证返回顺序，所以实现必须按 ES hits 重新组装。

本阶段没有增加多字段搜索、高亮、拼音、联想或复杂打分。

## 4. 空关键词处理

原接口在 `name` 为空时不会追加 LIKE 条件，而是分页读取 MySQL 全部店铺。本次严格保留该行为：

```text
keyword 为空或全是空格
          ↓
跳过 Elasticsearch
          ↓
MySQL 无 LIKE 条件分页
```

这样避免把原有“浏览全部店铺”的请求语义误改为 ES 空 match 无结果。

## 5. 异常降级

以下异常触发降级：

- Elasticsearch 连接或读取失败。
- Elasticsearch 返回非成功状态。
- 搜索响应为空或缺少 `hits.hits` 数组。
- JSON 序列化或解析失败。
- 命中文档缺少有效 Long 类型店铺 ID。

降级流程：

```text
ES 异常
  ↓
error 日志记录 keyword、页码和异常堆栈
  ↓
MySQL name LIKE 分页
  ↓
ShopDTO 列表
```

没有使用 `catch (Exception)` 吞掉所有编程错误，也没有引入熔断、重试或复杂搜索框架。

## 6. 接口切换

修改文件：`src/main/java/com/hmdp/controller/ShopController.java`

仅 `queryShopByName` 改为调用：

```java
shopSearchService.searchByName(name, current)
```

保持不变：

- `GET /shop/of/name`
- 参数 `name`
- 参数 `current`，默认 1
- 每页 10 条
- `Result.ok(data)` 响应包装
- `GET /shop/{id}` 详情缓存流程
- `POST /shop`、`PUT /shop`
- `GET /shop/of/type` 的 MySQL/Redis GEO 流程
- Redis、Kafka、Lua 和秒杀业务

## 7. 文件清单

### 新增

- `src/main/java/com/hmdp/dto/ShopDTO.java`
- `src/main/java/com/hmdp/elasticsearch/service/ShopSearchService.java`
- `src/main/java/com/hmdp/elasticsearch/service/impl/ShopSearchServiceImpl.java`
- `src/test/java/com/hmdp/elasticsearch/service/ShopSearchServiceImplTest.java`
- `src/test/java/com/hmdp/controller/ShopControllerSearchTest.java`
- `docs/elasticsearch/08_es_search_implementation.md`
- `docs/elasticsearch/09_es_search_test.md`

### 修改

- `src/main/java/com/hmdp/controller/ShopController.java`
- `docs/development-log.md`

### 删除

无。
