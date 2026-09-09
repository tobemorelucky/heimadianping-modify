# Elasticsearch 商户搜索测试记录

## 1. 测试日期

2026-09-09

## 2. 隔离搜索测试

测试文件：`src/test/java/com/hmdp/elasticsearch/service/ShopSearchServiceImplTest.java`

使用 `MockRestServiceServer` 模拟 ES HTTP 响应，使用 Mockito 模拟 `ShopMapper`。测试不依赖真实 ES 或修改外部数据。

| 测试场景 | 验证内容 | 结果 |
| --- | --- | --- |
| 正常关键词搜索 | 非空关键词发送 `match name`，命中 ID 后批量查询完整 Shop 并转为 ShopDTO | 通过 |
| 无结果搜索 | ES hits 为空时返回空列表且不查询 MySQL 详情 | 通过 |
| ES 异常降级 | ES 返回 500 时记录异常并调用 MySQL LIKE 分页 | 通过 |
| 排序保持 | Mapper 返回顺序与 ES 不同时，最终结果仍按 ES hits 顺序 | 通过 |
| 空关键词兼容 | 空关键词不访问 ES，保持 MySQL 无 LIKE 条件分页 | 通过 |

执行：

```powershell
mvn '-Dtest=ShopSearchServiceImplTest' test
```

结果：5 个测试通过，0 失败，0 错误，0 跳过。

异常降级用例会按预期打印一次 ES 500 错误堆栈，这是被验证的降级路径，不是测试失败。

## 3. HTTP 接口契约测试

测试文件：`src/test/java/com/hmdp/controller/ShopControllerSearchTest.java`

验证：

- 原路径 `/shop/of/name` 保持可用。
- `name`、`current` 参数原样传给 `ShopSearchService`。
- HTTP 状态为 200。
- `success`、`data` 包装不变。
- 返回数据仍包含 `id`、`name`、`images`、`avgPrice` 等原 Shop 字段。

与搜索服务测试组合执行：

```powershell
mvn '-Dtest=ShopSearchServiceImplTest,ShopControllerSearchTest' test
```

结果：6 个测试通过，0 失败，0 错误，0 跳过。

## 4. 真实 Elasticsearch 冒烟验证

对本地 `shop_index` 执行只读 `match name=火锅` 查询，按 `_score desc`、`id asc` 排序。

实际命中：

| 顺序 | ID | 名称 | `_score` |
| --- | ---: | --- | ---: |
| 1 | 5 | 海底捞火锅(水晶城购物中心店） | 2.7038403 |
| 2 | 9 | 羊老三羊蝎子牛仔排北派炭火锅(运河上街店) | 2.224679 |
| 3 | 6 | 幸福里老北京涮锅（丝联店） | 1.1707139 |
| 4 | 2 | 蔡馬洪涛烤肉·老北京铜锅涮羊肉 | 1.049006 |

结果说明 `shop_index` 可以执行本次实现使用的 match 和排序 DSL。当前 Mapping 使用 `standard` analyzer，其中文召回只是基础效果，中文分词优化不属于本次简单搜索接入范围。

## 5. 完整回归测试

执行：

```powershell
mvn test
```

最终结果：

- 测试总数：35
- 失败：0
- 错误：0
- 跳过：2
- Maven：`BUILD SUCCESS`
- 总耗时：44.662 秒

跳过的是需要显式环境开关的集成测试。完整 Spring 上下文测试期间本地 Kafka 未启动，因此出现 `127.0.0.1:9092` 连接告警；这不影响测试结果，也不是本次 ES 搜索改造产生的业务失败。

## 6. 测试结论

- 指定的四类核心场景全部通过。
- 额外覆盖了空关键词兼容和 Controller 协议。
- 非空关键词搜索走 ES；空关键词和 ES 异常走 MySQL。
- MySQL 详情批量查询不会破坏 ES 相关性顺序。
- 其他店铺、Redis 和 Kafka 既有测试未受影响。
