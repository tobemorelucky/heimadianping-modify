# AI Assistant Phase 3 业务工具测试记录

## 一、测试环境

- 日期：2026-09-10
- Spring Boot：2.3.12.RELEASE，端口 8081
- FastAPI/Uvicorn：端口 8000
- LLM：本地 `.env` 配置的 OpenAI-compatible DeepSeek 模型
- MySQL：本地 `hmdp` 数据库
- Redis：一次性 Redis 7 测试容器，仅用于启动既有 Spring 上下文
- Canal Client：测试与联调时关闭
- API Key：未输出、未写入本文档

## 二、自动化测试

### 2.1 Java 专项测试

执行：

```bash
mvn -Dtest=AiOrderStatisticsControllerTest,AiCouponStatisticsControllerTest,AiBusinessStatisticsServiceImplTest test
```

结果：7 个测试通过，0 失败，0 错误，`BUILD SUCCESS`。

覆盖内容：

- 两个 AI 专用接口的成功响应和店铺不存在响应。
- 最近 7 天订单统计调用。
- 优惠券使用率计算与四舍五入。
- 店铺不存在时不执行聚合 SQL。

### 2.2 Python Tool 与 Agent 测试

执行：

```bash
python -B -m unittest discover -s tests -v
```

结果：4 个测试通过，0 失败，0 错误。

覆盖内容：

- `get_shop_info` HTTP 调用和字段白名单。
- `get_order_statistics` 路径、解析和字段白名单。
- `get_coupon_statistics` 路径、解析和字段白名单。
- 单个 Agent 回合并行产生订单与优惠券两个 Tool Call，ToolNode 执行后返回最终答案。

### 2.3 完整 Maven 测试

首次执行时，本地 Redis 未运行，5 个既有 Spring 上下文测试因无法连接 `127.0.0.1:6379` 报错；这不是本阶段代码失败。启动一次性 Redis 并设置 `CANAL_ENABLED=false` 后重新执行完整 `mvn test`：

- 总数：52
- 失败：0
- 错误：0
- 跳过：3（需独立外部环境的既有集成测试）
- 结果：`BUILD SUCCESS`

Kafka 未启动时出现 Broker 连接警告，但不影响测试结果，也未对 Kafka 配置或代码做任何修改。

## 三、真实接口验证

### 3.1 Spring Boot AI 接口

`GET /api/ai/shop/1/orders`：

```json
{"success":true,"data":{"orderCount":0,"transactionAmount":0.0}}
```

`GET /api/ai/shop/1/coupons`：

```json
{"success":true,"data":{"issuedCount":0,"usedCount":0,"usageRate":0.0}}
```

本地店铺 1 当前没有满足口径的业务记录，因此零值与数据库现状一致。

## 四、真实 DeepSeek 多 Tool Calling

请求：

```http
POST /chat
Content-Type: application/json

{"message":"分析一下店铺1最近经营情况"}
```

真实 LangGraph 轨迹提取结果：

```text
TOOLS=get_order_statistics,get_coupon_statistics
```

说明 DeepSeek 在同一轮分析中自主选择并调用了两个新增工具，ToolNode 分别通过 HTTP 获取 Spring Boot 数据，然后模型生成最终经营分析。`POST /chat` 返回 HTTP 200；回答正确指出最近 7 天订单数和交易金额均为 0，并结合累计优惠券发放、使用和使用率均为 0 给出运营建议。

一次额外请求中，模型还主动调用 `get_shop_info` 补充了店铺名称“103茶餐厅”、类型和地址，表明新增工具与原有工具可以在同一 Agent 循环内协同。

## 五、结论

Phase 3 已通过 Java 接口测试、Python Tool 测试、全量 Maven 测试和真实 DeepSeek 多 Tool Calling 联调。数据访问边界仍为 `Python → Spring Boot → MySQL`，没有引入数据库直连、RAG、向量库、Memory、MCP 或多 Agent，也没有改动 Kafka、Elasticsearch、Canal 和秒杀逻辑。
