# AI Assistant Phase 3 业务分析工具设计

## 一、目标与边界

本阶段在已有 `get_shop_info` 的基础上增加两个只读业务工具：

- `get_order_statistics`：查询指定店铺最近 7 天订单数量和交易金额。
- `get_coupon_statistics`：查询指定店铺累计优惠券发放数量、使用数量和使用率。

Python AI 服务只能通过 Spring Boot AI 专用接口读取业务数据，不持有 MySQL、Redis 等基础设施凭据。本阶段不引入 RAG、向量数据库、持久化 Memory、MCP 或多 Agent，也不改变 Kafka、Elasticsearch、Canal 和秒杀链路。

## 二、架构

```text
用户问题
   ↓
FastAPI POST /chat
   ↓
LangGraph Agent Node（DeepSeek + Tool Calling）
   ↓ 选择一个或多个工具
ToolNode
   ├── get_shop_info
   ├── get_order_statistics
   └── get_coupon_statistics
   ↓ HTTP
Spring Boot AI 专用 Controller
   ↓
AiBusinessStatisticsService
   ↓
VoucherOrderMapper 聚合查询
   ↓
MySQL
   ↓
Tool Result → Agent Node → 自然语言回答
```

LangGraph 仍是单 Agent 的 `Agent → ToolNode → Agent` 循环。对于“分析店铺经营情况”这类复合问题，系统指令要求模型同时取得订单和优惠券统计，再统一生成结论。

## 三、Java 接口设计

### 3.1 订单统计

`GET /api/ai/shop/{id}/orders`

成功响应中的 `data`：

```json
{
  "orderCount": 8,
  "transactionAmount": 128.50
}
```

统计口径：

- 时间窗口为请求时刻向前 7 天。
- 排除已取消、退款中、已退款订单，即状态 `4、5、6`。
- 交易金额使用优惠券 `pay_value` 汇总，并从分转换为元。

### 3.2 优惠券统计

`GET /api/ai/shop/{id}/coupons`

成功响应中的 `data`：

```json
{
  "issuedCount": 20,
  "usedCount": 5,
  "usageRate": 25.00
}
```

统计口径：

- 发放数量：该店铺优惠券产生的累计订单记录数。
- 使用数量：订单状态为已核销，或存在核销时间的记录数。
- 使用率：`usedCount / issuedCount × 100%`，无发放记录时为 `0.00`，保留两位小数。

两个 Controller 仅负责路径映射与统一 `Result` 包装，店铺存在性校验、时间窗口和使用率计算位于业务服务，SQL 聚合位于 Mapper，避免把业务逻辑堆入 Controller。

## 四、Python Tool 设计

| Tool | Spring Boot 接口 | 返回字段 | 权限 |
| --- | --- | --- | --- |
| `get_shop_info` | `/api/ai/shop/{id}` | id、name、type、address | 只读 |
| `get_order_statistics` | `/api/ai/shop/{id}/orders` | orderCount、transactionAmount | 只读 |
| `get_coupon_statistics` | `/api/ai/shop/{id}/coupons` | issuedCount、usedCount、usageRate | 只读 |

每个 Tool 都校验正数店铺 ID，通过 `SPRING_BOOT_BASE_URL` 组装地址，使用 5 秒 HTTP 超时，并对白名单字段重新组装返回结果。Spring Boot 的失败响应、HTTP 异常和非法 JSON 会转换为 `ToolException`，交给 Agent 调用链处理。

## 五、安全设计

- Python 不连接 MySQL，不读取数据库账号。
- Agent 只能调用显式注册到 `TOOLS` 的三个只读工具。
- Java AI Controller 不提供新增、修改、删除接口。
- Tool 对 Java 响应实施字段白名单，避免 DTO 扩展时意外泄露字段。
- `.env` 保持 Git 忽略，LLM 密钥不进入代码、文档和日志。
- 请求状态只在当前图执行中保存，不构成 Agent Memory。

## 六、文件改造

Java 新增两个 Controller、两个统计 DTO、一个统计 Service 接口及实现；在 `VoucherOrderMapper` 增加两条只读聚合查询。Python 新增订单与优惠券 Tool，并将其注册到现有 LangGraph `ToolNode`。测试分别覆盖 Controller 协议、业务口径、HTTP Tool 代理和多 Tool 图循环。
