# AI 运营助手最终架构

## 一、模块定位

AI 运营助手是 HM-DianPing Plus 的独立 Python 服务。它负责理解自然语言问题、选择只读业务工具并组织回答；Spring Boot 仍是唯一业务数据出口和业务口径所有者。

本模块保持以下边界：

- 不实现 RAG 和向量数据库。
- 不保存 Agent Memory，每个请求相互独立。
- 不使用 MCP 或多 Agent。
- Python 不直接连接 MySQL、Redis、Kafka、Elasticsearch 或 Canal。
- Agent 不具备新增、修改、删除业务数据的能力。

## 二、整体架构

```text
运营人员
   │ POST /chat
   ▼
FastAPI AI Service
   │
   ▼
LangGraph StateGraph
   │
   ├── Agent Node
   │     └── OpenAI-compatible DeepSeek LLM
   │
   └── ToolNode（只读工具白名单）
         ├── get_shop_info
         ├── get_order_statistics
         └── get_coupon_statistics
                   │ HTTP
                   ▼
Spring Boot AI 专用接口
   ├── AiShopController
   ├── AiOrderStatisticsController
   └── AiCouponStatisticsController
                   │
                   ▼
业务 Service / Mapper
                   │
                   ▼
                 MySQL
```

FastAPI 对外提供统一的 `POST /chat`，请求协议为 `{"message":"..."}`，响应协议为 `{"answer":"..."}`。LLM、Spring Boot 地址均从本地环境变量读取，真实密钥不进入代码仓库。

## 三、LangGraph 流程

```text
START
  ↓
Agent Node
  ├── 无需业务数据 ───────────────→ END
  │                                  输出自然语言回答
  │
  └── 产生一个或多个 tool_calls
                    ↓
                 ToolNode
                    ↓
              ToolMessage 结果
                    ↓
                 Agent Node
                    ├── 仍需数据 → ToolNode
                    └── 信息充分 → END
```

`AgentState` 保存当前请求的原始问题、最终答案和本轮消息列表。消息列表仅服务一次 Graph 调用，不跨请求持久化，因此不是 Agent Memory。

Agent 使用固定工具白名单和系统指令：业务事实必须来自工具，不允许臆造；分析店铺经营情况时必须同时取得订单与优惠券统计，并准确区分订单最近 7 天、优惠券累计两个统计口径。

## 四、Tool Calling 流程

一次经营分析请求的完整路径如下：

```text
“分析一下店铺1最近经营情况”
              ↓
DeepSeek 判断需要经营数据
              ↓
tool_calls:
  get_order_statistics(shop_id=1)
  get_coupon_statistics(shop_id=1)
              ↓
ToolNode 校验参数并执行 Python Tool
              ↓
HTTP GET Spring Boot AI 专用接口
              ↓
Spring Boot 查询并聚合 MySQL 数据
              ↓
Tool 仅保留白名单字段
              ↓
ToolMessage 返回 LangGraph
              ↓
DeepSeek 基于真实结果生成运营分析
```

工具使用 5 秒 HTTP 超时。非法店铺 ID、HTTP 异常、非法 JSON、业务失败响应会转换为 Tool 异常，不会绕过 Spring Boot 或改写数据库。

## 五、Spring Boot 与 Python 职责边界

| 领域 | Spring Boot | Python AI 服务 |
| --- | --- | --- |
| 业务数据 | 唯一数据出口 | 不直接连接数据库 |
| 业务口径 | 校验店铺、定义时间范围、聚合统计 | 使用接口返回结果，不重算业务数据 |
| 接口权限 | 提供 AI 专用只读 Controller | 仅调用允许的 HTTP 地址 |
| 自然语言 | 不负责 | 理解意图、选择工具、生成回答 |
| LLM 调用 | 不负责 | 通过 OpenAI-compatible API 调用 DeepSeek |
| 状态 | 管理原有业务状态 | 请求级无状态，不保存 Memory |
| 安全 | DTO 限定业务返回 | Tool 字段白名单、无写工具、密钥环境化 |

这样的分工避免 Python 复制 Java 业务规则，也防止 Agent 绕过既有权限和服务边界直接操作数据库。

## 六、三个业务 Tool

### 6.1 get_shop_info

- Java 接口：`GET /api/ai/shop/{id}`
- 返回：`id`、`name`、`type`、`address`
- 用途：回答店铺名称、类型、地址等基础信息问题。
- 数据来源：现有店铺业务服务。

### 6.2 get_order_statistics

- Java 接口：`GET /api/ai/shop/{id}/orders`
- 返回：`orderCount`、`transactionAmount`
- 用途：分析店铺最近 7 天订单量与交易金额。
- 口径：排除取消、退款中、已退款订单；金额由分转换为元。

### 6.3 get_coupon_statistics

- Java接口：`GET /api/ai/shop/{id}/coupons`
- 返回：`issuedCount`、`usedCount`、`usageRate`
- 用途：分析累计优惠券领取、核销和使用效率。
- 口径：使用率为 `usedCount / issuedCount × 100%`，保留两位小数；无发放记录时为 0。

## 七、最终验证状态

- Java AI Controller 与统计 Service 单元测试通过。
- Python 三个 Tool 与多 Tool Graph 测试通过。
- 完整 Maven 测试：52 个，0 失败，0 错误，3 个外部环境集成测试跳过。
- 真实 DeepSeek Tool Calling 已验证。
- “分析一下店铺1最近经营情况”真实轨迹包含 `get_order_statistics` 和 `get_coupon_statistics`，随后生成自然语言结论。
- 本地 `.env` 已被 Git 忽略，文档与日志未保存 API Key。
