# AI Assistant Phase 2 真实 DeepSeek Tool Calling 联调结果

## 1. 测试目标

验证真实 DeepSeek 模型驱动 LangGraph Agent，根据“查询店铺1的信息”自主调用 `get_shop_info`，通过 Spring Boot 获取业务数据并生成自然语言回答。

测试日期：2026-09-09。

## 2. 模型配置

本次使用以下非敏感配置：

- Base URL：`https://ark.cn-beijing.volces.com/api/v3`
- 模型：`deepseek-v4-pro-ga-260813`
- Spring Boot：`http://localhost:8081`

API Key 仅保存在本地 `ai-assistant/.env`，由 `config.py` 通过环境变量读取；本文档和运行日志均未记录其内容。`.env` 继续由 `ai-assistant/.gitignore` 排除。

联调前使用方舟 `/ping` 和最小 Chat Completions 请求验证配置，均返回 HTTP 200。

## 3. 服务与请求

启动一次性 Redis 后，以 `canal.enabled=false` 启动 Spring Boot；服务成功监听 8081。随后启动 FastAPI：

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -B -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

测试请求：

```http
POST http://127.0.0.1:8000/chat
Content-Type: application/json; charset=utf-8
```

```json
{
  "message": "查询店铺1的信息"
}
```

## 4. Agent 执行流程

真实 LangGraph 消息轨迹为：

```text
HumanMessage
  ↓
AIMessage(tool_calls=[get_shop_info])
  ↓
ToolMessage(name=get_shop_info)
  ↓
AIMessage(最终自然语言回答)
```

完整业务链路：

```text
用户请求
  ↓
FastAPI POST /chat
  ↓
LangGraph Agent Node
  ↓
DeepSeek 选择 get_shop_info
  ↓
ToolNode 执行 Python HTTP Tool
  ↓
GET Spring Boot /api/ai/shop/1
  ↓
Spring Boot 查询 MySQL
  ↓
Tool Result 返回 Agent
  ↓
DeepSeek 生成最终回答
```

## 5. Tool 调用结果

Spring Boot 返回成功，业务数据为：

```json
{
  "id": 1,
  "name": "103茶餐厅",
  "type": "美食",
  "address": "金华路锦昌文华苑29号"
}
```

轨迹检查确认首个 AI 响应的 Tool Call 名称为 `get_shop_info`，随后存在同名 `ToolMessage`，最终 AI 响应不再请求工具并生成 `answer`。

## 6. 最终返回

`POST /chat` 返回 HTTP 200，最终回答：

```text
店铺1的信息如下：

- ID：1
- 名称：103茶餐厅
- 类型：美食
- 地址：金华路锦昌文华苑29号
```

## 7. 测试结论

AI Assistant Phase 2 真实 DeepSeek Tool Calling 联调通过：

- `.env` 配置读取正确。
- DeepSeek 请求成功。
- 模型自主产生 `get_shop_info` Tool Call。
- LangGraph 正确路由至 ToolNode 并返回 Agent Node。
- Python Tool 成功访问 Spring Boot 专用只读接口。
- Spring Boot 返回真实店铺数据。
- 模型根据 Tool Result 生成正确自然语言回答。
- 未引入数据库直连、RAG、向量库或 Agent Memory。

## 8. 配置问题修正

首次配置的 `LLM_API_KEY` 在复制过程中多出一个字符段，方舟网关在鉴权阶段返回 HTTP 401。将其替换为另一套已验证环境中的完整 `ARK_API_KEY` 后，方舟 `/ping`、最小模型请求和完整 Agent 链路均恢复正常。本文档不记录任何密钥内容。

## 9. 环境清理

验证完成后停止 Spring Boot、FastAPI 和一次性 Redis 容器，不保留联调进程。Kafka、Elasticsearch、Canal、Lua 和秒杀代码均未修改。
