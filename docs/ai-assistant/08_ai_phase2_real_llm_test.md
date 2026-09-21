# AI Assistant Phase 2 真实 LLM 联调记录

## 1. 测试目标

验证 DeepSeek OpenAI 兼容模型能否根据用户问题“查询店铺1的信息”自主生成 `get_shop_info` Tool Call，完成以下真实链路：

```text
POST /chat
  ↓
FastAPI
  ↓
LangGraph Agent
  ↓
DeepSeek LLM 选择 get_shop_info
  ↓
Python HTTP Tool
  ↓
Spring Boot /api/ai/shop/1
  ↓
Tool Result
  ↓
DeepSeek 生成自然语言回答
```

测试日期：2026-09-09。

## 2. 模型配置说明

### 2.1 配置读取机制检查

`ai-assistant/app/config.py` 的读取链路为：

1. `load_dotenv(AI_ASSISTANT_ROOT / ".env")` 加载 AI 服务目录下的本地 `.env`。
2. `os.getenv("LLM_BASE_URL", "")` 读取 OpenAI 兼容 API 根地址。
3. `os.getenv("LLM_API_KEY", "")` 读取 API Key。
4. `os.getenv("LLM_MODEL", "")` 读取模型名称。
5. 首次构造 LLM Client 时调用 `validate_llm()`，任一变量为空则拒绝调用。

检查结论：三项 LLM 配置均来自环境变量或未提交的本地 `.env`，代码中没有硬编码 API Key。

### 2.2 本次配置状态

| 配置项 | 本地 `.env` | 当前进程环境 | 结果 |
|---|---|---|---|
| `LLM_BASE_URL` | 文件不存在 | 未设置 | 缺失 |
| `LLM_API_KEY` | 文件不存在 | 未设置 | 缺失 |
| `LLM_MODEL` | 文件不存在 | 未设置 | 缺失 |

本记录不包含 API Key，也没有创建、猜测或提交任何真实密钥。因为模型名也未实际注入，本次不能声称调用了 `.env.example` 中的示例模型。

## 3. 服务启动

### 3.1 Spring Boot

为满足原项目启动依赖，启动一次性 Redis 7 测试容器，并以 `CANAL_ENABLED=false` 启动 Spring Boot：

```powershell
docker run --rm -d --name hmdp-ai-llm-test-redis -p 6379:6379 redis:7-alpine
$env:CANAL_ENABLED='false'
mvn spring-boot:run
```

结果：Spring Boot 启动成功，监听 `127.0.0.1:8081`。

使用真实请求检查业务接口：

```http
GET http://127.0.0.1:8081/api/ai/shop/1
```

结果：`success=true`，说明 Java 只读业务出口、MySQL 查询和运行依赖可用。

Kafka 本轮没有启动，Spring 日志存在既有 Consumer 的连接重试警告，但不影响店铺只读接口，也没有修改 Kafka 配置或逻辑。

### 3.2 FastAPI

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -B -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

结果：Uvicorn 启动成功，FastAPI application startup complete，监听 `127.0.0.1:8000`。

## 4. 测试请求

```http
POST http://127.0.0.1:8000/chat
Content-Type: application/json; charset=utf-8
```

```json
{
  "message": "查询店铺1的信息"
}
```

## 5. 实际 Agent 流程

本次实际执行路径为：

```text
FastAPI /chat
  ↓
LangGraph START
  ↓
Agent Node
  ↓
创建 LLM Client 前执行 validate_llm()
  ↓
检测到 LLM_BASE_URL / LLM_API_KEY / LLM_MODEL 缺失
  ↓
FastAPI 返回 503
```

流程在 DeepSeek 请求发出前终止。因此：

- DeepSeek 没有收到本次请求。
- 没有产生 LLM Tool Call。
- `tools_condition` 没有路由到 ToolNode。
- `get_shop_info` 没有被本次 Agent 调用。
- Spring Boot 就绪检查访问了店铺接口，但这不是 Agent 触发的 Tool 调用，不能作为 Tool Calling 成功证据。

## 6. Tool 调用结果

| 验证项 | 状态 | 说明 |
|---|---|---|
| Spring Boot 店铺接口可用 | 通过 | 独立就绪检查返回 `success=true` |
| DeepSeek 接收用户问题 | 未执行 | LLM 配置校验失败 |
| LLM 触发 `get_shop_info` | 未通过 | 没有产生 Tool Call |
| Tool HTTP 请求由 Agent 发起 | 未执行 | Graph 未进入 ToolNode |
| LLM 基于 Tool Result 生成回答 | 未执行 | 没有模型请求和 Tool Result |

Phase 2 上一阶段已经使用确定性假模型验证过 Agent → ToolNode → 真实 Spring Boot → MySQL → Agent 的代码和 HTTP 数据链路；该结果不能替代本次要求的真实 DeepSeek 决策验证。

## 7. 返回结果

HTTP 状态：`503 Service Unavailable`。

响应：

```json
{
  "detail": "Missing required environment variables: LLM_BASE_URL, LLM_API_KEY, LLM_MODEL"
}
```

该响应只包含缺失的变量名，没有输出或记录任何配置值。

## 8. 测试结论

本次真实 LLM Tool Calling 联调状态：**未通过，环境配置阻塞**。

已确认：

- `.env` 和环境变量读取机制正确。
- Spring Boot 与 FastAPI 均能启动。
- Spring Boot 店铺查询接口可用。
- 缺少 LLM 配置时服务安全失败，不泄露密钥。

未确认：

- DeepSeek 模型是否支持并实际产生 `get_shop_info` Tool Call。
- DeepSeek 是否能基于真实 Tool Result 返回自然语言店铺信息。

## 9. 复测前置条件与步骤

由使用者在 `ai-assistant/.env` 本地配置有效值，文件继续由 `.gitignore` 排除：

```dotenv
LLM_BASE_URL=<OpenAI兼容API地址>
LLM_API_KEY=<本地真实密钥>
LLM_MODEL=<实际支持Tool Calling的模型名>
SPRING_BOOT_BASE_URL=http://localhost:8081
```

不要把 `.env` 内容粘贴到开发日志、提交记录或测试报告。配置后重新启动两个服务并重复第 4 节请求；成功标准为：

1. HTTP `/chat` 返回 200。
2. LLM 首次响应包含名为 `get_shop_info` 的 Tool Call，参数 `shop_id=1`。
3. Spring Boot 访问日志出现由 Python Tool 发起的 `/api/ai/shop/1`。
4. Graph 出现 ToolMessage 并再次调用 LLM。
5. 最终 `answer` 正确包含店铺 1 的真实基础信息。

## 10. 环境清理

验证结束后已停止 Spring Boot、FastAPI，并停止自动删除一次性 Redis 容器。未修改任何源代码、应用配置、SQL、Lua、Kafka、Elasticsearch 或 Canal 逻辑。
