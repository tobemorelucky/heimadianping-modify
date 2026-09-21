# HM-DianPing Plus AI Assistant

独立的 Python AI 服务。Phase 2 在 FastAPI、LangGraph 和 OpenAI 兼容 LLM 基础上加入第一个只读业务 Tool；Python 不连接 HM-DianPing Plus 的数据库或任何中间件。

## 运行要求

- Python 3.11（建议）
- 可访问的 OpenAI 兼容 LLM API
- 由使用者提供的 API Key

## 安装

在仓库根目录执行：

```powershell
cd ai-assistant
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 配置

复制示例文件并填写本地值：

```powershell
Copy-Item .env.example .env
```

```dotenv
LLM_BASE_URL=https://your-openai-compatible-endpoint/v1
LLM_API_KEY=your-local-secret
LLM_MODEL=deepseek-v4-pro
SPRING_BOOT_BASE_URL=http://localhost:8081
```

`.env` 已由 `.gitignore` 排除，不应提交真实密钥。

## 启动

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

默认地址为 `http://127.0.0.1:8000`，交互式接口文档位于 `http://127.0.0.1:8000/docs`。

## 调用

```powershell
$body = @{ message = '你好' } | ConvertTo-Json
Invoke-RestMethod -Method Post `
  -Uri 'http://127.0.0.1:8000/chat' `
  -ContentType 'application/json' `
  -Body $body
```

成功响应：

```json
{
  "answer": "..."
}
```

## 当前范围

- Graph 为 `START -> Agent -> Tool -> Agent -> END` 循环。
- 每个请求无状态，不实现 Agent Memory。
- 当前只开放 `get_shop_info`，通过 HTTP 调用 Spring Boot 的 `/api/ai/shop/{id}`。
- 不包含 RAG、数据库、Redis、Kafka、Elasticsearch 或 Canal 客户端。

## 测试

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```
