# AI Assistant Phase 1 环境搭建

## 1. Python 环境

AI Assistant 是仓库中的独立 Python 服务，目录为 `ai-assistant/`，不加入 Spring Boot 进程。建议使用 Python 3.11 和项目内虚拟环境 `.venv`，避免依赖污染系统 Python 或 Java 项目。

当前开发机检测到 Python 3.11.7。

## 2. 创建虚拟环境与安装依赖

在仓库根目录执行：

```powershell
cd ai-assistant
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

主要依赖：

| 依赖 | 用途 |
|---|---|
| FastAPI | 暴露 `/chat` HTTP 接口 |
| Uvicorn | ASGI 开发服务器 |
| LangGraph | 构建 `START → LLM → END` 状态图 |
| LangChain | LLM 消息和基础抽象 |
| langchain-openai | 调用 OpenAI API 兼容模型服务 |
| python-dotenv | 从服务本地 `.env` 读取配置 |

`requirements.txt` 使用兼容版本范围，没有绑定未经验证的单一最高版本。

## 3. `.env` 配置

复制示例配置：

```powershell
Copy-Item .env.example .env
```

填写本机真实配置：

```dotenv
LLM_BASE_URL=https://your-openai-compatible-endpoint/v1
LLM_API_KEY=your-local-secret
LLM_MODEL=deepseek-v4-pro
SPRING_BOOT_BASE_URL=http://localhost:8081
```

变量说明：

| 变量 | 说明 |
|---|---|
| `LLM_BASE_URL` | DeepSeek 或其他 OpenAI 兼容服务的 API 根地址，通常包含 `/v1` |
| `LLM_API_KEY` | 本地密钥；不得提交到 Git、文档或日志 |
| `LLM_MODEL` | 服务端实际可用的模型名称 |
| `SPRING_BOOT_BASE_URL` | Phase 2 只读 Tool 使用的 Spring Boot 根地址 |

`.env.example` 只保存变量名和非敏感默认模型名；真实 `.env` 已加入 `ai-assistant/.gitignore`。

当前实现会在真正调用 LLM 时校验前三个变量，并在执行 Tool 时校验 Spring Boot 地址。缺少配置时服务仍可启动，但请求返回缺失变量名，不回显任何变量值。

## 4. 启动命令

```powershell
cd ai-assistant
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

等价的激活虚拟环境方式：

```powershell
cd ai-assistant
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --reload
```

默认监听：

- API：`http://127.0.0.1:8000`
- Swagger UI：`http://127.0.0.1:8000/docs`

## 5. 请求验证

```powershell
$body = @{ message = '你好' } | ConvertTo-Json
Invoke-RestMethod -Method Post `
  -Uri 'http://127.0.0.1:8000/chat' `
  -ContentType 'application/json' `
  -Body $body
```

预期成功响应：

```json
{
  "answer": "模型返回的文本"
}
```

空字符串、只有空白的消息或超过 4000 字符的消息会被拒绝。提供商异常时接口返回通用 502，不把上游错误细节暴露给调用方。

## 6. 环境边界

- Phase 1 基础对话不需要启动 Spring Boot；Phase 2 的 `get_shop_info` 真实联调需要启动 Spring Boot 及其店铺查询所需环境。
- 不读取 Java `application.yaml`。
- 不连接向量数据库，不创建索引，不实现 RAG。
- 不保存会话记忆。
