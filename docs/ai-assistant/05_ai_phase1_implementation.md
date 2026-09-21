# AI Assistant Phase 1 实现记录

## 1. 阶段目标

在不修改 Spring Boot 核心业务代码的前提下，新建独立 Python AI 服务，跑通以下最小链路：

```text
POST /chat
  ↓
FastAPI
  ↓
LangGraph StateGraph
  ↓
OpenAI 兼容 LLM
  ↓
{ "answer": "..." }
```

## 2. 服务结构

```text
ai-assistant/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── graph.py
│   │   └── state.py
│   └── tools/
│       └── __init__.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

| 文件 | 作用 |
|---|---|
| `app/main.py` | FastAPI 应用、请求/响应模型与 `/chat` 接口 |
| `app/config.py` | 加载 `.env`，读取并校验 LLM 三项环境变量 |
| `app/agent/state.py` | 定义含 `message`、`answer` 的 `AgentState` |
| `app/agent/graph.py` | 构建单节点 StateGraph 并调用兼容 OpenAI API 的 LLM |
| `app/tools/__init__.py` | 为后续只读业务 Tool 预留包，不含任何实现 |
| `.env.example` | 不含真实密钥的配置模板 |
| `.gitignore` | 排除 `.env`、虚拟环境和 Python 缓存 |
| `requirements.txt` | Python 运行依赖及兼容版本范围 |
| `README.md` | 独立服务安装、配置、启动和调用说明 |

## 3. FastAPI 接口

### 3.1 请求

```http
POST /chat
Content-Type: application/json
```

```json
{
  "message": "你好"
}
```

`message` 长度为 1～4000；只有空白字符的输入也会被拒绝。

### 3.2 成功响应

```json
{
  "answer": "..."
}
```

缺少 LLM 配置时返回 503；上游 LLM 调用失败时记录服务端日志并返回不含敏感细节的 502。

## 4. LangGraph 流程

```mermaid
flowchart LR
    S([START]) --> L[LLM Node]
    L --> E([END])
```

`AgentState`：

```text
message: 用户消息
answer: 模型回答
```

LLM Node 从状态读取 `message`，包装为 `HumanMessage` 后调用 `ChatOpenAI`，再把响应文本写入 `answer`。Graph 在模块加载时通过 `StateGraph(AgentState)` 编译，不包含 Tool、条件分支、检查点或 Memory。

## 5. LLM 调用链

1. `config.py` 从进程环境或 `ai-assistant/.env` 读取配置。
2. 首次请求 LLM 时校验 `LLM_BASE_URL`、`LLM_API_KEY` 和 `LLM_MODEL`。
3. `ChatOpenAI` 使用自定义 `base_url` 调用 OpenAI API 兼容服务。
4. 模型响应写回 LangGraph State。
5. FastAPI 只从最终 State 取出 `answer`，按固定 JSON 协议返回。

LLM Client 和 Settings 在进程内缓存，避免每次请求重复创建客户端。服务不记录 API Key。

## 6. 实现边界

- 没有修改 Java 源码、Java 配置、SQL 或 Lua。
- 没有 Spring AI 依赖或 Java/Python业务接口。
- 没有数据库、Redis、Kafka、Elasticsearch、Canal 或向量库连接。
- 没有 RAG、业务 Tool、Agent Memory、多 Agent 或持久化会话。
- `tools` 目录只是下一阶段的空包。

## 7. 验证项目

Phase 1 验证包括：

- 在独立 `.venv` 安装全部 Python 依赖。
- Python 源码编译检查。
- 导入 FastAPI 应用并核对 StateGraph 编译结果。
- 启动 Uvicorn 并向 `/chat` 发送请求。
- 有有效 DeepSeek 配置时确认真实回答；没有凭据时确认 503 配置保护行为，并将真实联调标记为待配置。

### 7.1 本次实测结果

验证环境：Python 3.11.7，项目内虚拟环境 `ai-assistant/.venv`。

| 验证项 | 结果 |
|---|---|
| `pip install -r requirements.txt` | 通过，FastAPI、Uvicorn、LangGraph、LangChain、langchain-openai、python-dotenv 安装成功 |
| `pip check` | 通过，`No broken requirements found` |
| 7 个 Python 文件 AST 语法检查 | 通过 |
| FastAPI 应用导入 | 通过，应用标题为 `HM-DianPing Plus AI Assistant` |
| LangGraph 编译 | 通过，节点为 `__start__`、`llm`、`__end__` |
| `uvicorn app.main:app --reload` | 通过，监听 `127.0.0.1:8000`，应用启动完成 |
| `GET /openapi.json` | 200，确认 `/chat` 已注册 |
| 无 LLM 配置调用 `/chat` | 503，准确列出三个缺失变量名且不回显值 |
| 空白消息调用 `/chat` | 422 |
| 隔离假 LLM 完整接口链路 | 200，响应 `{"answer":"mock-deepseek-answer"}` |

当前进程未设置 `LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL`，仓库中也没有本地 `ai-assistant/.env`，因此无法完成真实 DeepSeek 网络请求。本次没有生成或猜测密钥，也没有把假 LLM 结果冒充真实提供商结果。配置有效凭据后，按环境搭建文档重启服务并重复同一 POST 请求即可完成最后一步联调。

开发服务器在验证结束后已停止，8000 端口不再监听。
