# AIOps Console Demo

这是独立于黑马点评原 Vue 前端的 Vue 3 / Vite 单页控制台。它通过只读 FastAPI 展示 Agent Harness 的巡检、Incident、Skill、Tool、Evidence、Hypothesis、Report 与治理轨迹，不直连 SQLite，不提供审批或 Action 执行入口。

## 环境要求

- Windows 10/11、PowerShell
- Python 3.11+（`aiops-agent`）
- Node.js 建议 22.22.2+ 或 24.15+，npm 10+。已在 Node 24.14.0 / npm 11.9.0 验证安装、构建和测试；此版本可能出现来自测试依赖的非阻断 engine warning。

## 1. 启动 aiops-agent

在仓库根目录的 PowerShell 中运行。首次运行按 `aiops-agent/README.md` 创建 Python 虚拟环境并安装依赖。

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.console_demo --database ./data/console-demo.db
$env:AIOPS_DATABASE_PATH = './data/console-demo.db'
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8010
```

访问 `http://127.0.0.1:8010/health` 应得到 `status: ok`。`console_demo` 使用固定 FaultBench Observation，不连接真实 Kafka、Redis 或 MySQL。重复运行会复用已生成的 Demo Incident，不产生重复 Timeline。

## 2. 启动 aiops-console

另开 PowerShell：

```powershell
cd aiops-console
Copy-Item .env.example .env
npm install
npm run dev
```

打开 `http://127.0.0.1:5173`。默认 `VITE_AIOPS_API_BASE_URL=/api`，Vite 将 `/api` 代理到本机 `127.0.0.1:8010`。无后端时页面显示 `Agent Offline` 和重试入口，不会显示旧的 Healthy 状态。

## 演示路线：Kafka Consumer Failure

1. Dashboard：查看最近巡检和 Kafka Incident，点击进入。
2. Incident Detail：沿 Timeline 查看 `anomaly_detected → incident_created → skill_selected / skill_loaded → tool_called → evidence_created → hypothesis_updated → diagnosis_completed`。
3. 右侧查看 Kafka Skill、`get_kafka_status → search_application_logs` 调用顺序和 Diagnosis Report。
4. 下方查看 Kafka / Log Evidence 与 Hypothesis 从 `UNKNOWN` 到 `SUPPORTED` 的置信度变化。
5. 进入 Proposal：查看 `restart_consumer` 建议与 `REQUIRE_APPROVAL`；页面不含执行按钮。

Incident Detail 顶部的 `Replay` 按钮可从第 0 步逐步或自动回放已保存的 Trace。Skill、Tool Calls、Evidence、Hypothesis、Reflection 和 Report 仅在相应事件出现后展示；回放不会重新调用 Agent、LLM 或 MCP，也不会改写 Incident。

完整路径：`FaultBench Fixture → Monitoring Observation → AnomalySignal → Incident → Skill → MCP Tool → Evidence → Hypothesis → Report → Proposal → Permission Check`。

首次使用空数据库、不运行 `console_demo` 时，Dashboard 显示 `Healthy` 和“尚无巡检记录”。

## 验证与构建

```powershell
cd aiops-console
npm test
npm run build
```

构建输出在 `dist/`。Console 的 Evidence API 只返回摘要与白名单结构化事实，不传送原始日志。`tool_started` / `evidence_added` 显示为 `tool_called` / `evidence_created`，仍保留原始事件名。Proposal 由历史审计 Trace 与 Report 结论构成只读展示，未产生的 Proposal 不会被推测或合成。

真实故障演示的前提、注入边界与降级验收见 `docs/aiops/09_real_fault_demo.md`；Trace 回放契约见 `docs/aiops/10_agent_trace_replay.md`。当前 `console_demo` 仍是确定性 Fixture，不能冒充真实基础设施故障。
