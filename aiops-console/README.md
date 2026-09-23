# AIOps Console Demo

这是独立于黑马点评原 Vue 前端的 Vue 3 / Vite 单页控制台。它不是聊天 Agent，也不是 Grafana 替代品；它通过只读 FastAPI 展示 `Monitoring → Detection → Diagnosis → Governance` 的 Agent Harness 轨迹，不直连 SQLite，不提供审批或 Action 执行入口。

## 环境要求

- Windows 10/11、PowerShell
- Python 3.11+（`aiops-agent`）
- Node.js 建议 22.22.2+ 或 24.15+，npm 10+。已在 Node 24.14.0 / npm 11.9.0 验证安装、构建和测试；此版本可能出现来自测试依赖的非阻断 engine warning。

## 1. 启动 aiops-agent

在仓库根目录的 PowerShell 中运行。首次运行按 `aiops-agent/README.md` 创建 Python 虚拟环境并安装依赖。

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.replay_catalog
$env:AIOPS_DATABASE_PATH = './data/product-demo.db'
$env:AIOPS_REPLAY_DIRECTORY = './data/console-replays'
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8010
```

访问 `http://127.0.0.1:8010/health` 应得到 `status: ok`。`replay_catalog` 会校验并导入两个真实事故的只读展示投影：Kafka Consumer Down 与 MySQL Persistence Failure。重复导入是幂等覆盖；该命令不会实例化 Agent Runtime，也不会调用 LLM、MCP 或 Action。

## 2. 启动 aiops-console

另开 PowerShell：

```powershell
cd aiops-console
Copy-Item .env.example .env
npm install
npm run dev
```

打开 `http://127.0.0.1:5173`。默认 `VITE_AIOPS_API_BASE_URL=/api`，Vite 将 `/api` 代理到本机 `127.0.0.1:8010`。无后端时页面显示 `Agent Offline` 和重试入口，不会显示旧的 Healthy 状态。

## 演示路线

Dashboard 同时列出两个 `RECORDED` Incident。Recorded Incident 保留故障发生时的状态，但不会计入当前 Active Incident 数量，因此不会把历史事故误报为当前系统不健康。

### Kafka Consumer Failure

1. Dashboard：查看最近巡检和 Kafka Incident，点击进入。
2. Incident Detail：沿 Timeline 查看 `anomaly_detected → incident_created → skill_selected / skill_loaded → tool_called → evidence_created → hypothesis_updated → diagnosis_completed`。
3. 右侧查看 Kafka Skill、`get_kafka_status → search_application_logs` 调用顺序和 Diagnosis Report。
4. 下方查看 Kafka / Log Evidence 与 Hypothesis 从 `UNKNOWN` 到 `SUPPORTED` 的置信度变化。
5. 进入 Proposal：查看 `restart_consumer` 建议与 `REQUIRE_APPROVAL`；页面不含执行按钮。

Incident Detail 顶部的 `Replay` 按钮可从第 0 步逐步或自动回放已保存的 Trace。Skill、Tool Calls、Evidence、Hypothesis、Reflection 和 Report 仅在相应事件出现后展示；回放不会重新调用 Agent、LLM 或 MCP，也不会改写 Incident。

### MySQL Persistence Failure

1. Business Metrics 显示请求、Lua 和 Kafka 发送增长，但订单创建没有增长。
2. Kafka Evidence 显示 Group 稳定、`member_count=1`、`lag=0`，将 Kafka Hypothesis 更新为 `CONTRADICTED`。
3. MySQL Evidence 显示 `health_state=DEGRADED` 与 `failure_class=CONNECTION_TIMEOUT`。
4. MySQL Persistence Hypothesis 更新为 `SUPPORTED`，Report 置信度为 93%。
5. Proposal 仅展示并经过 Permission Check，不提供执行按钮。

完整路径：`Recorded Observation → AnomalySignal → Incident → Skill → MCP Tool Trace → Evidence → Hypothesis → Report → Proposal → Permission Check`。

首次使用空数据库、不运行 `console_demo` 时，Dashboard 显示 `Healthy` 和“尚无巡检记录”。

## 验证与构建

```powershell
cd aiops-console
npm test
npm run build
```

构建输出在 `dist/`。Console 的 Evidence API 只返回摘要与白名单结构化事实，不传送原始日志。`tool_started` / `evidence_added` 显示为 `tool_called` / `evidence_created`，仍保留原始事件名。Proposal 由历史审计 Trace 与 Report 结论构成只读展示，未产生的 Proposal 不会被推测或合成。

真实故障演示的前提、注入边界与降级验收见 `docs/aiops/09_real_fault_demo.md`；Trace 回放契约见 `docs/aiops/10_agent_trace_replay.md`。`evaluation/replays/` 是从已完成真实实验中导出的展示安全投影；原始 Observation 仍保留独立来源引用，回放不冒充一次新的实时诊断。

统一 FaultBench 产品报告：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.benchmark_report
```

报告写入 `evaluation/reports/FAULTBENCH_PRODUCT_DEMO.json`，统一输出 Accuracy、Evidence Coverage、Tool Efficiency、Trace Completeness 与 False Positive。
