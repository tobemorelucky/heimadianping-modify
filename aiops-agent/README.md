# HMDP AIOps Agent

HMDP AIOps Agent 是独立于 Spring Boot 和现有经营分析助手的本地只读故障诊断项目。本目录实现了可配置 LLM Runtime 闭环：配置加载、Incident 创建、Skill Discovery、结构化 Planner、MCP Observation、Evidence 持久化、Reflection、报告生成和 Trace；同时提供本地只读持续采集、确定性异常检测、主动 Incident 诊断、Phase G1 Proposal 权限治理和 Phase S1 领域 Skill Runtime。

Planner、Reflection 和 Reporter 统一通过结构化 LLM Provider 调用。默认 Provider 是无网络、无密钥的 `MockLLMProvider`；也可切换到 OpenAI-compatible Chat Completions API。Runtime 使用官方 MCP Python SDK，通过 stdio 子进程访问独立 MCP Server。当前提供 `search_application_logs`、`get_kafka_status` 和 `get_business_metrics` 三个只读 Observation Tool，不连接 Redis 或 MySQL，也不包含任何自动修复能力。

## 环境要求

- Windows 10/11
- Python 3.11+
- PowerShell

## 安装

在仓库根目录执行：

```powershell
cd aiops-agent
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

`.env` 只用于本地配置且已被 Git 忽略。默认数据库位于 `aiops-agent/data/aiops.db`，目录和数据库会在应用启动时自动创建。

默认无需配置外部模型：

```dotenv
AI_MODEL_PROVIDER=mock
```

如需使用 OpenAI-compatible API，在本地 `.env` 中配置：

```dotenv
AI_MODEL_PROVIDER=openai_compatible
AI_MODEL_BASE_URL=https://your-provider.example/v1
AI_MODEL_API_KEY=replace-with-local-secret
AI_MODEL_NAME=your-model-name
AI_MODEL_TIMEOUT_SECONDS=30
```

`AI_MODEL_*` 是推荐配置名。为便于复用现有本地环境，Base URL、API Key 和模型名也分别兼容 `BASE_URL / ARK_API_KEY / MODEL` 以及 `DOUBAO_BASE_URL / DOUBAO_API_KEY / DOUBAO_MODEL`；显式的 `AI_MODEL_*` 优先。不要把真实密钥写入 `.env.example` 或提交到 Git。

MCP 配置：

```dotenv
HMDP_PROJECT_ROOT=..
AIOPS_MCP_TOOL_TIMEOUT_SECONDS=10
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_VOUCHER_ORDER_TOPIC=hmdp.seckill.order.create.v1
KAFKA_CONSUMER_GROUP=hmdp-seckill-order-create-v1
AIOPS_KAFKA_REQUEST_TIMEOUT_MS=3000
AIOPS_KAFKA_LAG_THRESHOLD=1000
```

`HMDP_PROJECT_ROOT` 指向 HMDP 仓库根目录。Runtime 会自动使用当前 Python 解释器启动 `mcp/server.py`，不需要单独常驻一个 MCP 服务。

Runtime 会将 MCP discovery 与 `tool_manifest.json` 做启动校验。Manifest 固定声明工具名、描述、输入 Schema、Server 身份以及 `category / permission_level / risk_level / approval_required`；名称、Schema 或 Server 不一致时，工具不会提供给 Planner。当前三个工具均为 `category=observation`、`READ_ONLY`、`risk_level=none` 且不需要审批。

## 启动

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8010
```

健康检查：

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8010/health
```

预期响应：

```json
{
  "service": "HMDP AIOps Agent",
  "status": "ok",
  "database": "ok",
  "environment": "local"
}
```

## 创建诊断任务

`POST /incidents` 会同步执行一次只读 MCP 诊断闭环，并返回 `incident_id`：

```powershell
$body = @{
    title = "秒杀订单延迟"
    description = "用户反馈订单创建缓慢"
    time_window = "10m"
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri http://127.0.0.1:8010/incidents `
    -ContentType application/json `
    -Body $body
```

默认 Mock Planner 会根据 Incident 内容选择 `search_application_logs` 或 `get_kafka_status`。日志工具只读取以下两个固定路径：

- `target/runtime/spring-boot.out.log`
- `target/runtime/spring-boot.error.log`

输入包含 `incident_id`、`log_paths`、可选 `query` 和最多 500 的 `max_lines`。任意非白名单路径都会被拒绝。日志不存在或结果被截断时，工具返回 `completeness=partial`，而不是伪造完整数据。

Kafka 工具接受可选 `topic` 和 `consumer_group`；未提供时使用上述环境变量。它只读取 Topic 元数据、Consumer Group 成员、已提交 offset 和 end offset，并计算 lag。它不会订阅或读取消息、加入消费者组、提交 offset，也不会创建或修改 Topic。无法连接 Broker 时返回结构化 `error` Evidence。

### Business Metrics Observation（Phase D1）

`get_business_metrics` 从两个既有 Spring Runtime 日志白名单中派生以下秒杀链路计数：

- `seckill_request_count`
- `lua_admission_success_count`
- `kafka_message_sent_count`
- `order_created_success_count`
- `order_created_failure_count`

输入只有 `incident_id` 和最多 10000 行的 `max_lines`，不接受任意路径、PromQL、SQL 或 Redis 命令。工具最多读取每个文件末尾 2 MiB，并识别受控业务事件文本或以下结构化日志标记：

```text
AIOPS_METRIC seckill_request_count=100
AIOPS_METRIC lua_admission_success_count=100
AIOPS_METRIC kafka_message_sent_count=100
AIOPS_METRIC order_created_success_count=20
AIOPS_METRIC order_created_failure_count=80
```

当前 Java 应用未被修改，因此只有日志中实际存在相应事件或标记时指标才可用。文件缺失、读取被截断或某类标记不存在时返回 `partial` 并列出 `unavailable_metrics`，不会把缺失值伪装成零。读取异常返回结构化 `error` Evidence。

联合诊断流程：

```text
Incident: 秒杀业务链路订单创建下降
  -> get_business_metrics
     requests≈Lua accepted≈Kafka sent, orders created下降
  -> Reflection: 将异常范围收敛到 Kafka 发送之后
  -> get_kafka_status
     检查 Consumer 成员、offset 和 lag
  -> search_application_logs
     查找 Consumer 停止或订单创建失败的独立证据
  -> Evidence-backed Diagnosis Report
```

Business Metrics 只能定位链路阶段，不能单独证明 Consumer 或 MySQL 是根因。当前没有 MySQL Tool，因此报告必须保留该证据缺口。

## Evidence-grounded Hypothesis Ledger（Phase D2）

每次 Incident 诊断都会创建一个仅存在于当前 Runtime run 的 `HypothesisLedger`。它不是长期 Memory，也不跨 Incident 检索。Planner 创建调查假设后，Ledger 显式维护：

- `UNKNOWN`：尚无足够 Evidence。
- `SUPPORTED`：存在支持当前假设的 Evidence。
- `CONTRADICTED`：当前 Evidence 反驳假设。
- `REJECTED`：假设被调查流程明确排除。

每个 Hypothesis 保存置信度、支持 Evidence ID、反驳 Evidence ID以及创建和更新时间。Evidence ID 由 Runtime 根据实际 Tool Observation 本地绑定，模型不能自行生成或修改引用。

```text
Planner hypothesis
  -> hypothesis_created: UNKNOWN
  -> MCP Observation
  -> ContextPacket.current_hypotheses + EvidenceCard
  -> Reflection
  -> Runtime binds current evidence_id
  -> hypothesis_updated: SUPPORTED / CONTRADICTED / REJECTED
  -> next Planner receives ranked hypotheses
  -> Diagnosis Report.final_hypothesis
```

例如，业务指标显示请求、Lua 和 Kafka 发送正常而订单创建下降时，只能支持“异常位于下游”的假设；随后 Kafka `member_count=0` 和 Consumer stopped 日志可以共同支持 Kafka Consumer 假设。若业务指标全链路一致，则对应的链路下降假设会记录为 `CONTRADICTED`，而不是被静默丢弃。

Ledger 变化通过 `hypothesis_created` 和 `hypothesis_updated` Trace 审计。Trace 只记录状态、置信度与 Evidence 引用，不记录模型隐式思维链。最终 Hypothesis 同时写入 Diagnosis Report 和本地 Incident SQLite 报告记录。

## Continuous Monitoring（Phase M1 + M2 + M3）

Monitoring 使用独立进程运行持久化 Scheduler。Phase M1 负责只读采集，Phase M2 在 Observation 入库后执行确定性规则，Phase M3 将新 AnomalySignal 转换为 Incident 并调用现有 Agent Runtime。当前只注册一个 `get_kafka_status` 采集计划和 `kafka-consumer-down-v1` 规则，不包含 Redis、MySQL、Elasticsearch 或 Action Tool。

单次采集并退出：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m monitoring.scheduler --once
```

持续运行（默认每秒检查到期计划，Kafka 默认每 30 秒采集一次）：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m monitoring.scheduler
```

按 `Ctrl+C` 安全停止。也可调整 Scheduler 检查频率；这不会改变持久化采集计划的 30 秒间隔：

```powershell
.\.venv\Scripts\python.exe -m monitoring.scheduler --poll-seconds 2
```

采集链路：

```text
Persisted CollectionSchedule
  -> Scheduler: monitoring_tick_started
  -> Tool Registry: Manifest + MCP discovery + READ_ONLY validation
  -> existing stdio MCP Client
  -> get_kafka_status
  -> success / partial / error / timeout ToolObservation
  -> SQLite collection run + evidence_ref
  -> observation_collected trace
  -> Deterministic Anomaly Detector
  -> consecutive-window and cooldown evaluation
  -> optional AnomalySignal + anomaly_detected trace
  -> Incident Manager: fingerprint and cooldown deduplication
  -> Incident OPEN -> DIAGNOSING
  -> existing Planner / MCP / Context / Reflection / Reporter
  -> Incident RESOLVED or FAILED
  -> optional Action Proposal
  -> Permission Gateway: ALLOW / REQUIRE_APPROVAL / DENY
  -> stop; no executor
  -> calculate next_run_at
```

Kafka 首条规则：

```text
rule_id: kafka-consumer-down-v1
condition: member_count == 0 AND lag > AIOPS_KAFKA_LAG_THRESHOLD
consecutive windows: 2
lookback: 2 minutes
cooldown: 10 minutes
severity: high
fingerprint: topic + consumer_group
```

只有 `success` Observation 可以满足规则。`partial/error/timeout` 会打断连续窗口，不会被当作异常事实。cooldown 内持续异常不会重复创建 Signal。`lag` 在规则层兼容真实工具的 `total_lag` 字段，但 Signal facts 统一记录为 `lag`。

SQLite 中包含七个 Monitoring/Detection/Incident Manager 表：

- `monitoring_schedules`：保存 `schedule_id`、工具名、间隔、启停状态、超时和下一次执行时间。
- `monitoring_collection_runs`：保存每次调用的 tool、status、timestamp、evidence_ref 和完整 Observation JSON。
- `monitoring_trace_events`：保存 `monitoring_tick_started`、`observation_collected` 与 `anomaly_detected`。
- `detection_rules`：保存版本化规则、条件、连续窗口、lookback 和 cooldown。
- `anomaly_signals`：保存 fingerprint、facts、Observation 引用和 first/last seen。
- `managed_incidents`：保存主动 Incident 状态、Signal 引用、fingerprint 和诊断报告引用。
- `incident_manager_trace_events`：保存 `incident_created`、`diagnosis_started` 和 `diagnosis_completed`。

规则命中后，Incident Manager 使用 Signal fingerprint 去重。同一个 Kafka Topic 和 Consumer Group 在 10 分钟 cooldown 内的新 Signal 会合并到已有 Incident 的 `trigger_signal_ids`，不会再次启动诊断。新 Incident 使用同一个 `incident_id` 调用现有 Runtime，因此 Planner、MCP、Context、Reflection 和 Reporter 不会在 Monitoring 中重复实现。

主动 Incident 使用 `OPEN -> DIAGNOSING -> RESOLVED/FAILED` 生命周期。这里的 `RESOLVED` 仅表示诊断流程完成并已生成报告，不表示业务故障已修复。Runtime 自身仍会将诊断任务推进到 `awaiting_human`，等待人工查看。

工具异常和超时会保存为结构化 Observation，Scheduler 会继续按下一周期运行。若本机 Kafka 未启动，`--once` 正常完成并记录 `error`，不会误报消费者异常，也不会尝试启动、重启或修改 Kafka。

## Action Proposal 与 Permission Governance（Phase G1）

Diagnosis Report 确认 Kafka consumer down 且引用有效 Evidence 时，确定性 Proposal Generator 可以生成：

```json
{
  "action_name": "restart_consumer",
  "risk_level": "low",
  "approval_required": true,
  "evidence_refs": ["evi_kafka_status", "evi_consumer_log"]
}
```

Proposal 同时包含 reason、rollback plan 和只读 verification plan。它只是建议对象；`restart_consumer` 没有注册为 MCP Tool，也没有实现服务重启函数或 Executor。

Permission Gateway 的固定策略：

| Permission | 结果 |
| --- | --- |
| `READ_ONLY` | `ALLOW`，可自动执行现有 Observation Tool |
| `LOW_RISK_ACTION` | `REQUIRE_APPROVAL`，Phase G1 到此停止 |
| `HIGH_RISK_ACTION` | `DENY`，不可通过审批放行 |

当前 action allowlist 只包含 proposal-only 的 `restart_consumer`。未知 Action、风险字段与策略不匹配的 Proposal 以及所有高风险 Action 均 fail closed。诊断完成后记录 `action_proposal_created` 和 `permission_checked`，但不会产生任何外部状态变更。

## FaultBench Demo

首个可重复场景为 `KAFKA_CONSUMER_DOWN_001`。它不会停止或修改真实 Kafka，而是通过固定 Fixture 返回以下观测：

- Consumer Group 活跃成员数为 0，lag 为 50000。
- 应用日志包含 `consumer stopped unexpectedly`。

运行 Demo：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.runner `
    --fault-id KAFKA_CONSUMER_DOWN_001
```

执行链路：

```text
用户输入：秒杀订单延迟，请分析原因
  -> Planner
  -> get_kafka_status Fixture MCP Observation
  -> Reflection：Kafka Evidence supports，继续交叉验证
  -> search_application_logs Fixture MCP Observation
  -> Reflection：两类 Evidence 相互印证
  -> Diagnosis Report
  -> FaultEvaluator
```

报告写入 `evaluation/reports/KAFKA_CONSUMER_DOWN_001.json`，包括最终诊断、工具调用顺序、Evidence ID、Trace 摘要以及 Root Cause Accuracy、Evidence Coverage、Tool Efficiency、Trace Completeness。相同 Case 和 Fixture 多次执行会产生一致的评测报告。

当前基准结果：

| 指标 | 结果 |
| --- | ---: |
| Root Cause Accuracy | 1.0 |
| Evidence Coverage | 1.0 |
| Tool Efficiency | 1.0 |
| Trace Completeness | 1.0 |

该 Demo 只验证诊断轨迹，不包含故障注入、自动修复或基础设施状态修改。

统一 Evidence Envelope：

```json
{
  "evidence_id": "evi_mcp_...",
  "status": "success",
  "kind": "log",
  "source": "hmdp.spring_boot.runtime_logs",
  "source_tool": "search_application_logs",
  "collected_at": "2026-09-19T00:00:00Z",
  "observation_window": null,
  "summary": "Application log search returned 0 event(s).",
  "completeness": "complete",
  "raw_ref": "logs://hmdp/target/runtime/spring-boot",
  "data": {
    "events": [],
    "files": [],
    "truncated": false,
    "warnings": []
  },
  "error": null
}
```

所有工具调用统一返回 `success / partial / error / timeout` Observation。可恢复的工具错误和超时会保存为 Evidence 并交给 Reflection；只有 MCP 启动、discovery 或 Manifest 校验等控制面失败才直接终止 Incident。

LLM 调用链如下：

```text
Incident + selected Skill + bounded ContextPacket + Registry-authorized Tool Manifest
  -> Planner Provider -> one next Action -> registry validation
  -> Stdio MCP Client -> MCP Server -> business metrics, Kafka status, or log search
  -> success/partial/error/timeout Observation -> generic Evidence -> SQLite
  -> Context Manager -> ranked/deduplicated EvidenceCards
  -> Reflection Provider + bounded ContextPacket + current hypotheses -> strict decision JSON
  -> Hypothesis Ledger -> evidence-grounded status/confidence update
  -> Reporter Provider + Evidence + Trace -> strict report JSON -> Pydantic validation
  -> SQLite + awaiting_human
```

模型返回非 JSON、字段不满足 Schema 或选择未授权工具时，Runtime 会拒绝结果。Planner 每轮只生成一个下一步 Action；Reflection 决定报告、结束或生成下一轮 Action。Planner 和 Reflection 只接收有界的 `ContextPacket`，不会直接读取 Evidence `data`。Context Manager 对日志和 Kafka Evidence 执行时间排序、来源可靠性评分、fingerprint 去重、Evidence 数量限制和样本长度限制，并用 `context_built` Trace 记录选择、排除原因和预算。工具错误和超时追加 `tool_failed` Trace，但仍作为结构化 Evidence 进入 Reflection；原始工具数据不会写入 Trace payload。

## Agent Skill Runtime（Phase S1）

Runtime 在首次规划前根据 Incident 标题、描述和受影响组件匹配本地领域 Skill：

```text
Incident
  -> Skill Registry: metadata-only discovery and trigger matching
  -> Skill Loader: load selected SKILL.md and references on demand
  -> Planner ContextPacket.selected_skill
  -> Planner
  -> registered read-only MCP Tool
  -> Evidence
  -> Reflection without Skill content
  -> Diagnosis Report
```

当前内置两个 Skill：

- `incident-triage`：通用故障分诊，指导确认影响、采集基础证据、建立和验证假设。
- `kafka-consumer-diagnosis`：面向 HMDP 秒杀订单链路，建议依次检查 Consumer Group、lag 和应用日志。

Registry 启动发现阶段只读取 `SKILL.md` TOML front matter 中的 `name / description / trigger_conditions` 等元数据。只有 Incident 命中后，Loader 才读取完整 Markdown 正文和 `references/`。选中的 Skill 通过 `ContextPacket.selected_skill` 进入 Planner，并记录 `skill_selected`、`skill_loaded` Trace；重新规划时复用同一个 Skill。

Skill 是程序化调查指导，不是 Evidence。它不能调用工具、不能绕过 Tool Registry，也不会进入 Reflection Context。Kafka Skill 即使建议优先检查 Kafka，当实际 Evidence 显示 Consumer 稳定且 lag 正常时，Reflection 仍会反驳 Kafka 假设并继续调查或输出不确定结论。

## 测试

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m pytest
```

测试使用临时 SQLite 数据库，不会写入正式的 `data/aiops.db`。

## Consumer MySQL 只读健康观测（Phase P4.1）

`get_mysql_health` 通过 MCP stdio 调用 Consumer JVM 的固定本机健康出口；不接受 SQL 或任意 URL。该出口只在 `consumer` Profile 中显式启用时启动，绑定 `127.0.0.1:18082`，不开放业务 Controller。启动 Consumer 时追加：

```powershell
--hmdp.aiops.mysql-health.enabled=true --hmdp.aiops.mysql-health.port=18082
```

Agent 端设置 `AIOPS_MYSQL_HEALTH_PORT=18082`，与 Consumer 端一致。健康出口通过 Consumer 自身的 DataSource 借连接并调用 `isValid()`；Hikari active/idle 来自 JVM 内的池快照。当前没有可信的累计连接超时数和业务错误数，因此返回 `null`，Tool Observation 为 `partial`，不把缺失当作零。出口未启用、角色错误或无法连接时返回结构化 `error/timeout`。本阶段不添加 MySQL 巡检计划、Detection、Incident 或自动恢复；启用前应确认该端口未被其他本机进程占用。

## Phase 1 目录

```text
aiops-agent/
├── tool_contracts.py # 通用 Tool Observation / Evidence 契约
├── tool_manifest.json # 静态只读工具授权清单
├── api/          # FastAPI 入口和 HTTP Schema
├── context/      # EvidenceCard、排序、压缩和 ContextPacket
├── mcp_tools/    # 可测试的只读 Kafka、业务指标与 Consumer MySQL 健康采集
├── runtime/      # Orchestrator、stdio MCP Client、Planner、Reflection 和报告
├── llm/          # Mock 与 OpenAI-compatible 结构化 Provider
├── mcp/          # 独立 MCP Server 入口
├── memory/       # SQLite Schema 和持久化
├── monitoring/   # 持久化 Scheduler、只读 Collector 和 Observation Store
│   └── detector/ # DetectionRule、连续窗口 Detector 和 Signal Store
├── incident/     # Signal 去重、主动 Incident、Runtime 触发和独立 Trace
├── governance/   # Action Proposal、静态策略和 Permission Gateway
├── reasoning/    # Incident 内 Hypothesis 模型和 Evidence-grounded Ledger
├── skills/       # metadata-only Registry、按需 Loader 和领域排障 Skill
│   ├── incident-triage/
│   └── kafka-consumer-diagnosis/
├── trace/        # Trace 事件模型
├── evaluation/   # 确定性 FaultBench
│   ├── faults/   # FaultCase 定义
│   ├── fixtures/ # 模拟 Kafka 与日志 Observation
│   ├── reports/  # 可重复生成的评测报告
│   ├── runner.py
│   ├── evaluator.py
│   └── schemas.py
└── tests/        # 启动、SQLite 和 Schema 测试
```

## AIOps Console（Phase UI1）

仓库根目录新增独立 `aiops-console/`（Vue 3 + Vite）。它只使用以下只读接口，不访问 SQLite：

- `GET /api/incidents`：Runtime 与主动 Incident Manager 的合并列表。
- `GET /api/incidents/{id}`：Incident、按生命周期和各 Trace 流序列合并的轨迹、Evidence 摘要、Hypothesis 置信度历史、Report 和已记录的 Proposal / Permission 结果。
- `GET /api/monitoring/summary`：最近一次巡检、健康摘要与 Active Incident 数量。

API 只返回有界 Evidence 摘要与白名单事实，不暴露原始日志或工具 Observation 数据。Console 的 Proposal 页面没有审批或执行功能。现有 `/incidents` 诊断入口、Planner、Reflection 和 MCP 工具均保持不变。

Windows 演示启动：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.console_demo --database ./data/console-demo.db
$env:AIOPS_DATABASE_PATH = './data/console-demo.db'
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8010
```

另开 PowerShell：

```powershell
cd aiops-console
npm install
npm run dev
```

访问 `http://127.0.0.1:5173`，进入 Kafka Incident，可查看 `故障 Fixture → Incident → Agent 诊断 → Report → Proposal（仅展示）`。不运行演示种子时，空库 Dashboard 显示 Healthy。更多说明见 `aiops-console/README.md`。

## 产品化双场景 Replay（Phase P5）

本项目不是聊天 Agent。产品演示主线是：

```text
Monitoring → Detection → Incident → Diagnosis → Governance
```

已完成的 Kafka Consumer Down 与 MySQL Persistence Failure 真实事故被导出为经过 Pydantic 校验的只读 Replay 投影。导入只复制历史快照，不创建 RuntimeOrchestrator，不调用 LLM/MCP，也不重新生成诊断：

```powershell
cd aiops-agent
.\.venv\Scripts\python.exe -m evaluation.replay_catalog
$env:AIOPS_DATABASE_PATH = './data/product-demo.db'
$env:AIOPS_REPLAY_DIRECTORY = './data/console-replays'
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8010
```

Console API 将实时 SQLite Incident 与 Replay Catalog 合并。Replay Incident 带 `replay_only=true` 和 `recording_type=recorded_observation`；历史 ACTIVE 状态不会计入当前健康摘要。Kafka Replay 展示 Skill、Kafka Tool、Evidence 与支持假设；MySQL Replay 展示 Business Metrics、Kafka 反证、MySQL timeout Evidence 与最终支持假设。

生成统一的双场景 FaultBench 报告：

```powershell
.\.venv\Scripts\python.exe -m evaluation.benchmark_report
```

输出 `evaluation/reports/FAULTBENCH_PRODUCT_DEMO.json`，包含 Accuracy、Evidence Coverage、Tool Efficiency、Trace Completeness 和 False Positive。所有 Proposal 都是展示与权限审计数据，没有执行器。

## 安全边界

- 服务默认只监听 `127.0.0.1`。
- MCP Server 只读取白名单 Spring Boot 日志、Kafka 元数据/offset，以及显式启用的 Consumer 本机 MySQL 健康出口；业务指标仍从日志白名单派生。它不读取 Kafka 消息，不执行 SQL，也不修改 HMDP 数据。
- Monitoring Collector 在 MCP discovery 和调用前强制校验 Tool Manifest 的 `READ_ONLY` 权限；非只读或未知工具会记录结构化错误且不会被调用。
- 不连接 Redis 或 Docker API；MySQL 仅由 Consumer JVM 内只读连接校验，不由 Agent 执行任意 SQL。不使用 Prometheus，业务指标仅来自有界只读日志派生。
- Phase M3 会从确定性 Signal 自动创建只读诊断 Incident；外部状态仍保持只读。
- Phase G1 可以生成低风险 Action Proposal 并执行权限检查，但没有审批接口或执行器。
- Phase S1 Skill 只向 Planner 提供领域调查指导，不执行工具、不作为 Evidence，也不进入 Reflection 判断。
- Phase D2 Ledger 仅维护当前 Incident 的短期假设状态，不提供长期 Memory、向量检索或跨事故学习。
- Incident Manager 只触发既有诊断 Runtime，不包含任何自动修复或状态变更工具。
- HIGH_RISK_ACTION 始终拒绝；没有 Kubernetes、服务重启、数据删除或任意命令执行路径。
- 外部 LLM 只在显式设置 `AI_MODEL_PROVIDER=openai_compatible` 时调用。
- P4.1 仅新增 Consumer Profile 的可选只读健康出口；不修改订单业务逻辑、事务边界、Docker 或 Nginx。
- 本阶段没有执行修复或向 HMDP 数据库写入的代码路径。
