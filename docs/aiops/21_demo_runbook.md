# AIOps 一键演示 Runbook

## 1. 演示定位与安全边界

该 Demo 展示的不是聊天机器人，而是：

```text
Monitoring → Detection → Incident → Agent Diagnosis → Governance → Console Replay
```

一键脚本只编排现有隔离基础设施、双 JVM、只读 AIOps Agent 和 Console。它不修改订单业务逻辑、Agent 推理逻辑或数据库结构，不执行自动修复，也不删除 Docker volume、SQLite、日志、Topic 或业务数据。

所有命令均在仓库根目录的 PowerShell 中执行。仅在本机隔离演示环境使用测试账号与测试券，禁止指向生产或共享环境。

## 2. 前置条件

- Windows PowerShell 7 或 Windows PowerShell 5.1。
- Docker Desktop 已启动。
- Java 17、Maven、Python 3.11+、Node.js 22+ 与 npm 可用。
- 首次运行允许 Maven、pip 或 npm 安装依赖；已有 JAR、`.venv` 和 `node_modules` 时不会重复安装。
- 默认端口可用：`8081`、`18081`、`18082`、`18083`、`8010`、`5173`。

若源码有更新，首次启动时使用 `-Rebuild`；否则脚本复用 `target/hm-dianping-0.0.1-SNAPSHOT.jar`。

## 3. 一键启动

```powershell
& .\scripts\aiops-demo\runtime\start-aiops-demo.ps1
```

该命令依次执行：

1. 启动 `docker-compose.aiops-demo.yml`，等待 Demo MySQL、Redis、Kafka 和 Consumer MySQL Proxy healthy。
2. 创建隔离 Kafka main/retry/DLT Topic。
3. 启动 `hmdp-consumer`，等待 MySQL Health 与 Consumer Business Metrics 出口。
4. 启动 `hmdp-web`，等待 HTTP 与 Web Business Metrics 出口。
5. 导入两个真实事故的只读 Replay 投影。
6. 启动 FastAPI、Monitoring Scheduler 和 Vue/Vite Console。
7. 保存所有 PID、启动参数和日志位置，打开 `http://localhost:5173`。

不希望自动打开浏览器时：

```powershell
& .\scripts\aiops-demo\runtime\start-aiops-demo.ps1 -NoOpen
```

每次全新启动会创建独立运行目录和 SQLite，不覆盖上一次演示：

```text
target/runtime/aiops-demo-runtime/
├── state.json
└── runs/<yyyyMMdd-HHmmss>/
    ├── aiops-demo.db
    ├── consumer.out.log
    ├── web.out.log
    ├── agent-api.out.log
    ├── monitoring.out.log
    └── console.out.log
```

`state.json` 是故障和恢复脚本唯一认可的进程清单。脚本不会按进程名批量结束用户的其他 Java、Python 或 Node 进程。

## 4. 准备真实测试订单

启动后先等待至少一次 Dashboard 巡检，再通过真实 HTTP 业务入口创建测试券和正常基线订单：

```powershell
$baseline = & .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase demo_baseline `
  -Phones @('19920925001') `
  -Stock 10 `
  -IUnderstandLocalTestData | ConvertFrom-Json

$voucherId = $baseline.voucher_id
$baseline
```

手机号必须是未在当前测试券下单的新本地测试账号。重复演示时更换号码后缀，不要删除订单或重置数据库。

## 5. 场景 A：Kafka Consumer Down

### 5.1 注入

```powershell
& .\scripts\aiops-demo\runtime\inject-kafka-failure.ps1
```

脚本会验证 Web 与 Consumer PID，只停止本轮 Demo Consumer JVM并输出 UTC 故障开始时间；Web、Kafka、Redis、MySQL、Agent 和 Console 保持运行。

继续通过真实 HTTP 入口发送至少三笔订单，使 `lag > 1`：

```powershell
& .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase kafka_consumer_down `
  -VoucherId $voucherId `
  -Phones @('19920925002','19920925003','19920925004') `
  -IUnderstandLocalTestData
```

等待两个约 30 秒的 Kafka 巡检窗口。Console 应出现实时 Incident，并展示：

- `member_count=0`、`lag>1`。
- `kafka-consumer-diagnosis` Skill。
- `get_kafka_status` Tool 调用。
- Kafka Hypothesis 从 `UNKNOWN` 到 `SUPPORTED`。
- Diagnosis 完成后 Incident 仍保持 ACTIVE，直到恢复检测确认。

### 5.2 恢复

```powershell
& .\scripts\aiops-demo\runtime\restore-kafka.ps1
```

脚本使用相同 Profile、Topic 和 Group 重启 Consumer，并更新状态文件与新日志路径。恢复是人工演示操作，不是 Agent 自动修复。等待 Consumer 重新加入 Group、lag 清零和健康窗口出现。

## 6. 场景 B：MySQL Persistence Failure

先确保 Kafka Consumer 已恢复且 Dashboard 能看到健康 Kafka Observation。不要与 Kafka Consumer Down 同时注入。

### 6.1 注入

```powershell
& .\scripts\aiops-demo\runtime\inject-mysql-failure.ps1
```

该脚本调用既有 `inject-consumer-mysql-fault.ps1`，只停止无状态 Consumer MySQL TCP Proxy，并输出代理当前状态。Demo MySQL 容器、Web 的直连数据库路径和所有 volume 保持不变。

使用新的测试账号继续发送两笔订单：

```powershell
& .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase mysql_persistence_timeout `
  -VoucherId $voucherId `
  -Phones @('19920925005','19920925006') `
  -IUnderstandLocalTestData
```

等待业务指标和 MySQL Health 巡检窗口。Console 应展示：

1. Business Metrics：请求、Lua、Kafka 发送增长，订单创建没有增长。
2. Kafka Evidence：Group 稳定、成员存在、lag 正常，Kafka Hypothesis 为 `CONTRADICTED`。
3. MySQL Evidence：`health_state=DEGRADED`、`failure_class=CONNECTION_TIMEOUT`。
4. MySQL Persistence Hypothesis 为 `SUPPORTED`。
5. Action Proposal 仅展示且需要人工审批，不执行任何操作。

### 6.2 恢复

```powershell
& .\scripts\aiops-demo\runtime\restore-mysql.ps1
```

脚本调用既有恢复脚本，仅重新启动 Consumer Proxy，并输出 `running/healthy` 状态。建议恢复后用新的测试账号发送一笔订单，验证真实持久化恢复；脚本不会自动重放 DLT 或补写订单。

## 7. 停止

```powershell
& .\scripts\aiops-demo\runtime\stop-aiops-demo.ps1
```

默认停止状态文件中记录的 Consumer、Web、FastAPI、Monitoring 和 Vue 进程，然后调用 Demo Compose 的 `stop`。不会执行 `docker compose down -v`，不会删除容器、volume、Topic、SQLite、日志或业务数据。

如需保留 Docker 基础设施，只停止宿主进程：

```powershell
& .\scripts\aiops-demo\runtime\stop-aiops-demo.ps1 -KeepDocker
```

## 8. 重复执行与故障互斥

- `inject-kafka-failure.ps1` 在 Consumer 已停止时拒绝重复注入；恢复后可再次执行。
- `restore-kafka.ps1` 在 Consumer 已运行时幂等返回。
- MySQL 注入脚本在 Proxy 已停止时拒绝重复注入；恢复后可再次执行。
- Kafka 和 MySQL 场景应串行执行，中间必须恢复并等待健康窗口。
- 每次重新运行一键启动会使用新的 SQLite；历史运行目录保留用于审计。
- Recorded Replay 与实时 Incident 明确区分，历史 ACTIVE 快照不会计入当前系统健康状态。

## 9. 真实 LLM API 配置位置

真实密钥只写入本机文件：

```text
aiops-agent/.env
```

首次配置：

```powershell
Copy-Item .\aiops-agent\.env.example .\aiops-agent\.env
```

编辑 `.env` 中以下四项：

```dotenv
AI_MODEL_PROVIDER=openai_compatible
AI_MODEL_BASE_URL=https://your-openai-compatible-endpoint/v1
AI_MODEL_API_KEY=your-real-api-key
AI_MODEL_NAME=your-model-or-endpoint-id
AI_MODEL_THINKING=disabled
AI_MODEL_PLANNER_TIMEOUT_SECONDS=30
AI_MODEL_REFLECTION_TIMEOUT_SECONDS=30
AI_MODEL_REPORTER_TIMEOUT_SECONDS=30
```

火山方舟等 OpenAI-compatible 服务同样使用上述标准字段；`AI_MODEL_NAME` 填模型或 Endpoint ID。启动脚本会让 FastAPI 和 Monitoring 子进程从 `aiops-agent/.env` 加载配置，不会把密钥写入 `state.json`、命令行参数、Replay 或 Console。

Console 的后端地址写在 `aiops-console/.env`：

```dotenv
VITE_AIOPS_API_BASE_URL=/api
```

浏览器前端禁止保存模型 API Key。`.env` 已被 Git 忽略；不要把真实密钥写入 `.env.example`、PowerShell 脚本、README、截图或提交记录。任何曾在聊天、日志或仓库中暴露过的 Key 都应先在供应商控制台轮换，再写入本机 `.env`。

## 10. 演示健康检查

一键启动后可检查 Agent、Console、Nginx、Spring Boot 和隔离 Docker 服务：

```powershell
& .\scripts\aiops-demo\runtime\demo-health-check.ps1
```

脚本只读取服务状态，以中文表格输出检查结果；任一检查项异常时返回非零退出码，不会启动、停止或修改服务。
