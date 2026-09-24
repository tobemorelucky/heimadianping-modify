# AIOps Demo 操作手册

> 本手册面向本地开发和面试演示。故障注入与恢复是后台演示工具，不属于 AIOps Console 功能。Console 始终保持只读，只展示系统状态、Incident、Agent 诊断过程、Evidence、Hypothesis、Report 和 Proposal。

## 1. 演示边界

完整演示流程是：

```text
启动系统
  ↓
建立正常状态
  ↓
后台脚本注入故障
  ↓
Monitoring 自动采集
  ↓
Detector 生成 AnomalySignal
  ↓
Incident Manager 创建或合并 Incident
  ↓
Agent 自动诊断
  ↓
Console 只读展示
  ↓
后台脚本恢复故障
  ↓
连续健康检测确认恢复
  ↓
Incident RECOVERED
```

本阶段没有修改 Agent Runtime、Detector、Incident 去重、Console 交互或业务逻辑。相同 fingerprint 的活动故障继续由现有 cooldown 和 Incident 去重逻辑合并，不会因为重复 Monitoring Tick 创建多个活动 Incident。故障注入脚本本身也拒绝对已经停止的 Consumer 或 Proxy重复注入。

所有命令默认在仓库根目录执行：

```powershell
Set-Location 'E:\work\code\java\hm-dianping-total'
```

## 2. 一键启动

正常启动：

```powershell
& .\scripts\aiops-start-all.ps1
```

需要重新构建 Spring Boot Jar 时：

```powershell
& .\scripts\aiops-start-all.ps1 -Rebuild
```

不自动打开浏览器时：

```powershell
& .\scripts\aiops-start-all.ps1 -NoOpen
```

统一入口直接复用 `scripts/aiops-demo/runtime/start-aiops-demo.ps1`，不会复制进程管理逻辑。启动内容和顺序为：

1. Demo Docker：MySQL、Redis、Kafka、Consumer MySQL Proxy；
2. Consumer JVM；
3. Web JVM；
4. AIOps Agent API；
5. Monitoring Scheduler；
6. AIOps Console。

启动成功后访问：

- Console：[http://127.0.0.1:5173](http://127.0.0.1:5173)
- Agent：[http://127.0.0.1:8010](http://127.0.0.1:8010)
- Web：[http://127.0.0.1:8081](http://127.0.0.1:8081)

PID、日志和本次 SQLite 数据保存在：

```text
target/runtime/aiops-demo-runtime/state.json
target/runtime/aiops-demo-runtime/runs/<启动时间>/
```

## 3. 正常状态验证

先执行已有只读健康检查：

```powershell
& .\scripts\aiops-demo\runtime\demo-health-check.ps1
```

当前 AIOps Demo 不依赖 Nginx，`8080` 未启动不影响演示。必须正常的是 `8081`、`18081`、`18082`、`18083`、`8010` 和 `5173`。

### 3.1 Dashboard

打开 Console，正常基线应显示：

- 系统状态：健康 Healthy；
- Active Incident：0；
- 最近巡检时间持续更新；
- 历史 Replay 不计入 Active Incident。

### 3.2 Kafka

检查 Consumer Group：

```powershell
docker compose --project-name hmdp-aiops-demo `
  --file .\docker-compose.aiops-demo.yml exec --no-TTY kafka-demo `
  /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka-demo:19092 `
  --describe --group hmdp-aiops-mysql-demo-consumer-v1 --state
```

检查 lag：

```powershell
docker compose --project-name hmdp-aiops-demo `
  --file .\docker-compose.aiops-demo.yml exec --no-TTY kafka-demo `
  /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka-demo:19092 `
  --describe --group hmdp-aiops-mysql-demo-consumer-v1
```

正常状态要求：

```text
member_count = 1
lag = 0
```

### 3.3 MySQL

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:18082/internal/aiops/mysql-health' |
  ConvertTo-Json -Depth 8
```

正常状态要求：

```text
source_role = hmdp-consumer
database_reachable = true
connection_test_status = valid
health_state = HEALTHY（由 Agent 规范化）
```

某些 Hikari 累计指标无法可信采集时会返回 `null` 或 `unavailable`，不能解释成数值 0。

## 4. Kafka Consumer Down 演示

### 4.1 注入前提

确认：

- Kafka Group 有 1 个成员；
- lag 为 0；
- Web、Agent、Scheduler 均运行；
- MySQL Proxy 正常；
- 已经有至少一次成功消费，用于建立 committed offset。

### 4.2 后台注入故障

```powershell
& .\scripts\aiops-demo\faults\inject-kafka-consumer-down.ps1
```

脚本只停止 `state.json` 中记录的 Consumer JVM。以下组件保持运行：

- Web JVM；
- Kafka；
- MySQL 和 Redis；
- Agent API；
- Monitoring Scheduler；
- Console。

脚本输出：

```text
Kafka Consumer故障已注入
等待Monitoring检测...
```

### 4.3 通过真实入口发送订单

使用新的本地测试手机号：

```powershell
$faultOrders = & .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase kafka_consumer_down `
  -Phones @('19920927001','19920927002','19920927003') `
  -Stock 10 `
  -IUnderstandLocalTestData | ConvertFrom-Json

$faultOrders
```

该辅助脚本经过真实 HTTP、Redis Lua 和 Kafka Producer，不直接写 Kafka 或数据库。

### 4.4 观察自动诊断

Kafka CLI 应显示：

```text
member_count = 0
lag > AIOPS_KAFKA_LAG_THRESHOLD
```

等待所需的连续 Monitoring 窗口后，Console 应依次展示：

```text
Monitoring Observation
  ↓
AnomalySignal
  ↓
Incident ACTIVE
  ↓
Diagnosis RUNNING
  ↓
Kafka Evidence：无活动成员且 lag 累积
  ↓
Kafka Consumer Failure Hypothesis SUPPORTED
  ↓
Diagnosis COMPLETED
```

诊断完成不等于故障恢复。因此此时必须是：

```text
Incident status = ACTIVE
Diagnosis status = COMPLETED
```

Console 中的 Proposal 只用于展示，不会自动重启 Consumer。

### 4.5 后台恢复

```powershell
& .\scripts\aiops-demo\faults\recover-kafka-consumer.ps1
```

恢复脚本会：

- 使用原 Consumer Profile 重启 JVM；
- 等待 Consumer 稳定加入 `hmdp-aiops-mysql-demo-consumer-v1`；
- 保留 Kafka Topic、offset 和数据；
- 不清理 lag，不删除消息，不执行数据补偿。

Consumer 恢复后 lag 会由正常消费逐步下降。等待连续健康检测窗口，最终应看到：

```text
Incident ACTIVE
  ↓
健康 Observation
  ↓
恢复条件满足
  ↓
Incident RECOVERED
```

## 5. MySQL Consumer Timeout 演示

### 5.1 注入前提

确保：

- Consumer 正常运行；
- Kafka Group member=1；
- Kafka lag=0；
- MySQL Health 为 HEALTHY；
- 不与 Kafka Consumer Down 故障同时演示。

### 5.2 后台注入故障

```powershell
& .\scripts\aiops-demo\faults\inject-mysql-timeout.ps1
```

脚本只停止：

```text
hmdp-aiops-demo-mysql-consumer-proxy
```

以下组件保持运行：

- Demo MySQL 本身；
- Web 对 MySQL 的直连路径；
- Consumer JVM；
- Kafka 和 Redis；
- Agent、Scheduler 和 Console。

### 5.3 发送真实测试订单

```powershell
$faultOrders = & .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase mysql_persistence_timeout `
  -Phones @('19920927004','19920927005') `
  -Stock 10 `
  -IUnderstandLocalTestData | ConvertFrom-Json

$faultOrders
```

预期业务现象：

- 秒杀请求数量增加；
- Redis Lua 准入成功增加；
- Kafka 消息发送增加；
- Kafka Consumer Group 仍有活动成员；
- 订单创建成功数不再等量增加，或失败数增加；
- Consumer MySQL Health 出现 timeout/DEGRADED。

### 5.4 观察三源 Evidence 和假设变化

Console 应展示：

```text
Business Metrics
请求正常 / Lua 正常 / Kafka 发送正常 / 订单持久化异常
  ↓
H1 Kafka Consumer Failure
  ↓
Kafka Evidence：Group 有成员，Kafka 路径正常
  ↓
H1 CONTRADICTED
  ↓
MySQL Evidence：CONNECTION_TIMEOUT / DEGRADED
  ↓
H2 MySQL Persistence Failure SUPPORTED
  ↓
Diagnosis Report
```

Detector 必须结合 Business Metrics、Kafka 和 MySQL Observation。任一关键来源缺失或为不可判定 error 时，应返回 insufficient，不能仅凭订单下降猜测 MySQL 根因。

### 5.5 后台恢复

```powershell
& .\scripts\aiops-demo\faults\recover-mysql-timeout.ps1
```

恢复脚本会：

- 恢复无状态 MySQL Proxy；
- 等待 Consumer 健康出口重新报告 `database_reachable=true` 和 `connection_test_status=valid`；
- 不修改数据库；
- 不修改表结构；
- 不补偿订单；
- 不重放 Kafka 消息。

恢复后等待连续健康检测，Incident 状态应从 `ACTIVE` 变为 `RECOVERED`。故障期间产生的 Retry/DLT 或未成功订单只作为诊断事实保留，不由恢复脚本处理。

## 6. 一键停止

完成演示后执行：

```powershell
& .\scripts\aiops-stop-all.ps1
```

统一入口复用 `scripts/aiops-demo/runtime/stop-aiops-demo.ps1`，按以下顺序停止：

```text
Console
  ↓
Monitoring Scheduler
  ↓
Agent API
  ↓
Web JVM
  ↓
Consumer JVM
  ↓
Demo Docker
```

如需保留 Docker 服务运行：

```powershell
& .\scripts\aiops-stop-all.ps1 -KeepDocker
```

停止过程不会删除：

- Docker Volume；
- Kafka Topic；
- Kafka 数据；
- MySQL 数据；
- SQLite Incident/Trace；
- 运行日志；
- 环境配置。

## 7. 面试演示建议顺序

1. 执行 `aiops-start-all.ps1`；
2. 在 Dashboard 展示 Healthy、最近巡检和 Active Incident=0；
3. 后台注入 Kafka Consumer Down；
4. 用真实 HTTP 入口发送少量订单；
5. 展示 Monitoring → Incident → Skill → Tool → Evidence → Hypothesis → Report；
6. 后台恢复 Consumer，展示 ACTIVE → RECOVERED；
7. 确认 Kafka 恢复健康后，再单独注入 MySQL Timeout；
8. 展示 Kafka 假设被反驳、MySQL 假设得到支持；
9. 后台恢复 MySQL Proxy并等待 RECOVERED；
10. 执行 `aiops-stop-all.ps1`。

整个过程中不要在 Console 增加或展示故障控制按钮。故障控制始终由操作者在后台 PowerShell 中完成，Console 只负责呈现可审计的 Agent Harness 运行结果。
