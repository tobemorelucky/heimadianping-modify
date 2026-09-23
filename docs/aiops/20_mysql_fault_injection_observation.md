# Phase E2.2：MySQL Consumer 专属故障路径预实验记录

> 实验日期：2026-09-23（Asia/Shanghai）。本记录来自隔离 `hmdp-aiops-demo` 环境的真实 HTTP、Kafka、Consumer JVM、MySQL 和 MCP Observation，不使用 Fixture。实验期间未启动 Monitoring Scheduler 或 Incident Manager，因此没有创建 Incident；未修改 Detector、Agent 推理或订单事务逻辑。

## 1. 实验目标与结论

本实验验证了可恢复的 Consumer 专属 MySQL 故障边界，并得到以下结论：

1. Web、Redis、Kafka 和 MySQL Demo 本体在故障期间保持运行；只有 Consumer 到 MySQL 的 TCP 路径被切断。
2. HTTP 仍会受理请求，Lua 准入和 Kafka 发送继续成功，但订单不会落库。
3. Consumer Group 保持活跃。主 Topic lag 会短暂上升，随后因现有错误处理器把记录转入 Retry/DLT 而回到 0。因此“主 Group lag=0”不能证明订单持久化成功。
4. `get_business_metrics` 完整记录了链路断点：请求、Lua、Kafka 均增加，订单成功不增加，失败尝试增加。
5. `get_mysql_health` 的真实失败语义是 `status=partial`、`database_reachable=null`、`connection_test_status=timeout`，不是 `database_reachable=false`。
6. 由于现有 `order-persistence-failure-v1` 明确要求 `database_reachable is false`，本次真实输入的判定结果是 `not_matched`，不会产生 Signal。该结果是安全的保守判定，不应伪造为匹配。

## 2. 故障拓扑

```mermaid
flowchart LR
    U[测试 HTTP 请求] --> W[hmdp-web JVM]
    W --> R[(Redis Demo<br/>Lua 准入)]
    W --> K[(Kafka Demo<br/>main / retry / DLT)]
    K --> C[hmdp-consumer JVM]
    C --> P[Consumer-only TCP Proxy<br/>127.0.0.1:13308]
    P --> M[(MySQL Demo<br/>mysql-demo:3306)]
    W -.Web JDBC 如需使用.-> D[127.0.0.1:13307]
    D --> M

    O[人工故障开关] -.仅 stop/start.-> P
    A[AIOps MCP] -.只读.-> K
    A -.127.0.0.1:18081/18083.-> W
    A -.127.0.0.1:18082.-> C
```

边界说明：

- `hmdp-consumer` 默认 JDBC 地址改为 `127.0.0.1:13308`；`hmdp-web` 仍直接使用 `127.0.0.1:13307`。
- 代理是无状态 BusyBox TCP relay，复用 Demo 已依赖的 `redis:7-alpine` 镜像，不挂载数据卷。
- 代理的停止会关闭既有转发连接，并拒绝 Consumer 的新连接；MySQL 容器、端口 `13307` 和数据卷不受影响。
- Agent 不拥有故障开关。注入和恢复脚本不是 MCP Tool，也不是 Action Executor。

## 3. 注入与恢复方式

### 正常启动

```powershell
& .\scripts\aiops-demo\start-demo.ps1
```

脚本等待 MySQL、Redis、Kafka 和 `mysql-consumer-proxy` 全部健康。正常端口：

| 角色 | 地址 |
| --- | --- |
| Web / 运维直连 MySQL | `127.0.0.1:13307` |
| Consumer 专属 MySQL 代理 | `127.0.0.1:13308` |
| Redis Demo | `127.0.0.1:16379` |
| Kafka Demo | `127.0.0.1:29092` |

### 注入

```powershell
& .\scripts\aiops-demo\inject-consumer-mysql-fault.ps1
```

脚本在操作前确认 Demo MySQL 为 `running/healthy`，然后只执行：

```text
docker compose ... stop mysql-consumer-proxy
```

注入后再次确认 MySQL 仍为 `running/healthy`。脚本不停止 MySQL，不删除容器或 volume，不修改网络和数据。

### 恢复

```powershell
& .\scripts\aiops-demo\restore-consumer-mysql-path.ps1
```

恢复只重新启动并等待代理健康，不重放 DLT、不补订单、不执行数据库修复。实验后 `get_mysql_health` 恢复为 `database_reachable=true / connection_test_status=valid`。

## 4. 故障前基线

JVM 启动后的零点业务快照为五项计数全 0。随后通过真实 HTTP 入口发送一笔健康订单：

```json
{
  "phase": "baseline_e22_healthy",
  "voucher_id": 13,
  "attempted": 1,
  "accepted": 1,
  "order_ids": [640723440260612100]
}
```

MySQL 只读核验确认订单 `640723440260612100` 已落库。故障前 Observation：

| Observation | Evidence ID | 关键事实 |
| --- | --- | --- |
| Business Metrics | `evi_e94a87185412492eb7d2c8fba5b5eddb` | `requests=1, lua=1, kafka_sent=1, created=1, failed=0`；`success/complete` |
| Kafka Status | `evi_8f67a2b0c3634ff189a6a1cac34566b9` | `stable, member_count=1, committed=4, end=4, lag=0`；`success/complete` |
| MySQL Health | `evi_7528e48419a84d6d925f431d92cffde4` | `database_reachable=true, connection_test_status=valid, active=0, idle=10`；Envelope 为 `partial`，因为累计 timeout/error 指标不可用 |

MySQL Health 的 `partial` 是既有诚实缺失语义，并不表示连接异常。

## 5. 故障期间真实行为

停止 Consumer 代理后，Demo MySQL 仍显示 `running/healthy`。立即采集到的 MySQL Evidence `evi_6c8be0ea1dae461baa3d2b796dbccdeb` 已变为：

```json
{
  "status": "partial",
  "kind": "mysql_health",
  "data": {
    "source_role": "hmdp-consumer",
    "database_reachable": null,
    "connection_test_status": "timeout",
    "hikari_active": 0,
    "hikari_idle": 0,
    "connection_timeout_count": null,
    "error_count": null
  }
}
```

随后通过真实 HTTP 入口发送两笔故障期订单：

```json
{
  "phase": "fault_e22_consumer_mysql_path",
  "voucher_id": 14,
  "attempted": 2,
  "accepted": 2,
  "order_ids": [640723801037864965, 640723805332832262]
}
```

HTTP 的 `accepted=2` 仅表示 Lua 准入和 Kafka Broker 确认成功。MySQL 只读查询最终只返回健康基线订单；上述两笔故障订单均未落库。

### 5.1 早期窗口

| Observation | Evidence ID | 真实结果 |
| --- | --- | --- |
| Business Metrics | `evi_1db470b2c8584be283ec3fd9ceeae69b` | 累计 `3/3/3/1/0`；两笔消息已进入链路，Listener 当时仍等待 Hikari 的 30 秒取连接超时 |
| Kafka Status | `evi_06bdcbb75d294cbb88e2c1e1830fcab4` | Group `stable/member=1`；`committed=5, end=6, lag=1` |
| MySQL Health | `evi_d2e6800ce871460a9ccaf033db0e0db3` | `partial, database_reachable=null, connection_test_status=timeout, active=0, idle=0` |

这说明 Consumer 没有宕机，而是在事务开始时等待数据库连接；短期 main lag 可以出现，但不是 Consumer Down。

### 5.2 错误处理完成后的稳定窗口

约 70 秒后采集：

| Observation | Evidence ID | 真实结果 |
| --- | --- | --- |
| Business Metrics | `evi_6fbbd47b3c4e47cd993e4dd63605058c` | 累计 `requests=3, lua=3, kafka_sent=3, created=1, failed=4`；相对健康基线增量为 `+2/+2/+2/+0/+4` |
| Kafka Status | `evi_02aac2621b6c4b45969ebad58ffad733` | Group 仍 `stable/member=1`；`committed=6, end=6, lag=0` |
| MySQL Health | `evi_868f113c61104e0f85a3559243be772d` | `partial, database_reachable=null, connection_test_status=timeout, active=0, idle=0` |

Consumer 日志记录了真实 `CannotCreateTransactionException`、Hikari 30 秒 connection timeout、MySQL `CommunicationsException` 和底层 `Connection refused`。现有错误处理链把两笔记录分别从 main 转入 Retry，失败后再进入 DLT：

| Group | 故障后 offset | end | lag | 状态 |
| --- | ---: | ---: | ---: | --- |
| main `hmdp-aiops-mysql-demo-consumer-v1` | 6 | 6 | 0 | 有活跃成员 |
| retry `hmdp-aiops-mysql-demo-retry-v1` | 3 | 3 | 0 | 有活跃成员 |
| DLT `hmdp-aiops-mysql-demo-dlt-v1` | 3 | 3 | 0 | 有活跃成员 |

`order_created_failure_count=4` 是 **Listener 持久化失败尝试次数**：两笔订单各在 main 和 retry 失败一次，不是四笔不同订单。后续指标命名或报告必须保留这一语义，不能解释为四笔唯一订单失败。

## 6. 是否满足现有 Detector

使用真实基线与稳定故障窗口，可形成以下输入：

```text
Business delta:
  request +2
  Lua admission +2
  Kafka publish +2
  order success +0
  persistence attempt failure +4

Kafka:
  status=success, completeness=complete
  member_count=1, consumer_status=stable
  total_lag=0, lag_status=normal

MySQL:
  status=partial
  source_role=hmdp-consumer
  database_reachable=null
  connection_test_status=timeout
```

业务退化和 Kafka 健康条件均满足，但当前规则在检查 MySQL 时执行：

```python
database_reachable is False
and connection_test_status in {
    "invalid", "connection_failed", "timeout", "acquisition_error"
}
```

真实 `database_reachable` 为 `null`，因此 assessment 为：

```json
{
  "outcome": "not_matched",
  "reason": "Consumer MySQL failure was not observed"
}
```

结论：**当前真实故障不满足 `order-persistence-failure-v1` 的全部条件。** 本阶段未改规则、未生成 Signal、未创建 Incident。把 `null` 手工改成 `false`、使用 Fixture 替换、或只根据业务下降断言 MySQL 根因，都违反 Evidence 约束。

## 7. 恢复观测

重新启动代理后，Evidence `evi_17ea653e2087469d87be1196ce33c6be` 返回：

```json
{
  "status": "partial",
  "data": {
    "database_reachable": true,
    "connection_test_status": "valid",
    "hikari_active": 0,
    "hikari_idle": 10
  }
}
```

恢复只证明 Consumer 数据库连接路径重新健康。故障期消息已经进入 DLT，不会被本脚本自动重放，故障期缺失订单也不会自动补写。

## 8. 下一步修改建议

本阶段保留现有安全失败语义。进入后续实现前建议单独评审：

1. **区分探针超时与数据库不可达。** 当前健康出口在 1.5 秒先于 Hikari 30 秒失败，得到 `reachable=null/timeout`。可考虑为 Demo Profile 设置更短的 Hikari acquisition timeout，并让健康探针等待略长于它；或增加不执行 SQL的、受限 TCP/JDBC connection-only 探针。两种方案都必须保持固定目标、脱敏错误分类和只读边界。
2. **不要直接放宽 Detector 为 `null + timeout = MySQL failure`。** `null/timeout` 也可能来自连接池繁忙或探针线程饱和。若要支持，应要求业务退化、Kafka 健康、连续多个 MySQL timeout 窗口，并引入可区分的 `failure_class` 后再评审规则版本。
3. **澄清失败计数名称。** 当前 `order_created_failure_count` 实际按 main/retry Listener 尝试累计。未来可改为 `order_persistence_attempt_failure_count`，或增加按 message/order ID 去重的只读计数；不能在报告中把尝试数当唯一失败订单数。
4. **补充 Retry/DLT 只读语义。** 主 Topic lag 归零是错误路由成功，不是业务恢复。未来 Kafka Evidence 应显式关联 Retry/DLT 增量，或由现有日志 Evidence 交叉验证。
5. **恢复识别必须使用新业务流量。** MySQL Health 恢复不足以证明订单持久化恢复；应在人工恢复后发送新的隔离测试订单，再观察 success 增量。禁止自动重放或自动修复。

## 9. 文件与安全边界

本阶段新增/调整：

- `docker-compose.aiops-demo.yml`：增加无状态 `mysql-consumer-proxy`，独立端口 `13308`。
- `application-consumer-aiops-demo.yaml`：仅 Demo Consumer JDBC 默认经过代理。
- `scripts/aiops-demo/start-demo.ps1`：等待并显示代理健康状态。
- `scripts/aiops-demo/inject-consumer-mysql-fault.ps1`：仅停止代理，并复核 MySQL 健康。
- `scripts/aiops-demo/restore-consumer-mysql-path.ps1`：仅恢复代理。

未修改业务事务、Detector、Agent Runtime、MCP Tool、Console、数据库结构或数据卷。实验操作没有执行 `down -v`、删表、删 Topic、offset 修改、消息重放或自动修复。

## 10. E2.3 语义归一化后的真实复验

E2.3 在不改变故障脚本和业务事务的前提下，将真实
`database_reachable=null + connection_test_status=timeout` 规范化为：

```json
{
  "health_state": "DEGRADED",
  "failure_class": ["CONNECTION_TIMEOUT"]
}
```

使用新的隔离 SQLite 保存两轮 Scheduler 采集。健康基线业务计数为全 0；停止相同 Consumer 代理并经 HTTP 接受两笔新订单后，真实采集为：

- 业务增量：请求 `+2`、Lua `+2`、Kafka 发送 `+2`、订单成功 `+0`、持久化失败尝试 `+3`。
- Kafka：`stable`、`member_count=1`、`lag=0`。
- MySQL：Envelope `partial`，`health_state=DEGRADED`，`failure_class=[CONNECTION_TIMEOUT]`。

规则 `order-persistence-failure-v1` 的内部版本 2 产生 Signal
`sig_81e04fec62814faf98ca84e6ddc199b1`，随后 Incident Manager 创建
`inc_9409e7bc55144ad99321a6c508ab17d1`：

```json
{
  "incident_status": "ACTIVE",
  "diagnosis_status": "COMPLETED",
  "root_cause": "MySQL Persistence Failure",
  "confidence": 0.93
}
```

Incident 在诊断完成后仍为 `ACTIVE`，符合“Diagnosis completed 不等于系统恢复”的生命周期语义。复验结果已以真实 Tool Observation 形式保存到 `aiops-agent/evaluation/recordings/MYSQL_PERSISTENCE_TIMEOUT_001.json`；该目录不是 Fixture。实验结束后由人工恢复代理，没有自动修复或消息重放。
