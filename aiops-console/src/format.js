export function formatTime(value) {
  if (!value) return '尚无记录'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit'
  }).format(date)
}

export function percent(value) {
  return `${Math.round((Number(value) || 0) * 100)}%`
}

export function shortId(value) {
  if (!value) return '—'
  return value.length > 22 ? `${value.slice(0, 12)}…${value.slice(-6)}` : value
}

const statusNames = {
  ACTIVE: '进行中 ACTIVE', RECOVERED: '已恢复 RECOVERED', ACKNOWLEDGED: '已确认 ACKNOWLEDGED',
  PENDING: '等待诊断 PENDING', RUNNING: '诊断中 RUNNING', COMPLETED: '诊断完成 COMPLETED', FAILED: '失败 FAILED',
  queued: '等待诊断 queued', planning: '规划中 planning', investigating: '调查中 investigating',
  reflecting: '反思中 reflecting', reporting: '生成报告中 reporting', awaiting_human: '等待人工复核 awaiting_human', failed: '失败 failed',
  UNKNOWN: '待验证 UNKNOWN', SUPPORTED: '已支持 SUPPORTED', CONTRADICTED: '已反驳 CONTRADICTED', REJECTED: '已排除 REJECTED',
  success: '成功 success', partial: '部分完成 partial', error: '错误 error', timeout: '超时 timeout',
  confirmed: '已确认 confirmed', inconclusive: '证据不足 inconclusive',
  ALLOW: '允许 ALLOW', REQUIRE_APPROVAL: '需审批 REQUIRE_APPROVAL', DENY: '拒绝 DENY'
}

const kindNames = {
  log: '应用日志', kafka_consumer_status: 'Kafka 消费状态',
  business_metrics: '业务指标', mysql_health: 'MySQL 健康状态'
}

const toolNames = {
  search_application_logs: '应用日志检索', get_kafka_status: 'Kafka 状态检查',
  get_business_metrics: '业务指标检查', get_mysql_health: 'MySQL 健康检查'
}

const skillNames = {
  'incident-triage': '通用故障分诊',
  'kafka-consumer-diagnosis': 'Kafka 消费者诊断'
}

export function statusLabel(value) {
  return statusNames[value] || value || '未知'
}

export function kindLabel(value) {
  return kindNames[value] ? `${kindNames[value]} · ${value}` : value || '未知来源'
}

export function toolLabel(value) {
  return toolNames[value] ? `${toolNames[value]} · ${value}` : value || '未知工具'
}

export function skillLabel(value) {
  return skillNames[value] ? `${skillNames[value]} · ${value}` : value || '未知技能'
}

export function severityLabel(value) {
  const normalized = String(value || '').toLowerCase()
  const names = { critical: '严重', high: '高', medium: '中', low: '低' }
  return names[normalized] ? `${names[normalized]} ${normalized.toUpperCase()}` : value || '未知'
}

export function decisionLabel(value) {
  const names = { continue: '继续调查', replan: '重新规划', report: '生成报告', inconclusive: '证据不足' }
  return names[value] ? `${names[value]} · ${value}` : value || '未知'
}

export function scenarioLabel(value) {
  const names = {
    'Kafka Consumer Down · Real': 'Kafka 消费者停机 · 真实记录',
    'MySQL Persistence Failure · Real': 'MySQL 持久化失败 · 真实记录'
  }
  return names[value] || value
}

export function sourceLabel(value) {
  const names = {
    alert: '监控告警', recorded_observation: '真实观测记录', faultbench: '故障基准评测', user: '人工提交', human: '人工提交'
  }
  return names[value] ? `${names[value]} · ${value}` : value || '未知来源'
}

const localizedTexts = {
  'Kafka Consumer Failure': 'Kafka 消费者故障',
  'MySQL Persistence Failure': 'MySQL 持久化故障',
  'No active consumers': '消费者组没有活跃成员',
  'Consumer group has no members': '消费者组没有活跃成员',
  'No active members and lag': '消费者组无活跃成员且消息持续积压',
  'order-persistence-failure-v1 correlated Business, Kafka and MySQL observations.': '规则 order-persistence-failure-v1 已关联业务指标、Kafka 与 MySQL 观测。',
  'Created proactive Incident from anomaly signal.': '已根据异常信号主动创建故障事件。',
  'Started Agent Runtime for proactive diagnosis.': '已启动智能诊断运行时进行主动诊断。',
  'Selected Kafka Consumer Diagnosis for the seckill async path.': '已为秒杀异步链路选择 Kafka 消费者诊断技能。',
  'Loaded skill guidance; Evidence remains authoritative.': '已加载技能指导；最终判断仍以证据为准。',
  'Starting MCP tool get_business_metrics.': '正在调用 MCP 工具 get_business_metrics。',
  'Starting MCP tool get_kafka_status.': '正在调用 MCP 工具 get_kafka_status。',
  'Starting MCP tool get_mysql_health.': '正在调用 MCP 工具 get_mysql_health。',
  'get_business_metrics returned a successful real Observation.': 'get_business_metrics 返回成功的真实观测。',
  'get_kafka_status returned a successful real Observation.': 'get_kafka_status 返回成功的真实观测。',
  'get_mysql_health returned a partial real Observation.': 'get_mysql_health 返回部分完整的真实观测。',
  'Requests, Lua admission and Kafka sends increased, but no order was created.': '请求、Lua 准入和 Kafka 发送均增长，但没有订单创建成功。',
  'Business metrics prove downstream degradation but are insufficient to distinguish Kafka from persistence.': '业务指标证明下游发生退化，但尚不足以区分 Kafka 消费与数据库持久化问题。',
  'Kafka group is stable with one member and zero lag.': 'Kafka 消费组状态稳定，存在 1 个成员且积压为 0。',
  'Healthy Kafka evidence contradicts the Kafka Consumer Failure hypothesis.': 'Kafka 健康证据反驳了 Kafka 消费者故障假设。',
  'Consumer MySQL health is DEGRADED with CONNECTION_TIMEOUT.': 'Consumer 的 MySQL 健康状态为 DEGRADED，失败类型为 CONNECTION_TIMEOUT。',
  'MySQL Persistence Failure is supported by the Consumer connection timeout and cross-source evidence.': 'Consumer 连接超时及跨来源证据支持 MySQL 持久化故障假设。',
  'Reflection selected report after Kafka contradiction and MySQL support.': 'Kafka 假设被反驳且 MySQL 假设获得支持后，反思阶段决定生成报告。',
  'Consumer MySQL connection timeout caused order persistence failure while Kafka remained healthy.': 'Kafka 保持健康时，Consumer 的 MySQL 连接超时导致订单持久化失败。',
  'Agent Runtime completed proactive diagnosis.': '智能诊断运行时已完成主动诊断。',
  'Created a proposal only; no action was executed.': '仅生成操作建议，没有执行任何动作。',
  'Permission Gateway requires explicit human approval.': '权限网关要求明确的人工审批。',
  'Kafka consumer group is stable with one member and zero lag.': 'Kafka 消费组状态稳定，存在 1 个成员且积压为 0。',
  'Consumer MySQL health is degraded: CONNECTION_TIMEOUT.': 'Consumer 的 MySQL 健康状态已退化：CONNECTION_TIMEOUT。',
  'Business conversion degraded while Kafka remained healthy; the Consumer MySQL path timed out.': '业务转化下降，但 Kafka 保持健康；Consumer 的 MySQL 路径发生连接超时。'
}

export function localizeText(value) {
  if (!value) return value
  if (localizedTexts[value]) return localizedTexts[value]
  if (/^Requests=\d+, Lua accepted=\d+, Kafka sent=\d+, orders created=\d+, persistence failures=\d+\.$/.test(value)) {
    return value
      .replace('Requests=', '请求数=')
      .replace('Lua accepted=', 'Lua 准入数=')
      .replace('Kafka sent=', 'Kafka 发送数=')
      .replace('orders created=', '订单创建成功数=')
      .replace('persistence failures=', '持久化失败数=')
  }
  return value
}

export function factLabel(value) {
  const fieldNames = {
    seckill_request_count: '秒杀请求数', lua_admission_success_count: 'Lua 准入成功数',
    kafka_message_sent_count: 'Kafka 消息发送数', order_created_success_count: '订单创建成功数',
    order_created_failure_count: '订单创建失败数', consumer_status: '消费者状态',
    member_count: '消费者成员数', total_lag: '总积压量', lag_status: '积压状态',
    source_role: '来源角色', database_reachable: '数据库可达性',
    connection_test_status: '连接测试状态', health_state: '健康状态', failure_class: '故障分类'
  }
  const match = String(value).match(/^([^:]+):\s*(.*)$/)
  if (!match || !fieldNames[match[1]]) return value
  return `${fieldNames[match[1]]}（${match[1]}）：${match[2]}`
}

export function actionLabel(value) {
  const names = { restart_consumer: '重启消费者' }
  return names[value] ? `${names[value]} · ${value}` : value
}
