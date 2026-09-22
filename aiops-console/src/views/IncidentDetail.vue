<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { getIncident, isAgentOffline } from '../api/client.js'
import AgentOffline from '../components/AgentOffline.vue'
import { formatTime, percent } from '../format.js'
import IncidentTimeline from '../components/IncidentTimeline.vue'
import EvidenceCard from '../components/EvidenceCard.vue'
import HypothesisCard from '../components/HypothesisCard.vue'
import SkillCard from '../components/SkillCard.vue'
import TraceReplay from '../components/TraceReplay.vue'

const props = defineProps({ id: { type: String, required: true } })
const incident = ref(null)
const loading = ref(true)
const error = ref('')
const offline = ref(false)
const replayMode = ref(false)

async function load() {
  loading.value = true
  error.value = ''
  offline.value = false
  incident.value = null
  replayMode.value = false
  try { incident.value = await getIncident(props.id) }
  catch (failure) {
    if (isAgentOffline(failure)) offline.value = true
    else error.value = '此 Incident 不存在或不可访问。'
  }
  finally { loading.value = false }
}
onMounted(load)
watch(() => props.id, load)

const skill = computed(() => {
  const selected = incident.value?.traces.find(event => event.event_type === 'skill_selected')
  if (!selected) return null
  return { ...selected.payload, loaded: incident.value.traces.some(event => event.event_type === 'skill_loaded' && event.payload.skill_id === selected.payload.skill_id) }
})
const tools = computed(() => incident.value?.traces.filter(event => event.event_type === 'tool_called') || [])
</script>

<template>
  <div class="page-enter">
    <RouterLink class="back-link" to="/">← 返回 Overview</RouterLink>
    <div v-if="loading" class="loading-state">正在读取诊断轨迹…</div>
    <AgentOffline v-else-if="offline" :retry="load" />
    <div v-else-if="error" class="error-banner" role="alert">{{ error }}</div>
    <template v-else-if="incident">
      <div class="page-heading incident-heading"><div><span v-if="incident.demo_scenario" class="eyebrow">FAULTBENCH DEMO / {{ incident.demo_fault_id }}</span><span v-else class="eyebrow">INCIDENT / {{ incident.incident_id }}</span><h1>{{ incident.demo_scenario || incident.title }}<span class="heading-dot">.</span></h1><p v-if="incident.demo_scenario">秒杀订单异步创建延迟。Agent 将依据 Kafka 状态、应用日志及当前 Evidence 验证故障假设。</p><p v-else>{{ incident.description || '由监控异常信号触发的 Agent 诊断任务。' }}</p><small v-if="incident.demo_scenario" class="incident-original-title" :title="incident.title">Incident title: {{ incident.title }}</small></div><div class="incident-actions"><span class="status-chip large" :class="incident.status.toLowerCase()">Incident · {{ incident.status }}</span><span class="status-chip large" :class="(incident.diagnosis_status || '').toLowerCase()">Diagnosis · {{ incident.diagnosis_status || 'NOT STARTED' }}</span><button type="button" class="ghost-button" :aria-pressed="replayMode" @click="replayMode = !replayMode">{{ replayMode ? '退出 Replay' : '▶ Replay' }}</button></div></div>
      <div class="meta-strip"><span><b>CREATED</b>{{ formatTime(incident.created_at) }}</span><span><b>SEVERITY</b>{{ incident.severity }}</span><span><b>AGENT RUNTIME</b>{{ incident.runtime_status || 'not started' }}</span><span><b>TRIGGER SIGNALS</b>{{ incident.trigger_signal_ids.length }}</span></div>
      <TraceReplay v-if="replayMode" :incident="incident" />
      <div v-else class="detail-layout">
        <div class="detail-main">
          <section class="panel"><div class="section-head"><div><span class="eyebrow">AGENT TRAJECTORY</span><h2>Investigation timeline</h2></div><span class="count-label">{{ incident.traces.length }} EVENTS</span></div><IncidentTimeline :events="incident.traces" /></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">CURRENT FACTS</span><h2>Evidence</h2></div><span class="count-label">{{ incident.evidence.length }} CARDS</span></div><div v-if="incident.evidence.length" class="card-stack"><EvidenceCard v-for="item in incident.evidence" :key="item.evidence_id" :evidence="item" /></div><p v-else class="empty-small">尚无当前事故 Evidence。</p></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">EVIDENCE-GROUNDED REASONING</span><h2>Hypotheses</h2></div><span class="count-label">{{ incident.hypotheses.length }} TRACKED</span></div><div v-if="incident.hypotheses.length" class="card-stack"><HypothesisCard v-for="item in incident.hypotheses" :key="item.hypothesis_id" :hypothesis="item" /></div><p v-else class="empty-small">尚无已记录的 Hypothesis。</p></section>
        </div>
        <div class="detail-aside">
          <section class="panel"><div class="section-head"><div><span class="eyebrow">DOMAIN GUIDANCE</span><h2>Skill</h2></div></div><SkillCard :skill="skill" /></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">READ-ONLY MCP</span><h2>Tool sequence</h2></div></div><ol v-if="tools.length" class="tool-list"><li v-for="(event, index) in tools" :key="event.event_id"><span>{{ String(index + 1).padStart(2, '0') }}</span><strong>{{ event.payload.tool_name }}</strong><small>{{ formatTime(event.created_at) }}</small></li></ol><p v-else class="empty-small">尚未调用工具。</p></section>
          <section class="panel report-panel"><div class="section-head"><div><span class="eyebrow">AGENT OUTPUT</span><h2>Diagnosis report</h2></div></div><template v-if="incident.report"><span class="status-chip" :class="incident.report.status">{{ incident.report.status }}</span><h3>{{ incident.report.root_cause || '根因尚未确认' }}</h3><p>{{ incident.report.conclusion }}</p><div class="report-confidence">Confidence <strong>{{ percent(incident.report.confidence) }}</strong></div></template><p v-else class="empty-small">报告尚未生成。</p></section>
          <section class="panel governance-panel"><div class="section-head"><div><span class="eyebrow">PERMISSION GOVERNANCE</span><h2>Proposal</h2></div></div><p>{{ incident.proposals.length ? `${incident.proposals.length} 条只读治理建议待查看。` : '本次 Incident 尚无 Action Proposal。' }}</p><RouterLink v-if="incident.proposals.length" class="text-link" :to="`/incidents/${incident.incident_id}/proposal`">查看治理结果 ↗</RouterLink><small>此控制台不提供操作执行入口。</small></section>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.incident-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  align-items: center;
  gap: 10px;
}
.status-chip.active {
  color: #f2b37d;
  background: #4a312a;
}
.status-chip.recovered {
  color: #75ddba;
  background: #203d38;
}
</style>
