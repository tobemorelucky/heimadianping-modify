<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { getIncident, isAgentOffline } from '../api/client.js'
import AgentOffline from '../components/AgentOffline.vue'
import { formatTime, localizeText, percent, scenarioLabel, severityLabel, statusLabel, toolLabel } from '../format.js'
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
const refreshing = ref(false)
const lastUpdatedAt = ref(null)
let refreshTimer = null
let loadPromise = null

function load(reset = false) {
  if (loadPromise) return loadPromise
  if (reset) {
    loading.value = true
    incident.value = null
    replayMode.value = false
  }
  refreshing.value = true
  error.value = ''
  loadPromise = (async () => {
    try {
      incident.value = await getIncident(props.id)
      offline.value = false
      lastUpdatedAt.value = new Date().toISOString()
    } catch (failure) {
      if (isAgentOffline(failure)) offline.value = true
      else error.value = '此故障事件不存在或不可访问。'
    } finally {
      loading.value = false
      refreshing.value = false
      loadPromise = null
    }
  })()
  return loadPromise
}
onMounted(() => {
  load(true)
  refreshTimer = setInterval(() => load(false), 3000)
})
onBeforeUnmount(() => {
  if (refreshTimer !== null) clearInterval(refreshTimer)
  refreshTimer = null
})
watch(() => props.id, () => load(true))

const skill = computed(() => {
  const selected = incident.value?.traces.find(event => event.event_type === 'skill_selected')
  if (!selected) return null
  return { ...selected.payload, loaded: incident.value.traces.some(event => event.event_type === 'skill_loaded' && event.payload.skill_id === selected.payload.skill_id) }
})
const tools = computed(() => incident.value?.traces.filter(event => event.event_type === 'tool_called') || [])
</script>

<template>
  <div class="page-enter">
    <RouterLink class="back-link" to="/">← 返回总览</RouterLink>
    <div v-if="loading" class="loading-state">正在读取诊断轨迹…</div>
    <AgentOffline v-else-if="offline" :retry="load" />
    <div v-else-if="error" class="error-banner" role="alert">{{ error }}</div>
    <template v-else-if="incident">
      <div class="page-heading incident-heading"><div><span v-if="incident.replay_only" class="eyebrow">已记录故障事件 / {{ incident.demo_fault_id }}</span><span v-else-if="incident.demo_scenario" class="eyebrow">故障基准演示 / {{ incident.demo_fault_id }}</span><span v-else class="eyebrow">故障事件 / {{ incident.incident_id }}</span><h1>{{ scenarioLabel(incident.demo_scenario) || localizeText(incident.title) }}<span class="heading-dot">。</span></h1><p>{{ localizeText(incident.description) || '由监控异常信号触发的智能诊断任务。' }}</p></div><div class="incident-actions"><span class="live-refresh"><span class="live-dot"></span>实时监控中 · 每 3 秒刷新<br><small>最后更新时间：{{ formatTime(lastUpdatedAt) }}</small></span><span v-if="incident.replay_only" class="status-chip large recorded">只读记录 RECORDED</span><span class="status-chip large" :class="incident.status.toLowerCase()">故障状态：{{ statusLabel(incident.status) }}</span><span class="status-chip large" :class="(incident.diagnosis_status || '').toLowerCase()">诊断状态：{{ statusLabel(incident.diagnosis_status || 'PENDING') }}</span><button type="button" class="ghost-button" :disabled="refreshing" @click="load(false)">↻ {{ refreshing ? '刷新中…' : '刷新' }}</button><button type="button" class="ghost-button" :aria-pressed="replayMode" @click="replayMode = !replayMode">{{ replayMode ? '退出轨迹回放' : '▶ 轨迹回放' }}</button></div></div>
      <div class="meta-strip"><span><b>创建时间</b>{{ formatTime(incident.created_at) }}</span><span><b>严重级别</b>{{ severityLabel(incident.severity) }}</span><span><b>诊断运行态</b>{{ statusLabel(incident.runtime_status || 'PENDING') }}</span><span><b>触发信号</b>{{ incident.trigger_signal_ids.length }}</span></div>
      <TraceReplay v-if="replayMode" :incident="incident" />
      <div v-else class="detail-layout">
        <div class="detail-main">
          <section class="panel"><div class="section-head"><div><span class="eyebrow">智能体执行轨迹</span><h2>调查时间线</h2></div><span class="count-label">{{ incident.traces.length }} 个事件</span></div><IncidentTimeline :events="incident.traces" /></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">当前事实</span><h2>诊断证据</h2></div><span class="count-label">{{ incident.evidence.length }} 张证据卡</span></div><div v-if="incident.evidence.length" class="card-stack"><EvidenceCard v-for="item in incident.evidence" :key="item.evidence_id" :evidence="item" /></div><p v-else class="empty-small">当前故障事件尚无证据。</p></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">证据驱动推理</span><h2>故障假设</h2></div><span class="count-label">跟踪 {{ incident.hypotheses.length }} 项</span></div><div v-if="incident.hypotheses.length" class="card-stack"><HypothesisCard v-for="item in incident.hypotheses" :key="item.hypothesis_id" :hypothesis="item" /></div><p v-else class="empty-small">尚无已记录的故障假设。</p></section>
        </div>
        <div class="detail-aside">
          <section class="panel"><div class="section-head"><div><span class="eyebrow">领域排障指导</span><h2>诊断技能</h2></div></div><SkillCard :skill="skill" /></section>
          <section class="panel"><div class="section-head"><div><span class="eyebrow">只读 MCP</span><h2>工具调用序列</h2></div></div><ol v-if="tools.length" class="tool-list"><li v-for="(event, index) in tools" :key="event.event_id"><span>{{ String(index + 1).padStart(2, '0') }}</span><strong>{{ toolLabel(event.payload.tool_name) }}</strong><small>{{ formatTime(event.created_at) }}</small></li></ol><p v-else class="empty-small">尚未调用诊断工具。</p></section>
          <section class="panel report-panel"><div class="section-head"><div><span class="eyebrow">智能体输出</span><h2>诊断报告</h2></div></div><template v-if="incident.report"><span class="status-chip" :class="incident.report.status">{{ statusLabel(incident.report.status) }}</span><h3>{{ localizeText(incident.report.root_cause) || '根因尚未确认' }}</h3><p>{{ localizeText(incident.report.conclusion) }}</p><div class="report-confidence">置信度 <strong>{{ percent(incident.report.confidence) }}</strong></div></template><p v-else class="empty-small">诊断报告尚未生成。</p></section>
          <section class="panel governance-panel"><div class="section-head"><div><span class="eyebrow">权限治理</span><h2>操作建议</h2></div></div><p>{{ incident.proposals.length ? `${incident.proposals.length} 条只读治理建议待查看。` : '本次故障事件尚无操作建议。' }}</p><RouterLink v-if="incident.proposals.length" class="text-link" :to="`/incidents/${incident.incident_id}/proposal`">查看治理结果 ↗</RouterLink><small>此控制台不提供操作执行入口。</small></section>
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
.live-refresh { text-align: right; color: var(--muted); font-size: 12px; line-height: 1.55; }
.live-refresh .live-dot { display: inline-block; margin-right: 7px; }
.status-chip.active {
  color: #f2b37d;
  background: #4a312a;
}
.status-chip.recovered {
  color: #75ddba;
  background: #203d38;
}
.status-chip.recorded {
  color: #b8c8ff;
  background: #27304d;
}
</style>
