<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { getMonitoringSummary, listIncidents } from '../api/client.js'
import AgentOffline from '../components/AgentOffline.vue'
import { formatTime, scenarioLabel, shortId, sourceLabel, statusLabel, toolLabel } from '../format.js'

const summary = ref(null)
const incidents = ref([])
const loading = ref(true)
const refreshing = ref(false)
const offline = ref(false)
const lastUpdatedAt = ref(null)
const hasActiveIncident = computed(() => Number(summary.value?.active_incident_count || 0) > 0)
let refreshTimer = null
let refreshPromise = null

function refresh(showLoading = false) {
  if (refreshPromise) return refreshPromise
  if (showLoading) loading.value = true
  refreshing.value = true
  refreshPromise = (async () => {
    try {
      const [nextSummary, nextIncidents] = await Promise.all([getMonitoringSummary(), listIncidents()])
      summary.value = nextSummary
      incidents.value = nextIncidents
      offline.value = false
      lastUpdatedAt.value = new Date().toISOString()
    } catch {
      if (!summary.value) incidents.value = []
      offline.value = true
    } finally {
      loading.value = false
      refreshing.value = false
      refreshPromise = null
    }
  })()
  return refreshPromise
}

onMounted(() => {
  refresh(true)
  refreshTimer = setInterval(() => refresh(false), 5000)
})
onBeforeUnmount(() => {
  if (refreshTimer !== null) clearInterval(refreshTimer)
  refreshTimer = null
})
</script>

<template>
  <div class="page-enter">
    <div class="page-heading"><div><span class="eyebrow">运行中心 / 01</span><h1>智能诊断总览<span class="heading-dot">。</span></h1><p>巡检信号形成故障事件，诊断智能体通过技能、工具与证据完成研判。</p></div><div class="heading-actions"><span v-if="!offline" class="live-refresh"><span class="live-dot"></span>实时监控中 · 每 5 秒刷新<br><small>最后更新时间：{{ formatTime(lastUpdatedAt) }}</small></span><button class="ghost-button" type="button" :disabled="refreshing" @click="refresh(false)">↻ {{ refreshing ? '刷新中…' : '手动刷新' }}</button></div></div>
    <div v-if="loading" class="loading-state">正在读取智能诊断状态…</div>
    <AgentOffline v-else-if="offline" :retry="refresh" />
    <template v-else-if="summary">
      <section class="stat-grid" aria-label="巡检摘要">
        <div class="stat-card stat-health"><span class="eyebrow">系统健康</span><div class="stat-value"><span class="large-indicator" :class="hasActiveIncident ? 'attention' : 'healthy'"></span>{{ hasActiveIncident ? '需关注' : '健康' }}</div><p>{{ hasActiveIncident ? '存在未恢复故障事件' : '当前无活跃故障事件' }}</p></div>
        <div class="stat-card"><span class="eyebrow">最近巡检</span><div class="stat-value stat-time">{{ formatTime(summary.last_inspection_at) }}</div><p><span v-if="summary.inspection_mode === 'fixture'">故障基准回放 · </span>{{ summary.last_inspection_tool ? toolLabel(summary.last_inspection_tool) : '尚无持续巡检记录' }}<span v-if="summary.last_inspection_status"> · {{ statusLabel(summary.last_inspection_status) }}</span></p></div>
        <div class="stat-card"><span class="eyebrow">活跃故障事件</span><div class="stat-value stat-number">{{ summary.active_incident_count }}</div><p>等待智能诊断或人工处理</p></div>
      </section>
      <section class="panel incident-panel"><div class="section-head"><div><span class="eyebrow">最近活动</span><h2>最近故障事件</h2></div><span class="count-label">共 {{ incidents.length }} 条</span></div>
        <div v-if="!incidents.length" class="empty-state"><div class="empty-symbol">✓</div><h3>系统健康</h3><p>当前没有故障事件。巡检发现异常时，会在这里显示诊断任务。</p></div>
        <div v-else class="incident-list"><RouterLink v-for="incident in incidents" :key="incident.incident_id" class="incident-row" :to="`/incidents/${incident.incident_id}`"><span class="incident-status-dot" :class="incident.status.toLowerCase()"></span><span class="incident-main"><strong>{{ scenarioLabel(incident.demo_scenario) || incident.title }}</strong><small>{{ shortId(incident.incident_id) }} · {{ incident.replay_only ? '历史记录回放' : sourceLabel(incident.source) }}</small></span><span class="incident-side"><span v-if="incident.replay_only" class="status-chip recorded">已记录 RECORDED</span><span class="status-chip" :class="incident.status.toLowerCase()">{{ statusLabel(incident.status) }}</span><time>{{ formatTime(incident.created_at) }}</time></span><span class="row-arrow">↗</span></RouterLink></div>
      </section>
      <div class="flow-strip"><span>持续巡检</span><i>→</i><span>异常检测</span><i>→</i><span>智能诊断</span><i>→</i><span>权限治理</span></div>
    </template>
  </div>
</template>

<style scoped>
.heading-actions { display: flex; align-items: center; gap: 18px; }
.live-refresh { text-align: right; color: var(--muted); font-size: 12px; line-height: 1.6; }
.live-refresh .live-dot { display: inline-block; margin-right: 7px; }
.live-refresh small { color: var(--text-faint, #748094); }
</style>
