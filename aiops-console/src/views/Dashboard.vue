<script setup>
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { getMonitoringSummary, listIncidents } from '../api/client.js'
import AgentOffline from '../components/AgentOffline.vue'
import { formatTime, shortId } from '../format.js'

const summary = ref(null)
const incidents = ref([])
const loading = ref(true)
const offline = ref(false)

async function refresh() {
  loading.value = true
  offline.value = false
  try {
    const [nextSummary, nextIncidents] = await Promise.all([getMonitoringSummary(), listIncidents()])
    summary.value = nextSummary
    incidents.value = nextIncidents
  } catch {
    summary.value = null
    incidents.value = []
    offline.value = true
  } finally {
    loading.value = false
  }
}
onMounted(refresh)
</script>

<template>
  <div class="page-enter">
    <div class="page-heading"><div><span class="eyebrow">COMMAND CENTER / 01</span><h1>Agent overview<span class="heading-dot">.</span></h1><p>巡检信号进入 Incident，Agent 再用 Skill、Tool 与 Evidence 完成诊断。</p></div><button class="ghost-button" type="button" @click="refresh">↻ 刷新视图</button></div>
    <div v-if="loading" class="loading-state">正在读取 Agent 状态…</div>
    <AgentOffline v-else-if="offline" :retry="refresh" />
    <template v-else-if="summary">
      <section class="stat-grid" aria-label="巡检摘要">
        <div class="stat-card stat-health"><span class="eyebrow">SYSTEM HEALTH</span><div class="stat-value"><span class="large-indicator" :class="summary.health"></span>{{ summary.health === 'healthy' ? 'Healthy' : 'Attention' }}</div><p>{{ summary.health === 'healthy' ? '当前无活跃 Incident' : '存在待处理 Incident' }}</p></div>
        <div class="stat-card"><span class="eyebrow">LAST INSPECTION</span><div class="stat-value stat-time">{{ formatTime(summary.last_inspection_at) }}</div><p><span v-if="summary.inspection_mode === 'fixture'">FaultBench fixture replay · </span>{{ summary.last_inspection_tool || '尚无持续巡检记录' }}<span v-if="summary.last_inspection_status"> · {{ summary.last_inspection_status }}</span></p></div>
        <div class="stat-card"><span class="eyebrow">ACTIVE INCIDENTS</span><div class="stat-value stat-number">{{ summary.active_incident_count }}</div><p>等待 Agent 或人工处理</p></div>
      </section>
      <section class="panel incident-panel"><div class="section-head"><div><span class="eyebrow">RECENT ACTIVITY</span><h2>Recent incidents</h2></div><span class="count-label">{{ incidents.length }} TOTAL</span></div>
        <div v-if="!incidents.length" class="empty-state"><div class="empty-symbol">✓</div><h3>Healthy</h3><p>当前没有 Incident。巡检记录出现异常时，会在这里显示诊断任务。</p></div>
        <div v-else class="incident-list"><RouterLink v-for="incident in incidents" :key="incident.incident_id" class="incident-row" :to="`/incidents/${incident.incident_id}`"><span class="incident-status-dot" :class="incident.status.toLowerCase()"></span><span class="incident-main"><strong>{{ incident.demo_scenario || incident.title }}</strong><small>{{ shortId(incident.incident_id) }} · {{ incident.source }}</small></span><span class="incident-side"><span class="status-chip" :class="incident.status.toLowerCase()">{{ incident.status }}</span><time>{{ formatTime(incident.created_at) }}</time></span><span class="row-arrow">↗</span></RouterLink></div>
      </section>
      <div class="flow-strip"><span>OBSERVATION</span><i>→</i><span>INCIDENT</span><i>→</i><span>AGENT DIAGNOSIS</span><i>→</i><span>HUMAN REVIEW</span></div>
    </template>
  </div>
</template>
