<script setup>
import { computed, onBeforeUnmount, ref } from 'vue'
import { decisionLabel, formatTime, localizeText, percent, statusLabel, toolLabel } from '../format.js'
import EvidenceCard from './EvidenceCard.vue'
import IncidentTimeline from './IncidentTimeline.vue'
import SkillCard from './SkillCard.vue'

const props = defineProps({ incident: { type: Object, required: true } })
const position = ref(0)
const playing = ref(false)
let timer = null

const events = computed(() => props.incident.traces || [])
const visibleEvents = computed(() => events.value.slice(0, position.value))
const currentEvent = computed(() => visibleEvents.value.at(-1) || null)
const progress = computed(() => events.value.length ? Math.round(position.value / events.value.length * 100) : 0)
const selectedSkill = computed(() => {
  const selected = visibleEvents.value.find(event => event.event_type === 'skill_selected')
  if (!selected) return null
  return {
    ...selected.payload,
    loaded: visibleEvents.value.some(event => event.event_type === 'skill_loaded' && event.payload.skill_id === selected.payload.skill_id)
  }
})
const toolCalls = computed(() => visibleEvents.value.filter(event => event.event_type === 'tool_called'))
const visibleEvidenceIds = computed(() => new Set(visibleEvents.value.filter(event => event.event_type === 'evidence_created').map(event => event.payload?.evidence_id).filter(Boolean)))
const evidenceCards = computed(() => {
  return (props.incident.evidence || []).filter(item => visibleEvidenceIds.value.has(item.evidence_id))
})
const missingEvidenceIds = computed(() => [...visibleEvidenceIds.value].filter(id => !evidenceCards.value.some(item => item.evidence_id === id)))
const hypothesisStates = computed(() => {
  const states = new Map()
  for (const event of visibleEvents.value) {
    if (!['hypothesis_created', 'hypothesis_updated'].includes(event.event_type)) continue
    const payload = event.payload || {}
    if (!payload.hypothesis_id) continue
    const previous = states.get(payload.hypothesis_id)
    states.set(payload.hypothesis_id, {
      hypothesis_id: payload.hypothesis_id,
      description: payload.hypothesis || previous?.description || event.summary,
      status: payload.status || previous?.status || 'UNKNOWN',
      confidence: payload.confidence ?? previous?.confidence ?? 0
    })
  }
  return [...states.values()]
})
const lastReflection = computed(() => [...visibleEvents.value].reverse().find(event => event.event_type === 'reflection_completed'))
const reportVisible = computed(() => visibleEvents.value.some(event => event.event_type === 'report_generated'))

function stop() {
  if (timer !== null) clearInterval(timer)
  timer = null
  playing.value = false
}

function setPosition(value) {
  position.value = Math.max(0, Math.min(events.value.length, value))
  if (position.value >= events.value.length) stop()
}

function togglePlay() {
  if (playing.value) {
    stop()
    return
  }
  if (!events.value.length) return
  if (position.value >= events.value.length) position.value = 0
  playing.value = true
  timer = setInterval(() => setPosition(position.value + 1), 700)
}

function step(delta) {
  stop()
  setPosition(position.value + delta)
}

onBeforeUnmount(stop)
</script>

<template>
  <section class="panel replay-panel" aria-label="诊断轨迹回放">
    <div class="section-head"><div><span class="eyebrow">只读诊断轨迹回放</span><h2>智能体运行回放</h2></div><span class="count-label">{{ position }} / {{ events.length }} 个事件</span></div>
    <div class="replay-controls">
      <button type="button" class="ghost-button" aria-label="回放到开头" :disabled="position === 0" @click="step(-position)">↺ 开头</button>
      <button type="button" class="ghost-button" aria-label="上一步" :disabled="position === 0" @click="step(-1)">← 上一步</button>
      <button type="button" class="ghost-button replay-primary" :disabled="!events.length" @click="togglePlay">{{ playing ? 'Ⅱ 暂停' : '▶ 播放' }}</button>
      <button type="button" class="ghost-button" aria-label="下一步" :disabled="position === events.length" @click="step(1)">下一步 →</button>
      <span class="replay-progress-label">{{ progress }}%</span>
    </div>
    <div class="replay-progress" role="progressbar" :aria-valuenow="position" :aria-valuemax="events.length" aria-valuemin="0"><span :style="{ width: `${progress}%` }"></span></div>
    <p class="replay-note">按已保存的故障事件轨迹顺序回放；时间戳保留原始来源。回放不会重新调用模型或 MCP 工具。</p>
    <div v-if="currentEvent" class="replay-current" data-testid="replay-current"><span class="eyebrow">当前事件 · {{ String(position).padStart(2, '0') }}</span><h3>{{ currentEvent.event_type }}</h3><p>{{ localizeText(currentEvent.summary) }}</p><time>{{ formatTime(currentEvent.created_at) }}</time></div>
    <div v-else class="empty-small replay-start">点击“播放”或“下一步”，从故障事件的首个轨迹开始。</div>
    <div class="replay-grid">
      <div class="panel replay-subpanel"><div class="section-head"><div><span class="eyebrow">当前回放序列</span><h2>时间线</h2></div></div><div class="replay-timeline"><IncidentTimeline :events="visibleEvents" /></div></div>
      <div class="replay-facts">
        <div class="panel replay-subpanel"><span class="eyebrow">诊断技能</span><SkillCard :skill="selectedSkill" /></div>
        <div class="panel replay-subpanel"><span class="eyebrow">工具调用</span><ol v-if="toolCalls.length" class="tool-list"><li v-for="(event, index) in toolCalls" :key="event.event_id"><span>{{ String(index + 1).padStart(2, '0') }}</span><strong>{{ toolLabel(event.payload.tool_name) }}</strong></li></ol><p v-else class="empty-small">此时尚未调用工具。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">诊断证据</span><div v-if="evidenceCards.length" class="card-stack"><EvidenceCard v-for="item in evidenceCards" :key="item.evidence_id" :evidence="item" /></div><p v-if="missingEvidenceIds.length" class="empty-small">记录不完整：{{ missingEvidenceIds.length }} 条证据引用缺少卡片。</p><p v-else-if="!evidenceCards.length" class="empty-small">此时尚无证据。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">假设更新</span><div v-if="hypothesisStates.length" class="replay-hypotheses"><div v-for="item in hypothesisStates" :key="item.hypothesis_id"><strong>{{ item.description }}</strong><span>{{ statusLabel(item.status) }} · {{ percent(item.confidence) }}</span></div></div><p v-else class="empty-small">此时尚无故障假设。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">反思决策</span><p v-if="lastReflection" class="replay-text">{{ localizeText(lastReflection.summary) }} <span v-if="lastReflection.payload?.decision">（{{ decisionLabel(lastReflection.payload.decision) }}）</span></p><p v-else class="empty-small">此时尚未进行反思决策。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">诊断报告</span><template v-if="reportVisible && incident.report"><h3>{{ localizeText(incident.report.root_cause) || '根因未确认' }}</h3><p class="replay-text">{{ localizeText(incident.report.conclusion) }}</p></template><p v-else class="empty-small">诊断报告尚未生成。</p></div>
      </div>
    </div>
  </section>
</template>
