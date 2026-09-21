<script setup>
import { computed, onBeforeUnmount, ref } from 'vue'
import { formatTime, percent } from '../format.js'
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
  <section class="panel replay-panel" aria-label="Trace Replay">
    <div class="section-head"><div><span class="eyebrow">READ-ONLY TRACE REPLAY</span><h2>Agent Run Replay</h2></div><span class="count-label">{{ position }} / {{ events.length }} EVENTS</span></div>
    <div class="replay-controls">
      <button type="button" class="ghost-button" aria-label="回放到开头" :disabled="position === 0" @click="step(-position)">↺ 开头</button>
      <button type="button" class="ghost-button" aria-label="上一步" :disabled="position === 0" @click="step(-1)">← 上一步</button>
      <button type="button" class="ghost-button replay-primary" :disabled="!events.length" @click="togglePlay">{{ playing ? 'Ⅱ 暂停' : '▶ 播放' }}</button>
      <button type="button" class="ghost-button" aria-label="下一步" :disabled="position === events.length" @click="step(1)">下一步 →</button>
      <span class="replay-progress-label">{{ progress }}%</span>
    </div>
    <div class="replay-progress" role="progressbar" :aria-valuenow="position" :aria-valuemax="events.length" aria-valuemin="0"><span :style="{ width: `${progress}%` }"></span></div>
    <p class="replay-note">按已保存的 Incident Trace 顺序回放；时间戳保留原始来源。回放不重新调用模型或 MCP Tool。</p>
    <div v-if="currentEvent" class="replay-current" data-testid="replay-current"><span class="eyebrow">CURRENT EVENT · {{ String(position).padStart(2, '0') }}</span><h3>{{ currentEvent.event_type.replaceAll('_', ' ') }}</h3><p>{{ currentEvent.summary }}</p><time>{{ formatTime(currentEvent.created_at) }}</time></div>
    <div v-else class="empty-small replay-start">点击“播放”或“下一步”，从 Incident 的首个事件开始。</div>
    <div class="replay-grid">
      <div class="panel replay-subpanel"><div class="section-head"><div><span class="eyebrow">SEQUENCE SO FAR</span><h2>Timeline</h2></div></div><div class="replay-timeline"><IncidentTimeline :events="visibleEvents" /></div></div>
      <div class="replay-facts">
        <div class="panel replay-subpanel"><span class="eyebrow">SKILL</span><SkillCard :skill="selectedSkill" /></div>
        <div class="panel replay-subpanel"><span class="eyebrow">TOOL CALLS</span><ol v-if="toolCalls.length" class="tool-list"><li v-for="(event, index) in toolCalls" :key="event.event_id"><span>{{ String(index + 1).padStart(2, '0') }}</span><strong>{{ event.payload.tool_name }}</strong></li></ol><p v-else class="empty-small">此时尚未调用 Tool。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">EVIDENCE</span><div v-if="evidenceCards.length" class="card-stack"><EvidenceCard v-for="item in evidenceCards" :key="item.evidence_id" :evidence="item" /></div><p v-if="missingEvidenceIds.length" class="empty-small">记录不完整：{{ missingEvidenceIds.length }} 条 Evidence 引用缺少卡片。</p><p v-else-if="!evidenceCards.length" class="empty-small">此时尚无 Evidence。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">HYPOTHESIS UPDATE</span><div v-if="hypothesisStates.length" class="replay-hypotheses"><div v-for="item in hypothesisStates" :key="item.hypothesis_id"><strong>{{ item.description }}</strong><span>{{ item.status }} · {{ percent(item.confidence) }}</span></div></div><p v-else class="empty-small">此时尚无 Hypothesis。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">REFLECTION</span><p v-if="lastReflection" class="replay-text">{{ lastReflection.summary }} <span v-if="lastReflection.payload?.decision">({{ lastReflection.payload.decision }})</span></p><p v-else class="empty-small">此时尚未进行 Reflection。</p></div>
        <div class="panel replay-subpanel"><span class="eyebrow">REPORT</span><template v-if="reportVisible && incident.report"><h3>{{ incident.report.root_cause || '根因未确认' }}</h3><p class="replay-text">{{ incident.report.conclusion }}</p></template><p v-else class="empty-small">Report 尚未生成。</p></div>
      </div>
    </div>
  </section>
</template>
