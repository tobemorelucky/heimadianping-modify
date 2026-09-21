<script setup>
import { formatTime } from '../format.js'

defineProps({ events: { type: Array, default: () => [] } })

const labels = {
  anomaly_detected: 'ANOMALY', incident_created: 'INCIDENT',
  diagnosis_started: 'DIAGNOSIS', skill_selected: 'SKILL SELECTED',
  skill_loaded: 'SKILL LOADED', plan_created: 'PLAN',
  tool_called: 'TOOL CALLED', tool_completed: 'TOOL RETURNED',
  tool_failed: 'TOOL FAILED', evidence_created: 'EVIDENCE',
  hypothesis_created: 'HYPOTHESIS', hypothesis_updated: 'HYPOTHESIS',
  diagnosis_completed: 'DIAGNOSIS', report_generated: 'REPORT',
  action_proposal_created: 'PROPOSAL', permission_checked: 'PERMISSION'
}

function detail(event) {
  const p = event.payload || {}
  if (event.event_type === 'tool_called' || event.event_type === 'tool_completed') return p.tool_name
  if (event.event_type === 'skill_selected' || event.event_type === 'skill_loaded') return p.skill_id
  if (event.event_type.startsWith('hypothesis_')) {
    return [p.status, p.confidence == null ? null : `${Math.round(p.confidence * 100)}%`].filter(Boolean).join(' · ')
  }
  if (event.event_type === 'permission_checked') return p.decision
  return null
}
</script>

<template>
  <div v-if="events.length" class="timeline">
    <article v-for="event in events" :key="event.event_id" class="timeline-item" :class="`type-${event.event_type}`">
      <div class="timeline-rail"><span class="timeline-node"></span></div>
      <div class="timeline-body">
        <div class="timeline-meta"><span class="eyebrow">{{ labels[event.event_type] || event.event_type.replaceAll('_', ' ').toUpperCase() }}</span><time>{{ formatTime(event.created_at) }}</time></div>
        <p>{{ event.summary }}</p>
        <div v-if="detail(event)" class="timeline-detail">{{ detail(event) }}</div>
        <small v-if="event.raw_event_type !== event.event_type" class="muted">source: {{ event.raw_event_type }}</small>
      </div>
    </article>
  </div>
  <div v-else class="empty-small">尚无诊断轨迹。</div>
</template>
