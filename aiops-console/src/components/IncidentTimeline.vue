<script setup>
import { decisionLabel, formatTime, localizeText, skillLabel, statusLabel, toolLabel } from '../format.js'

defineProps({ events: { type: Array, default: () => [] } })

const labels = {
  anomaly_detected: '检测到异常', incident_created: '创建故障事件',
  diagnosis_started: '开始诊断', skill_selected: '选择诊断技能',
  skill_loaded: '加载诊断技能', plan_created: '生成调查计划',
  tool_called: '调用工具', tool_completed: '工具返回',
  tool_failed: '工具失败', evidence_created: '生成证据',
  hypothesis_created: '创建假设', hypothesis_updated: '更新假设',
  diagnosis_completed: '完成诊断', report_generated: '生成报告',
  action_proposal_created: '生成操作建议', permission_checked: '权限检查',
  reflection_completed: '反思决策', context_built: '构建上下文'
}

function detail(event) {
  const p = event.payload || {}
  if (event.event_type === 'tool_called' || event.event_type === 'tool_completed') return toolLabel(p.tool_name)
  if (event.event_type === 'skill_selected' || event.event_type === 'skill_loaded') return skillLabel(p.skill_id)
  if (event.event_type.startsWith('hypothesis_')) {
    return [statusLabel(p.status), p.confidence == null ? null : `${Math.round(p.confidence * 100)}%`].filter(Boolean).join(' · ')
  }
  if (event.event_type === 'permission_checked') return statusLabel(p.decision)
  if (event.event_type === 'reflection_completed') return decisionLabel(p.decision)
  return null
}
</script>

<template>
  <div v-if="events.length" class="timeline">
    <article v-for="event in events" :key="event.event_id" class="timeline-item" :class="`type-${event.event_type}`">
      <div class="timeline-rail"><span class="timeline-node"></span></div>
      <div class="timeline-body">
        <div class="timeline-meta"><span class="eyebrow">{{ labels[event.event_type] || '诊断事件' }} · {{ event.event_type }}</span><time>{{ formatTime(event.created_at) }}</time></div>
        <p>{{ localizeText(event.summary) }}</p>
        <div v-if="detail(event)" class="timeline-detail">{{ detail(event) }}</div>
        <small v-if="event.raw_event_type !== event.event_type" class="muted">原始类型：{{ event.raw_event_type }}</small>
      </div>
    </article>
  </div>
  <div v-else class="empty-small">尚无诊断轨迹。</div>
</template>
