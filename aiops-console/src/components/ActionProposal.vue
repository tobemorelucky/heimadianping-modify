<script setup>
import { actionLabel, localizeText, shortId, statusLabel } from '../format.js'
defineProps({ proposal: { type: Object, required: true } })
</script>

<template>
  <article class="data-card proposal-card">
    <div class="card-top"><span class="eyebrow">操作建议 · 仅展示</span><span class="status-chip" :class="proposal.permission_result?.toLowerCase()">{{ statusLabel(proposal.permission_result) }}</span></div>
    <h2>{{ actionLabel(proposal.action_name) }}</h2>
    <p class="card-summary">{{ localizeText(proposal.reason) }}</p>
    <div class="proposal-grid">
      <div><span>风险等级</span><strong>{{ proposal.risk_level }}</strong></div>
      <div><span>是否需要审批</span><strong>{{ proposal.approval_required ? '是 · 人工复核' : '否' }}</strong></div>
    </div>
    <div class="proposal-evidence"><span class="eyebrow">关联证据</span><div><code v-for="ref in proposal.evidence_refs" :key="ref" :title="ref">{{ shortId(ref) }}</code></div></div>
    <div class="read-only-note">此控制台只展示治理结果，不提供审批或执行入口。</div>
  </article>
</template>
