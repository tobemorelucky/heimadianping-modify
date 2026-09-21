<script setup>
import { shortId } from '../format.js'
defineProps({ proposal: { type: Object, required: true } })
</script>

<template>
  <article class="data-card proposal-card">
    <div class="card-top"><span class="eyebrow">ACTION PROPOSAL · DISPLAY ONLY</span><span class="status-chip" :class="proposal.permission_result?.toLowerCase()">{{ proposal.permission_result }}</span></div>
    <h2>{{ proposal.action_name }}</h2>
    <p class="card-summary">{{ proposal.reason }}</p>
    <div class="proposal-grid">
      <div><span>Risk level</span><strong>{{ proposal.risk_level }}</strong></div>
      <div><span>Approval required</span><strong>{{ proposal.approval_required ? 'Yes · human review' : 'No' }}</strong></div>
    </div>
    <div class="proposal-evidence"><span class="eyebrow">LINKED EVIDENCE</span><div><code v-for="ref in proposal.evidence_refs" :key="ref" :title="ref">{{ shortId(ref) }}</code></div></div>
    <div class="read-only-note">此控制台只展示治理结果，不提供审批或执行入口。</div>
  </article>
</template>
