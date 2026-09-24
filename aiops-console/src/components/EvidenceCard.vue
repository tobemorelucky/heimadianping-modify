<script setup>
import { factLabel, formatTime, kindLabel, localizeText, shortId, statusLabel, toolLabel } from '../format.js'
defineProps({ evidence: { type: Object, required: true } })
</script>

<template>
  <article class="data-card evidence-card">
    <div class="card-top"><span class="card-kind">{{ kindLabel(evidence.kind) }}</span><span class="status-chip" :class="evidence.status">{{ statusLabel(evidence.status) }}</span></div>
    <p class="card-summary">{{ localizeText(evidence.summary) }}</p>
    <ul v-if="evidence.facts?.length" class="fact-list"><li v-for="fact in evidence.facts" :key="fact">{{ factLabel(fact) }}</li></ul>
    <div class="card-foot"><span :title="evidence.evidence_id">{{ shortId(evidence.evidence_id) }}</span><span>{{ toolLabel(evidence.source_tool) }}</span><time>{{ formatTime(evidence.collected_at) }}</time></div>
  </article>
</template>
