<script setup>
import { formatTime, shortId } from '../format.js'
defineProps({ evidence: { type: Object, required: true } })
</script>

<template>
  <article class="data-card evidence-card">
    <div class="card-top"><span class="card-kind">{{ evidence.kind }}</span><span class="status-chip" :class="evidence.status">{{ evidence.status }}</span></div>
    <p class="card-summary">{{ evidence.summary }}</p>
    <ul v-if="evidence.facts?.length" class="fact-list"><li v-for="fact in evidence.facts" :key="fact">{{ fact }}</li></ul>
    <div class="card-foot"><span :title="evidence.evidence_id">{{ shortId(evidence.evidence_id) }}</span><span>{{ evidence.source_tool }}</span><time>{{ formatTime(evidence.collected_at) }}</time></div>
  </article>
</template>
