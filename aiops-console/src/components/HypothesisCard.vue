<script setup>
import { percent, shortId } from '../format.js'
defineProps({ hypothesis: { type: Object, required: true } })
</script>

<template>
  <article class="data-card hypothesis-card">
    <div class="card-top"><span class="card-kind">{{ shortId(hypothesis.hypothesis_id) }}</span><span class="status-chip" :class="hypothesis.status?.toLowerCase()">{{ hypothesis.status }}</span></div>
    <h3>{{ hypothesis.description }}</h3>
    <div class="confidence-row"><span>Confidence</span><strong>{{ percent(hypothesis.confidence) }}</strong></div>
    <div class="confidence-track"><span :style="{ width: percent(hypothesis.confidence) }"></span></div>
    <div v-if="hypothesis.history?.length" class="history-row" aria-label="置信度变化">
      <span v-for="(step, index) in hypothesis.history" :key="index">{{ step.status }} {{ percent(step.confidence) }}<b v-if="index < hypothesis.history.length - 1">→</b></span>
    </div>
    <div class="reference-row"><span>支持证据 {{ hypothesis.supporting_evidence_refs?.length || 0 }}</span><span>反对证据 {{ hypothesis.contradicting_evidence_refs?.length || 0 }}</span></div>
  </article>
</template>
