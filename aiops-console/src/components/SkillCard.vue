<script setup>
defineProps({ skill: { type: Object, default: null } })
</script>

<template>
  <div v-if="skill" class="skill-card">
    <div class="skill-icon">✦</div>
    <div><span class="eyebrow">SELECTED SKILL</span><h3>{{ skill.skill_id }}</h3><p>Version {{ skill.version }} · {{ skill.loaded ? 'loaded for planner guidance' : 'selected' }}</p></div>
  </div>
  <div v-else class="empty-small">本次诊断未选择领域 Skill。</div>
</template>
