<script setup>
import { skillLabel } from '../format.js'
defineProps({ skill: { type: Object, default: null } })
</script>

<template>
  <div v-if="skill" class="skill-card">
    <div class="skill-icon">✦</div>
    <div><span class="eyebrow">已选择诊断技能</span><h3>{{ skillLabel(skill.skill_id) }}</h3><p>版本 {{ skill.version }} · {{ skill.loaded ? '已加载并用于规划指导' : '已选择' }}</p></div>
  </div>
  <div v-else class="empty-small">本次诊断未选择领域技能。</div>
</template>
