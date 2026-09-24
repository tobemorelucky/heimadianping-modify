<script setup>
import { onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { getIncident, isAgentOffline } from '../api/client.js'
import ActionProposal from '../components/ActionProposal.vue'
import AgentOffline from '../components/AgentOffline.vue'

const props = defineProps({ id: { type: String, required: true } })
const incident = ref(null)
const loading = ref(true)
const error = ref('')
const offline = ref(false)

async function load() {
  loading.value = true
  error.value = ''
  offline.value = false
  incident.value = null
  try { incident.value = await getIncident(props.id) }
  catch (failure) {
    if (isAgentOffline(failure)) offline.value = true
    else error.value = '此 Incident 不存在或不可访问。'
  }
  finally { loading.value = false }
}
onMounted(load)
watch(() => props.id, load)
</script>

<template>
  <div class="page-enter">
    <RouterLink class="back-link" :to="`/incidents/${id}`">← 返回故障事件</RouterLink>
    <div class="page-heading"><div><span class="eyebrow">权限治理 / 操作建议</span><h1>人工决策<span class="heading-dot">。</span></h1><p>智能体只提出建议，权限网关给出判定；执行始终不在此控制台内。</p></div></div>
    <div v-if="loading" class="loading-state">正在读取操作建议…</div>
    <AgentOffline v-else-if="offline" :retry="load" />
    <div v-else-if="error" class="error-banner" role="alert">{{ error }}</div>
    <template v-else-if="incident"><div v-if="incident.proposals.length" class="proposal-list"><ActionProposal v-for="proposal in incident.proposals" :key="proposal.proposal_id" :proposal="proposal" /></div><div v-else class="panel empty-state"><div class="empty-symbol">—</div><h3>暂无操作建议</h3><p>此故障事件没有已记录的操作建议。</p></div></template>
  </div>
</template>
