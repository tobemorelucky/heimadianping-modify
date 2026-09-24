<script setup>
defineProps({ retry: { type: Function, required: true } })
</script>

<template>
  <section class="panel offline-panel" role="alert">
    <div class="offline-icon">!</div>
    <span class="eyebrow">连接状态</span>
    <h2>诊断服务离线</h2>
    <p>无法连接本地 AIOps 智能诊断 API。请确认 Python 服务运行在 127.0.0.1:8010，然后重试。</p>
    <button class="ghost-button" type="button" @click="retry">重新连接</button>
  </section>
</template>
