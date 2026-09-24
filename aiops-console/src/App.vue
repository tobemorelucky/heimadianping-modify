<script setup>
import { RouterLink, RouterView } from 'vue-router'
</script>

<template>
  <div class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/" aria-label="AIOps 控制台首页">
        <span class="brand-mark"><span></span><span></span><span></span></span>
        <span><strong>AIOps</strong><small>智能诊断控制台</small></span>
      </RouterLink>
      <div class="sidebar-label">工作区</div>
      <nav aria-label="主导航">
        <RouterLink class="nav-link" to="/"><span class="nav-icon">◫</span> 总览</RouterLink>
      </nav>
      <div class="sidebar-bottom">
        <span class="live-dot"></span>
        <span>本地 · 只读模式</span>
      </div>
    </aside>
    <div class="main-shell">
      <header class="topbar">
        <div class="breadcrumb"><span>HMDP</span><span class="slash">/</span><span>智能诊断框架</span></div>
        <span class="environment-pill"><span class="live-dot"></span> 本地演示环境</span>
      </header>
      <main class="page-content"><RouterView /></main>
      <footer class="footer">AIOps 智能诊断框架 <span>·</span> 仅诊断 <span>·</span> 操作需要人工审批</footer>
    </div>
  </div>
</template>
