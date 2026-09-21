<script setup>
import { RouterLink, RouterView } from 'vue-router'
</script>

<template>
  <div class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/" aria-label="AIOps Console 首页">
        <span class="brand-mark"><span></span><span></span><span></span></span>
        <span><strong>AIOps</strong><small>AGENT CONSOLE</small></span>
      </RouterLink>
      <div class="sidebar-label">WORKSPACE</div>
      <nav aria-label="主导航">
        <RouterLink class="nav-link" to="/"><span class="nav-icon">◫</span> Overview</RouterLink>
      </nav>
      <div class="sidebar-bottom">
        <span class="live-dot"></span>
        <span>LOCAL · READ ONLY</span>
      </div>
    </aside>
    <div class="main-shell">
      <header class="topbar">
        <div class="breadcrumb"><span>HMDP</span><span class="slash">/</span><span>Agent Harness</span></div>
        <span class="environment-pill"><span class="live-dot"></span> LOCAL ENVIRONMENT</span>
      </header>
      <main class="page-content"><RouterView /></main>
      <footer class="footer">AIOPS AGENT HARNESS <span>·</span> DIAGNOSIS ONLY <span>·</span> HUMAN APPROVAL REQUIRED</footer>
    </div>
  </div>
</template>
