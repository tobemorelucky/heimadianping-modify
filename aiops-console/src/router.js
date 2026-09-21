import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from './views/Dashboard.vue'
import IncidentDetail from './views/IncidentDetail.vue'
import Proposal from './views/Proposal.vue'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'dashboard', component: Dashboard },
    { path: '/incidents/:id', name: 'incident', component: IncidentDetail, props: true },
    { path: '/incidents/:id/proposal', name: 'proposal', component: Proposal, props: true },
    { path: '/:pathMatch(.*)*', redirect: '/' }
  ]
})
