import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'
import JobsView from '../views/JobsView.vue'
import JobDetailView from '../views/JobDetailView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'dashboard', component: DashboardView },
    { path: '/jobs', name: 'jobs', component: JobsView },
    { path: '/jobs/:name', name: 'job-detail', component: JobDetailView },
    {
      path: '/peers',
      name: 'peers',
      component: () => import('../views/PlaceholderView.vue'),
      meta: { title: 'Peers' },
    },
    {
      path: '/events',
      name: 'events',
      component: () => import('../views/PlaceholderView.vue'),
      meta: { title: 'Events' },
    },
  ],
})

export default router
