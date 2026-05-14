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
      path: '/snapshots',
      name: 'snapshots',
      component: () => import('../views/SnapshotsView.vue'),
    },
    {
      path: '/pools',
      name: 'pools',
      component: () => import('../views/PoolsView.vue'),
    },
    {
      path: '/arc',
      name: 'arc',
      component: () => import('../views/ArcView.vue'),
    },
    {
      path: '/pools/:name',
      name: 'pool-detail',
      component: () => import('../views/PoolDetailView.vue'),
    },
    {
      path: '/config',
      name: 'config',
      component: () => import('../views/ConfigView.vue'),
    },
    { path: '/peers', name: 'peers', component: () => import('../views/PeersView.vue') },
    {
      path: '/peers/:peer/:tab(jobs|snapshots)',
      name: 'peer-detail',
      component: () => import('../views/PeerDetailView.vue'),
    },
    {
      path: '/events',
      name: 'events',
      component: () => import('../views/EventsView.vue'),
    },
  ],
})

export default router
