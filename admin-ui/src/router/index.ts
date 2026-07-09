import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'
import JobsView from '../views/JobsView.vue'
import JobDetailView from '../views/JobDetailView.vue'

// Every view is host-scoped: bare paths are the local daemon, the same
// paths under /h/:host render the SAME components against a peer via
// the generic proxy. One console, N hosts.
const scoped = [
  { path: 'dashboard', name: 'dashboard', component: DashboardView },
  { path: 'jobs', name: 'jobs', component: JobsView },
  { path: 'jobs/:name', name: 'job-detail', component: JobDetailView },
  {
    path: 'snapshots',
    name: 'snapshots',
    component: () => import('../views/SnapshotsView.vue'),
  },
  {
    path: 'pools',
    name: 'pools',
    component: () => import('../views/PoolsView.vue'),
  },
  {
    path: 'pools/:name',
    name: 'pool-detail',
    component: () => import('../views/PoolDetailView.vue'),
  },
  {
    path: 'arc',
    name: 'arc',
    component: () => import('../views/ArcView.vue'),
  },
  {
    path: 'events',
    name: 'events',
    component: () => import('../views/EventsView.vue'),
  },
  {
    path: 'config',
    name: 'config',
    component: () => import('../views/ConfigView.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', redirect: '/dashboard' },
    ...scoped.map((r) => ({ ...r, path: `/${r.path}` })),
    ...scoped.map((r) => ({
      ...r,
      path: `/h/:host/${r.path}`,
      name: `h-${r.name}`,
    })),
    {
      path: '/peers',
      name: 'peers',
      component: () => import('../views/PeersView.vue'),
    },
    // Legacy tabbed peer page → the host-scoped console.
    { path: '/peers/:host/jobs', redirect: (to) => `/h/${String(to.params.host)}/jobs` },
    { path: '/peers/:host/snapshots', redirect: (to) => `/h/${String(to.params.host)}/snapshots` },
  ],
})

export default router
