import { createRouter, createWebHistory } from 'vue-router'
import Grid from './components/Grid.vue'

const routes = [
  {
    path: '/',
    name: 'Grid',
    component: Grid,
    // props: (route: any) => ({ query: route.query }),
  },
  {
    path: '/:asdf',
    component: Grid,
    props: (route: any) => ({ query: route.query, asdf: route.params.asdf }),
  },
  {
    path: '/',
    component: Grid,
    props: (route: any) => ({ query: route.query, asdf: route.params.asdf }),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
