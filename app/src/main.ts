import { createApp } from 'vue'
import './style.css'
import App from './App.vue'

import 'ag-grid-community/styles/ag-grid.min.css'
import 'ag-grid-community/styles/ag-theme-quartz.min.css'
import { OhVueIcon, addIcons } from 'oh-vue-icons'
import {
  GiTrophyCup,
  FcSettings,
  FaRegularCopy,
  BiCheckCircleFill,
} from 'oh-vue-icons/icons'

addIcons(GiTrophyCup, FcSettings, FaRegularCopy, BiCheckCircleFill)

const app = createApp(App)
app.component('v-icon', OhVueIcon)
app.mount('#app')
