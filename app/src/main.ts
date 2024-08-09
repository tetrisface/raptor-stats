import { createApp } from 'vue'
import './style.css'
import App from './App.vue'

import 'ag-grid-community/styles/ag-grid.min.css'
import 'ag-grid-community/styles/ag-theme-quartz.min.css'
import { OhVueIcon, addIcons } from 'oh-vue-icons'
import {
  BiCheckCircleFill,
  FaRegularCopy,
  FcSettings,
  GiTrophyCup,
  HiSolidExternalLink,
  LaInfoCircleSolid,
} from 'oh-vue-icons/icons'
import { AgGridVue } from 'ag-grid-vue3'
import CellInfo from './components/CellInfo.vue'
import CellReplayLinks from './components/CellReplayLinks.vue'
import CellCopy from './components/CellCopy.vue'
import TooltipDifficultyScore from './components/TooltipDifficultyScore.vue'
import CellGamesettingLink from './components/CellGamesettingLink.vue'
import router from './router'

addIcons(
  BiCheckCircleFill,
  FaRegularCopy,
  FcSettings,
  GiTrophyCup,
  HiSolidExternalLink,
  LaInfoCircleSolid,
)

const app = createApp(App)
app.component('v-icon', OhVueIcon)
app.component('ag-grid-vue', AgGridVue)
app.component('CellInfo', CellInfo)
app.component('CellReplayLinks', CellReplayLinks)
app.component('CellCopy', CellCopy)
app.component('TooltipDifficultyScore', TooltipDifficultyScore)
app.component('CellGamesettingLink', CellGamesettingLink)

app.use(router)

app.mount('#app')
