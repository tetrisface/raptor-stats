<template>
  <div id="buttons">
    <button
      v-for="value in views"
      :data-view="value.view"
      :data-ai="value.ai"
      :data-filter="value.filter"
      :class="{
        selected: isSelected(value.view, value.ai ?? '', value.filter ?? ''),
      }"
      @click="navButtonClick"
      @mousedown="navButtonClick"
    >
      <div class="icon-text-container">
        <span class="button-icon">
          <v-icon v-if="value.view === 'ratings'" name="gi-trophy-cup" />
          <v-icon v-else name="fc-settings" />
        </span>
        <span class="button-title">
          {{ value.title }}
        </span>
      </div>
    </button>
  </div>
  <div id="app">
    <ag-grid-vue
      class="ag-theme-quartz-auto-dark"
      style="height: 100%; margin-bottom: 0.16em"
      :rowData="rowData"
      :columnDefs="colDefs"
      :defaultColDef="defaultColDef"
      :gridOptions="{
        tooltipShowDelay: 0,
        tooltipInteraction: true,
        suppressScrollOnNewData: true,
      }"
      @grid-ready="onGridReady"
      @first-data-rendered="onFirstDataRendered"
      @row-data-updated="onRowDataUpdated"
    >
    </ag-grid-vue>
  </div>
</template>

<script setup lang="ts">
import { Ref, ref, watch } from 'vue'
import 'ag-grid-community/styles/ag-grid.css' // Mandatory CSS required by the Data Grid
import 'ag-grid-community/styles/ag-theme-quartz.css' // Optional Theme applied to the Data Grid
import { GridApi } from 'ag-grid-community'
import { fetchData } from '../resolver'
import { useRoute, useRouter } from 'vue-router'
import {
  AIType,
  FetchParams,
  FilterType,
  QueryParams,
  View,
  ViewType,
} from '../types'
import { RouteGenerator } from '../util'

const rowData = ref([])
const colDefs: Ref<any[]> = ref([])
const defaultColDef = ref({
  editable: true,
})

const views: { [key: string]: View } = {
  recent_games: { title: 'Recent Games', view: 'recent_games' },
  'Barbarian.regular.gamesettings': {
    title: 'Barbarian',
    view: 'gamesettings',
    ai: 'Barbarian',
    filter: 'regular',
  },
  'Barbarian.unbeaten.gamesettings': {
    title: 'Barbarian unbeaten',
    view: 'gamesettings',
    ai: 'Barbarian',
    filter: 'unbeaten',
  },
  'Barbarian.cheese.gamesettings': {
    title: 'Barbarian easy',
    view: 'gamesettings',
    ai: 'Barbarian',
    filter: 'easy',
  },
  'Raptors.regular.gamesettings': {
    title: 'Raptors',
    view: 'gamesettings',
    ai: 'Raptors',
    filter: 'regular',
  },
  'Raptors.unbeaten.gamesettings': {
    title: 'Raptors unbeaten',
    view: 'gamesettings',
    ai: 'Raptors',
    filter: 'unbeaten',
  },
  'Raptors.cheese.gamesettings': {
    title: 'Raptors easy',
    view: 'gamesettings',
    ai: 'Raptors',
    filter: 'easy',
  },
  'Scavengers.regular.gamesettings': {
    title: 'Scavengers',
    view: 'gamesettings',
    ai: 'Scavengers',
    filter: 'regular',
  },
  'Scavengers.unbeaten.gamesettings': {
    title: 'Scavengers unbeaten',
    view: 'gamesettings',
    ai: 'Scavengers',
    filter: 'unbeaten',
  },
  'Scavengers.cheese.gamesettings': {
    title: 'Scavengers easy',
    view: 'gamesettings',
    ai: 'Scavengers',
    filter: 'easy',
  },
  'PveRating.Barbarian_gamesettings': {
    title: 'PVE Rating Barbarian',
    view: 'ratings',
    ai: 'Barbarian',
  },
  'PveRating.Raptors_gamesettings': {
    title: 'PVE Rating Raptors',
    view: 'ratings',
    ai: 'Raptors',
  },
  'PveRating.Scavengers_gamesettings': {
    title: 'PVE Rating Scavengers',
    view: 'ratings',
    ai: 'Scavengers',
  },
}

const route = useRoute()
const router = useRouter()

const view = ref(route.query.view ?? 'recent_games')
const ai = ref(route.query.ai ?? '')
const filter = ref(route.query.filter ?? '')
const row = ref(route.query.row ?? '')

watch(
  () => route.query,
  (newQuery) => {
    view.value = newQuery.view ?? 'recent_games'
    ai.value = newQuery.ai ?? ''
    filter.value = newQuery.filter ?? ''
    row.value = newQuery.row ?? ''
  },
  { immediate: true },
)

watch([view, ai, filter, row], ([newView, newAi, newFilter, newRow]) => {
  router.replace({
    name: 'Grid',
    query: {
      view: newView,
      ai: newAi,
      filter: newFilter,
      row: newRow,
    },
  })
  fetchData({
    view: newView,
    ai: newAi,
    filter: newFilter,
    rowData,
    colDefs,
  } as FetchParams)
})

const onRowDataUpdated = () => {
  focusAndSelectRow()
}
const onGridReady = () => {
  fetchData({
    view: view.value as ViewType,
    ai: ai.value as AIType,
    filter: filter.value as FilterType,
    rowData,
    colDefs,
  })
}

function focusAndSelectRow(rowIndex: any | undefined = undefined) {
  if (!gridApi.value) return

  rowIndex = rowIndex ?? row.value

  if (rowIndex === '') {
    gridApi.value.ensureIndexVisible(0)
  }

  rowIndex = parseInt(rowIndex)
  if (
    isNaN(rowIndex) ||
    rowIndex === '' ||
    rowIndex === undefined ||
    rowIndex === null ||
    rowIndex > rowData.value.length ||
    rowIndex > gridApi.value.getDisplayedRowCount()
  ) {
    return
  }

  gridApi.value.ensureIndexVisible(rowIndex)
  gridApi.value.getDisplayedRowAtIndex(rowIndex)?.setSelected(true)
}
const gridApi = ref<GridApi | null>(null)

watch(gridApi, () => {
  focusAndSelectRow(row.value)
})

const navButtonClick = (event: any) => {
  if (event.type === 'mousedown' && event.button !== 1) return
  event.preventDefault()
  event.stopPropagation()
  const [_view, _ai, _filter] = ['view', 'ai', 'filter'].map(
    (x) => event.currentTarget.getAttribute(`data-${x}`) ?? '',
  )

  const queryParams = {
    view: _view,
    ai: _ai,
    filter: _filter,
    row: '',
  } as QueryParams

  const url = RouteGenerator(queryParams)

  if (event.ctrlKey || event.button === 1) {
    window.open(url, '_blank')
    return
  }

  router.push({ path: url })

  view.value = _view
  ai.value = _ai
  filter.value = _filter
  row.value = ''
}

const onFirstDataRendered = (event: any) => {
  gridApi.value = event.api
  event.api.autoSizeColumns(
    colDefs.value.reduce((acc: string[], column) => {
      if (
        column.field &&
        !column.field.includes('tweak') &&
        !/[A-Z]/.test(column.field)
      ) {
        acc.push(column.field)
      }
      return acc
    }, []),
    false,
  )
}

const isSelected = (_view: string, _ai: string, _filter: string) => {
  return view.value === _view && ai.value === _ai && filter.value === _filter
}
</script>

<style scoped>
.ag-theme-quartz-auto-dark {
  --ag-font-size: 0.75rem;
}
#buttons {
  display: flex;
  margin-top: 0.3rem;
  margin-bottom: 0.3rem;
  margin-left: 0.2rem;
  margin-right: 0.2rem;
  gap: 0.65rem;
  height: 3.8rem;
}

button {
  -webkit-user-select: none;
  background-color: color-mix(in srgb, #fff, #182230 93%);
  border-radius: 0.25rem;
  border: 2px solid transparent;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  color: #fff;
  cursor: pointer;
  flex: 1;
  font-family: 'IBM Plex Sans', sans-serif;
  font-style: normal;
  font-weight: 500;
  line-height: 20px;
  text-decoration: none;
  touch-action: manipulation;
  transition: all 0.1s ease-in-out;
  user-select: none;
}
@media (min-width: 1730px) {
  button {
    font-size: calc(0.75rem + 0.2vw);
  }
}
@media (min-width: 1260px) and (max-width: 1730px) {
  button {
    font-size: calc(0.222rem + 0.55vw);
  }
}
@media (min-width: 900px) and (max-width: 1345px) {
  #buttons {
    display: inline;
  }
  button {
    margin: 0.2rem;
  }
}
@media (min-width: 0px) and (max-width: 900px) {
  #buttons {
    display: inline;
    gap: 0.2rem;
  }
  button {
    margin: 0.2rem;
  }
}

button:hover,
button.selected {
  background-color: color-mix(in srgb, #fff, #182230 70%);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
  border-color: transparent;
  color: #fff;
}
.icon-text-container {
  display: flex;
  align-items: center;
}

.button-icon {
  margin-left: 0.2rem;
  margin-right: 0.4rem;
}

.link-overlay {
  text-decoration: none;
  color: inherit;
}
</style>
