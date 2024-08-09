<template>
  <div v-if="showTooltip" class="difficulty-score-tooltip">
    <ag-grid-vue
      class="ag-theme-quartz-auto-dark"
      :columnDefs="columnDefs"
      :domLayout="'autoHeight'"
      :getRowStyle="getRowStyle"
      :rowData="rowData"
      :components="components"
      :gridOptions="{
        tooltipShowDelay: 0,
        tooltipInteraction: true,
      }"
    />
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { AgGridVue } from 'ag-grid-vue3'
import { sum } from 'lodash'
import CellGamesettingLink from './CellGamesettingLink.vue'
import eventBus from '../eventBus'

const props = defineProps({
  params: Object,
})

const defaultColDef = {
  flex: 1,
  minWidth: 100,
  sortable: true,
  filter: true,
  editable: false,
  valueFormatter: (params: any) =>
    `${((params?.value ?? 0) * 100).toFixed(0)}%`,
}

const columnDefs = ref([
  {
    ...defaultColDef,
    ...{
      headerName: 'Difficulty',
      field: 'diff',
      cellRenderer: 'CellGamesettingLink',
      cellRendererParams: {
        view: props.params?.view,
        ai: props.params?.ai,
        filter: props.params?.filter,
      },
    },
  },
  {
    ...defaultColDef,
    ...{
      headerName: 'Completion',
      field: 'completion',
      headerTooltip:
        'The amount of unique teammates in the gamesetting divided by a mapped value for each lobby size. Solo lobby win gives full completion. For example a 16 player lobby wins requires 40 unique teammates. So 40% completion each 16 player win. It is about 2-3 different lobbies that is needed for full completion.',
    },
  },
  {
    ...defaultColDef,
    ...{
      headerName: 'Difficulty Completion',
      field: 'diff_completion',
      headerTooltip:
        'Difficulty*Completion. The sum of all 5 is divided by 5 and then used as Difficulty Score which has a Combined Rank weight of 0.25.',
    },
  },
  {
    ...defaultColDef,
    ...{
      headerName: 'Gamesetting',
      field: 'index',
      valueFormatter: (params: any) => params?.value?.toString(),
      hide: true,
    },
  },
])

const sums = {
  completion: sum(props.params?.value.completions) / 5,
  diff_completion: sum(props.params?.value.diff_completions) / 5,
  diff: sum(props.params?.value.diffs) / 5,
  index: '',
}
const rowData = ref(
  props.params?.value.indices
    .map((index: number, i: number) => ({
      index: Number(index),
      completion: props.params?.value.completions[i],
      diff_completion: props.params?.value.diff_completions[i],
      diff: props.params?.value.diffs[i],
    }))
    .concat(sums),
)

const getRowStyle = (params: any) => {
  if (params.node.rowIndex === rowData.value.length - 1) {
    return {
      background: '#5d6a7c',
    }
  }
}
const showTooltip = ref(true)

const handleGamesettingLinkClicked = () => {
  showTooltip.value = false
}

onMounted(() => {
  eventBus.on('gamesetting-link-clicked', handleGamesettingLinkClicked)
})

onUnmounted(() => {
  eventBus.off('gamesetting-link-clicked', handleGamesettingLinkClicked)
})

const components = ref({
  CellGamesettingLink,
})
</script>

<style scoped>
.difficulty-score-tooltip {
  min-width: 620px;
  padding: 0px;
  margin: 0;
  border-radius: 4px;
  color: #fff;
  font-size: 10px;
  text-align: center;
  white-space: nowrap;
}
</style>
