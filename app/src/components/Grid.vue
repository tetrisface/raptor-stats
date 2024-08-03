<script lang="ts">
import { Ref, ref, watch } from 'vue'
import 'ag-grid-community/styles/ag-grid.css' // Mandatory CSS required by the Data Grid
import 'ag-grid-community/styles/ag-theme-quartz.css' // Optional Theme applied to the Data Grid
import { AgGridVue } from 'ag-grid-vue3' // Vue Data Grid Component
import ReplayLink from './ReplayLink.vue'
import CellCopy from './CellCopy.vue'
import { fetchData } from '../resolver'

type View = { title: any; rating?: boolean | undefined }

export default {
  name: 'App',
  components: {
    AgGridVue,
    ReplayLink,
    CellCopy,
  },
  async setup() {
    const rowData = ref([])
    const colDefs: Ref<any[]> = ref([])
    const defaultColDef = ref({
      editable: true,
    })

    const views: { [key: string]: View } = {
      gamesetting_games: { title: 'Recent Games' },
      'Barbarian.regular.grouped_gamesettings': { title: 'Barbarian' },
      'Barbarian.unbeaten.grouped_gamesettings': {
        title: 'Barbarian unbeaten',
      },
      'Barbarian.cheese.grouped_gamesettings': { title: 'Barbarian cheese' },
      'Raptors.regular.grouped_gamesettings': { title: 'Raptors' },
      'Raptors.unbeaten.grouped_gamesettings': { title: 'Raptors unbeaten' },
      'Raptors.cheese.grouped_gamesettings': { title: 'Raptors cheese' },
      'Scavengers.regular.grouped_gamesettings': {
        title: 'Scavengers',
      },
      'Scavengers.unbeaten.grouped_gamesettings': {
        title: 'Scavengers unbeaten',
      },
      'Scavengers.cheese.grouped_gamesettings': { title: 'Scavengers cheese' },
      'PveRating.Barbarian_gamesettings': {
        title: 'PVE Rating Barbarian',
        rating: true,
      },
      'PveRating.Raptors_gamesettings': {
        title: 'PVE Rating Raptors',
        rating: true,
      },
      'PveRating.Scavengers_gamesettings': {
        title: 'PVE Rating Scavengers',
        rating: true,
      },
    }

    const dataParam = ref(
      new URL(window.location.href).hash.slice(1) == ''
        ? 'gamesetting_games'
        : new URL(window.location.href).hash.slice(1),
    )

    const changeDataParam = (event: any) => {
      window.location.hash = dataParam.value =
        event.currentTarget.getAttribute('data-param')
    }

    watch(dataParam, (newParam) => {
      fetchData(newParam, rowData, colDefs)
    })

    fetchData(dataParam.value, rowData, colDefs)

    const agGrid = ref()

    const onFirstDataRendered = (event: any) => {
      event.api.autoSizeColumns(
        colDefs.value.reduce((acc: string[], column) => {
          if (
            !column.field.includes('Replays') &&
            !column.field.includes('tweak') &&
            ![
              'Difficulty',
              '#Winners',
              '#Players',
              'Players',
              'Winners',
              'Copy Paste',
            ].includes(column.field)
          ) {
            acc.push(column.field)
          }
          return acc
        }, []),
        false,
      )
    }

    const isSelected = (param: any) => {
      return dataParam.value === param
    }

    return {
      isSelected,
      changeDataParam,
      rowData,
      colDefs,
      defaultColDef,
      onFirstDataRendered,
      agGrid,
      views,
    }
  },
}
</script>

<template>
  <div id="buttons">
    <button
      v-for="(value, key) in views"
      :key="key"
      :data-param="key"
      :class="{ selected: isSelected(key) }"
      @click="changeDataParam($event)"
    >
      <div class="icon-text-container">
        <span class="button-icon">
          <v-icon v-if="value.rating" name="gi-trophy-cup" />
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
      :rowData="rowData"
      :columnDefs="colDefs"
      :defaultColDef="defaultColDef"
      @first-data-rendered="onFirstDataRendered"
      ref="agGrid"
      style="height: 100%; margin-bottom: 0.16rem"
      class="ag-theme-quartz-auto-dark"
    >
    </ag-grid-vue>
  </div>
</template>

<style scoped>
.ag-theme-quartz-auto-dark {
  --ag-font-size: 0.75rem;
}
#buttons {
  display: flex;
  margin: 0;
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
  margin: 0.3rem 0.3rem;
  padding: 0.3rem;
  text-decoration: none;
  touch-action: manipulation;
  transition: all 0.1s ease-in-out;
  user-select: none;
}
@media (min-width: 1730px) {
  button {
    font-size: 1rem;
  }
}
@media (min-width: 1260px) and (max-width: 1730px) {
  button {
    font-size: clamp(0.6rem, 1rem - 11vw, 1.4rem);
  }
}
@media (min-width: 900px) and (max-width: 1260px) {
  #buttons {
    display: inline;
  }
  button {
    font-size: clamp(0.6rem, -0.7rem + 1.9vw, 0.8rem);
  }
}
@media (min-width: 0px) and (max-width: 900px) {
  #buttons {
    display: inline;
  }
  button {
    flex: 0;
    font-size: 0.8rem;
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
</style>
