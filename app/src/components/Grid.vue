<script lang="ts">
import { Ref, ref, watch } from 'vue'
import 'ag-grid-community/styles/ag-grid.css' // Mandatory CSS required by the Data Grid
import 'ag-grid-community/styles/ag-theme-quartz.css' // Optional Theme applied to the Data Grid
import { AgGridVue } from 'ag-grid-vue3' // Vue Data Grid Component
import { parquetMetadata, parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import ReplayLink from './ReplayLink.vue'
import columnsToColDefs from '../columnDefs'
import CellCopy from './CellCopy.vue'

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

    const dataParam = ref(
      new URL(window.location.href).hash.slice(1) == ''
        ? 'Barbarian.grouped'
        : new URL(window.location.href).hash.slice(1),
    )

    const changeDataParam = (event: any) => {
      window.location.hash = dataParam.value =
        event.currentTarget.getAttribute('data-param')
    }

    watch(dataParam, (newParam) => {
      fetchData(newParam)
    })

    const fetchData = async (dataParam: string) => {
      try {
        const response = await fetch(
          `https://${
            process.env.ENV === 'dev' ? 'dev.' : ''
          }files.pverating.bar/${dataParam}_gamesettings.parquet`,
        )
        if (!response.ok) {
          console.log('response.ok', response.ok, 'response', response)
        }

        const arrayBuffer = await response.arrayBuffer()
        await parquetRead({
          file: arrayBuffer,
          compressors,
          onComplete: (parquetData: any) => {
            const schema = parquetMetadata(arrayBuffer).schema
            const columns = schema.reduce(
              (acc: { [key: string]: string }[], column, index) => {
                if (
                  index > 0 &&
                  schema[index].name !== 'item' &&
                  schema[index].name !== 'list'
                ) {
                  acc.push({
                    name: column.name,
                    type: column.type === 'BYTE_ARRAY' ? 'string' : 'number',
                  })
                }
                return acc
              },
              [],
            )

            rowData.value = parquetData.map((row: any) =>
              row.reduce((acc: any, value: any, index: number) => {
                acc[columns[index].name] = value
                return acc
              }, {}),
            )

            colDefs.value = columnsToColDefs(columns, dataParam)
          },
        })
      } catch (error) {
        console.error('Fetch or read error:', error)
      }
    }
    fetchData(dataParam.value)

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
    const items = [
      { title: 'Barbarian', dataParam: 'Barbarian.regular.grouped' },
      { title: 'Barbarian unbeaten', dataParam: 'Barbarian.unbeaten.grouped' },
      { title: 'Barbarian cheese', dataParam: 'Barbarian.cheese.grouped' },
      { title: 'Raptors', dataParam: 'Raptors.regular.grouped' },
      { title: 'Raptors unbeaten', dataParam: 'Raptors.unbeaten.grouped' },
      { title: 'Raptors cheese', dataParam: 'Raptors.cheese.grouped' },
      { title: 'Scavengers', dataParam: 'Scavengers.regular.grouped' },
      {
        title: 'Scavengers unbeaten',
        dataParam: 'Scavengers.unbeaten.grouped',
      },
      { title: 'Scavengers cheese', dataParam: 'Scavengers.cheese.grouped' },
      {
        title: 'Barbarian PVE Rating',
        rating: true,
        dataParam: 'PveRating.Barbarian',
      },
      {
        title: 'Raptors PVE Rating',
        rating: true,
        dataParam: 'PveRating.Raptors',
      },
      {
        title: 'Scavengers PVE Rating',
        rating: true,
        dataParam: 'PveRating.Scavengers',
      },
    ]

    return {
      isSelected,
      changeDataParam,
      rowData,
      colDefs,
      defaultColDef,
      onFirstDataRendered,
      agGrid,
      items,
    }
  },
}
</script>

<template>
  <!-- The AG Grid component -->
  <div id="buttons">
    <button
      v-for="item in items"
      :key="item.dataParam"
      :data-param="item.dataParam"
      :class="{ selected: isSelected(item.dataParam) }"
      @click="changeDataParam($event)"
    >
      <div class="icon-text-container">
        <span class="button-icon">
          <v-icon v-if="item.rating" name="gi-trophy-cup" />
          <v-icon v-else name="fc-settings" />
        </span>
        <span class="button-title">
          {{ item.title }}
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
  align-items: center; /* Ensures vertical alignment if needed */
}

.button-icon {
  margin-left: 0.2rem; /* Adjust spacing between icon and text as needed */
  margin-right: 0.4rem; /* Adjust spacing between icon and text as needed */
  /* margin-top: 0.11rem; */
}
</style>
