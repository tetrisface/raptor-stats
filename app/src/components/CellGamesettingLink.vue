<template>
  <div class="link-cell">
    <div
      v-if="
        params?.data.index !== undefined &&
        params?.data.index !== '' &&
        params?.value !== undefined
      "
    >
      <div
        class="text"
        @click="gamesettingLinkClick"
        @mousedown="gamesettingLinkClick"
      >
        <v-icon name="hi-solid-external-link" class="icon" />
        {{ props.params?.colDef.valueFormatter(props.params) }}
      </div>
    </div>
    <div v-else>
      <span class="text">
        {{ props.params?.colDef.valueFormatter(props.params) }}
      </span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import { QueryParams } from '../types'
import { RouteGenerator } from '../util'
import eventBus from '../eventBus'

const props = defineProps({
  params: Object,
})
const router = useRouter()
const gamesettingLinkClick = (event: any) => {
  eventBus.emit('gamesetting-link-clicked')
  event.preventDefault()
  event.stopPropagation()

  let filter
  if (props.params?.value === 1) {
    filter = 'unbeaten'
  } else if (props.params?.value === 0) {
    filter = 'easy'
  } else {
    filter = 'regular'
  }

  const queryParams = {
    view: 'gamesettings',
    ai: props.params?.ai,
    filter,
    row: props.params?.data?.index,
  } as QueryParams

  const url = RouteGenerator(queryParams)
  if (event.button === 1 || event.ctrlKey) {
    window.open(url, '_blank')
  } else {
    router.push({ name: 'Grid', replace: true, query: queryParams })
  }
}
</script>
