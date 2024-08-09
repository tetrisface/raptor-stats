<template>
  <div class="info-cell">
    <v-icon name="la-info-circle-solid" class="icon" />
    <span class="text">{{
      props.params?.colDef.valueFormatter(props.params)
    }}</span>
  </div>
</template>

<script setup lang="ts">
const props = defineProps({
  params: Object,
})
</script>

<style scoped>
.info-cell {
  display: flex;
  align-items: center;
  gap: 6px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.icon {
  width: 1.25em;
  height: 1.25em;
}

.text {
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
