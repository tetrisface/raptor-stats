<script setup lang="ts">
import AgGridTest from './components/Grid.vue'
</script>

<template>
  <Suspense>
    <template #default>
      <AgGridTest />
    </template>
    <template #fallback>
      <div>Loading...</div>
    </template>
  </Suspense>
</template>
<style scoped></style>
