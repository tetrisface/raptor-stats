<script setup lang="ts">
import Grid from './components/Grid.vue'
</script>

<template>
  <Suspense>
    <template #default>
      <Grid />
    </template>
    <template #fallback>
      <div>Loading...</div>
    </template>
  </Suspense>
</template>
