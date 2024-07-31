<!-- ReplayLink.vue -->
<template>
  <span
    ><a v-for="(url, index) in urls" :key="index" :href="url"
      >[{{ index + 1 }}]</a
    >
  </span>
</template>

<script lang="ts">
export default {
  props: ['params'],
  computed: {
    urls() {
      const replayUrls = this.params.value.map(
        (id: string) => `https://bar-rts.com/replays/${id}`,
      )
      return replayUrls
    },
  },
}
</script>
<style scoped>
a {
  margin-right: 0.2em;
  text-decoration: none;
}
</style>
