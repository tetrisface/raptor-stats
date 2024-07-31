<template>
  <div class="cell-renderer">
    <!-- Transition element for copy button and checkmark icon -->
    <transition
      name="fade"
      @before-enter="beforeEnter"
      @enter="enter"
      @leave="leave"
    >
      <button v-if="!copied" @click="copyToClipboard" class="copy-button">
        <v-icon name="fa-regular-copy" />
      </button>
      <v-icon v-else name="bi-check-circle-fill" class="check-icon" />
    </transition>
    <!-- Display cell value -->
    <span class="text">{{ params?.value }}</span>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

const props = defineProps({
  params: Object,
})

const copied = ref(false)

function copyToClipboard() {
  navigator.clipboard
    .writeText(props.params?.value)
    .then(() => {
      copied.value = true
      setTimeout(() => {
        copied.value = false
      }, 1500) // Reset the icon after 1.5 seconds
    })
    .catch((err) => {
      console.error('Failed to copy: ', err)
    })
}

function beforeEnter(el: any) {
  el.style.opacity = 0
}

function enter(el: any, done: Function) {
  el.offsetHeight // trigger reflow
  el.style.transition = 'opacity 0.5s ease'
  el.style.opacity = 1
  done()
}

function leave(el: any, done: Function) {
  el.style.transition = 'opacity 0.5s ease'
  el.style.opacity = 0
  done()
}
</script>

<style scoped>
.cell-renderer {
  display: flex;
  align-items: center;
  gap: 2px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.text {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
}

.copy-button {
  padding: 0;
  background: transparent;
  border: none;
  cursor: pointer;
  font-size: 16px;
}

.check-icon {
  font-size: 16px;
  color: green;
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.5s ease;
}

.fade-enter,
.fade-leave-to {
  opacity: 0;
}
</style>
