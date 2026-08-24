<script setup lang="ts">
import DefaultTheme from "vitepress/theme";
import HomeSections from "./HomeSections.vue";
</script>

<template>
  <DefaultTheme.Layout>
    <template #home-features-after>
      <HomeSections />
    </template>
  </DefaultTheme.Layout>
</template>
