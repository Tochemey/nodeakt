<script setup lang="ts">
defineProps<{ name: string }>();
</script>

<template>
  <svg
    class="nk-mark"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.6"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    <template v-if="name === 'actor'">
      <circle cx="12" cy="12" r="4" />
      <circle cx="12" cy="12" r="8.5" opacity="0.35" />
    </template>

    <template v-else-if="name === 'typed'">
      <rect x="5" y="5" width="14" height="14" rx="2" />
      <path d="M8 10h8M8 14h5" />
    </template>

    <!-- Supervision: a parent over two children. -->
    <template v-else-if="name === 'supervision'">
      <circle cx="12" cy="6.5" r="2.2" />
      <circle cx="6.5" cy="17" r="2.2" />
      <circle cx="17.5" cy="17" r="2.2" />
      <path d="M12 8.7v2.3L7.8 15M12 11l4.2 4" />
    </template>

    <!-- Behaviors: stacked handlers. -->
    <template v-else-if="name === 'behaviors'">
      <rect x="6" y="8" width="12" height="10" rx="2" />
      <path d="M9 8V6.5A1.5 1.5 0 0 1 10.5 5h3A1.5 1.5 0 0 1 15 6.5V8" />
    </template>

    <!-- Mailbox: an open tray. -->
    <template v-else-if="name === 'mailbox'">
      <path d="M4.5 9.5 12 4.5l7.5 5V18a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 18V9.5Z" />
      <path d="M4.5 9.5 12 13.5l7.5-4" />
    </template>

    <!-- Pipe / request / schedule: a message leaving. -->
    <template v-else-if="name === 'pipe'">
      <circle cx="6.5" cy="12" r="2.2" />
      <path d="M9.2 12h8.3M15.2 8.5 19.5 12l-4.3 3.5" />
    </template>

    <!-- Multi-core: the node hexagon. -->
    <template v-else-if="name === 'cores'">
      <path d="M12 3.5 19 7.5v9L12 20.5 5 16.5v-9L12 3.5Z" />
      <circle cx="12" cy="12" r="2" />
    </template>

    <!-- Remoting: two nodes, a dashed hop. -->
    <template v-else-if="name === 'tcp'">
      <circle cx="6" cy="12" r="2.4" />
      <circle cx="18" cy="12" r="2.4" />
      <path d="M9 12h6" stroke-dasharray="2.2 2.2" />
    </template>

    <!-- TLS: the protocol wrapped by a carrier. -->
    <template v-else-if="name === 'tls'">
      <rect x="5" y="7" width="14" height="11" rx="2" />
      <path d="M8.5 7V6a3.5 3.5 0 0 1 7 0v1" />
    </template>
    <!-- One package: a single box. -->
    <template v-else-if="name === 'deps'">
      <rect x="6" y="6" width="12" height="12" rx="2" />
    </template>
  </svg>
</template>

<style scoped>
.nk-mark {
  display: block;
  width: 22px;
  height: 22px;
}
</style>
