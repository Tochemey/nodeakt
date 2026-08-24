<script setup lang="ts">
import { computed } from "vue";
import { useData, withBase } from "vitepress";
import FeatureMark from "./FeatureMark.vue";

interface Item {
  title: string;
  details: string;
  link?: string;
  icon?: string;
  wide?: boolean;
  badge?: string;
}

interface Band {
  kicker: string;
  title?: string;
  details: string;
  items: Item[];
}

const { frontmatter } = useData();
const runtime = computed(() => frontmatter.value.runtime as Band | undefined);
const networking = computed(() => frontmatter.value.networking as Band | undefined);
const deps = computed(() => frontmatter.value.deps as Item | undefined);
</script>

<template>
  <div v-if="runtime || networking || deps" class="nk-home">
    <section v-if="runtime" class="nk-band" aria-labelledby="nk-runtime-title">
      <div class="nk-wrap">
        <header class="nk-head">
          <p class="nk-kicker">{{ runtime.kicker }}</p>
          <h2 id="nk-runtime-title">{{ runtime.title }}</h2>
          <p class="nk-lede">{{ runtime.details }}</p>
        </header>
        <ul class="nk-bento">
          <li
            v-for="(item, i) in runtime.items"
            :key="item.title"
            :class="['nk-cell', i === 0 && 'nk-cell--lead', item.wide && 'nk-cell--wide']"
          >
            <a v-if="item.link" class="nk-tile" :href="withBase(item.link)">
              <span class="nk-tile__head">
                <span class="nk-tile__icon">
                  <FeatureMark :name="item.icon ?? 'actor'" />
                </span>
                <span class="nk-tile__title">{{ item.title }}</span>
              </span>
              <span class="nk-tile__details">{{ item.details }}</span>
              <span class="nk-go">Read <span aria-hidden="true">→</span></span>
            </a>
          </li>
        </ul>
      </div>
    </section>

    <section v-if="networking" class="nk-band nk-band--net" aria-labelledby="nk-net-kicker">
      <div class="nk-wrap">
        <header class="nk-head">
          <p id="nk-net-kicker" class="nk-kicker">{{ networking.kicker }}</p>
          <h2 v-if="networking.title" id="nk-net-title">{{ networking.title }}</h2>
          <p class="nk-lede">{{ networking.details }}</p>
        </header>
        <ul class="nk-panels">
          <li v-for="item in networking.items" :key="item.title">
            <a class="nk-panel" :href="withBase(item.link ?? '/')">
              <span class="nk-panel__top">
                <span class="nk-tile__icon">
                  <FeatureMark :name="item.icon ?? 'tcp'" />
                </span>
                <span class="nk-panel__title">{{ item.title }}</span>
                <span v-if="item.badge" class="nk-badge">{{ item.badge }}</span>
              </span>
              <span class="nk-panel__details">{{ item.details }}</span>
              <span class="nk-go">Read <span aria-hidden="true">→</span></span>
            </a>
          </li>
        </ul>
      </div>
    </section>

    <section v-if="deps" class="nk-band" aria-labelledby="nk-deps-title">
      <div class="nk-wrap">
        <article class="nk-tile nk-tile--static">
          <span class="nk-tile__head">
            <span class="nk-tile__icon">
              <FeatureMark name="deps" />
            </span>
            <h2 id="nk-deps-title" class="nk-tile__title">{{ deps.title }}</h2>
          </span>
          <p class="nk-tile__details">{{ deps.details }}</p>
        </article>
      </div>
    </section>
  </div>
</template>

<style scoped>
.nk-home {
  padding-bottom: 8px;
}

.nk-wrap {
  margin: 0 auto;
  max-width: 1152px;
  padding: 0 24px;
}

@media (min-width: 640px) {
  .nk-wrap {
    padding: 0 48px;
  }
}

@media (min-width: 960px) {
  .nk-wrap {
    padding: 0 64px;
  }
}

.nk-band {
  padding-top: 16px;
}

.nk-band--net {
  margin-top: 64px;
  padding: 56px 0 8px;
  background: var(--vp-c-bg-alt);
}

.nk-band--net + .nk-band {
  margin-top: 64px;
  padding-top: 0;
}

.nk-head {
  margin-bottom: 24px;
}

.nk-kicker {
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 0 0 12px;
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--vp-c-brand-1);
}

.nk-kicker::after {
  content: "";
  flex: 1;
  height: 1px;
  background: var(--vp-c-divider);
}

.nk-head h2 {
  margin: 0 0 8px;
  font-size: 28px;
  font-weight: 600;
  letter-spacing: -0.03em;
  line-height: 1.2;
}

.nk-lede {
  margin: 0;
  max-width: 38em;
  font-size: 15px;
  line-height: 1.6;
  color: var(--vp-c-text-2);
}

.nk-bento,
.nk-panels {
  display: grid;
  gap: 12px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.nk-bento {
  grid-template-columns: 1fr;
}

.nk-panels {
  grid-template-columns: 1fr;
}

@media (min-width: 640px) {
  .nk-bento {
    grid-template-columns: 1fr 1fr;
  }

  .nk-cell--lead,
  .nk-cell--wide {
    grid-column: span 2;
  }

  .nk-panels {
    grid-template-columns: 1fr 1fr;
  }
}

@media (min-width: 960px) {
  .nk-bento {
    grid-template-columns: repeat(3, 1fr);
  }
}

.nk-tile,
.nk-panel {
  position: relative;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  height: 100%;
  text-decoration: none;
  color: inherit;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg);
  transition:
    border-color 0.2s ease,
    background-color 0.2s ease;
}

.nk-tile {
  flex-direction: column;
  gap: 12px;
  padding: 20px;
  min-height: 148px;
}

.nk-cell--lead .nk-tile {
  min-height: 220px;
  padding: 28px;
}

.nk-tile:hover,
.nk-panel:hover {
  border-color: var(--vp-c-brand-1);
}

.nk-tile:focus-visible,
.nk-panel:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}

.nk-tile__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 36px;
  height: 36px;
  border-radius: 8px;
  color: var(--vp-c-brand-1);
  background: var(--vp-c-brand-soft);
}

.nk-tile__head {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.nk-tile__title,
.nk-panel__title {
  font-size: 16px;
  font-weight: 600;
  line-height: 1.35;
  letter-spacing: -0.02em;
}

.nk-cell--lead .nk-tile__title {
  font-size: 22px;
}

.nk-tile__details,
.nk-panel__details {
  font-size: 14px;
  font-weight: 400;
  line-height: 1.55;
  color: var(--vp-c-text-2);
}

.nk-tile--static {
  min-height: auto;
  pointer-events: none;
}

.nk-tile--static .nk-tile__title,
.nk-tile--static .nk-tile__details {
  margin: 0;
}

.nk-panel {
  align-items: flex-start;
  gap: 12px;
  min-height: 240px;
  padding: 28px;
}

.nk-panel__top {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
  min-width: 0;
}

.nk-panel__title {
  flex: 1;
  min-width: 0;
  font-size: 22px;
}

.nk-badge {
  flex-shrink: 0;
  margin-left: auto;
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
}

.nk-go {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-top: auto;
  padding-top: 8px;
  font-size: 13px;
  font-weight: 500;
  line-height: 1;
  color: var(--vp-c-brand-1);
}

.nk-go span {
  transition: transform 0.2s ease;
}

.nk-tile:hover .nk-go span,
.nk-panel:hover .nk-go span {
  transform: translateX(4px);
}

@media (prefers-reduced-motion: reduce) {
  .nk-tile,
  .nk-panel,
  .nk-go span {
    transition: none;
  }

  .nk-tile:hover .nk-go span,
  .nk-panel:hover .nk-go span {
    transform: none;
  }
}
</style>
