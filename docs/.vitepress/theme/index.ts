/// <reference path="../env.d.ts" />
import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme";
import "./custom.css";
import Layout from "./Layout.vue";

// The custom Layout adds the home page's feature bands; diagrams are inline SVGs
// in the markdown pages themselves, so the theme wires no diagram renderer.
export default {
  extends: DefaultTheme,
  Layout,
} satisfies Theme;
