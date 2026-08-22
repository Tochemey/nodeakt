import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme";
import { MermaidRenderer } from "vitepress-mermaid-renderer";

// Render ```mermaid fenced blocks client-side. The renderer imports mermaid
// at runtime and self-injects its styles, so no build-time plugin or CSS
// import is needed. Guarded to the browser: enhanceApp also runs during the
// SSR build, where there is no DOM to observe.
export default {
  extends: DefaultTheme,
  enhanceApp() {
    if (typeof window !== "undefined") {
      MermaidRenderer.getInstance();
    }
  },
} satisfies Theme;
