import { defineConfig } from "vitepress";

// The site is served from GitHub Pages under /nodeakt/.
// Mermaid diagrams are rendered client-side by the theme (see theme/index.ts),
// which imports mermaid at runtime, so no build-time plugin is needed.
export default defineConfig({
  title: "NodeAkt",
  description:
    "Actor framework for Node, Deno and Bun: typed actors, supervision, mailboxes, behaviors, an event stream, and a multi-core runtime",
  base: "/nodeakt/",
  cleanUrls: true,
  lastUpdated: true,
  sitemap: { hostname: "https://tochemey.github.io/nodeakt/" },
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: "/nodeakt/logo.svg" }],
    ["link", { rel: "preconnect", href: "https://fonts.googleapis.com" }],
    ["link", { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: "" }],
    [
      "link",
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap",
      },
    ],
    // Point VitePress's font variables at Geist. `!important` on the custom
    // property definitions makes them win over the theme defaults regardless
    // of stylesheet order.
    [
      "style",
      {},
      ':root{--vp-font-family-base:"Geist",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif!important;--vp-font-family-mono:"Geist Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace!important}',
    ],
  ],
  themeConfig: {
    logo: "/logo.svg",
    nav: [
      { text: "Guide", link: "/guide/" },
      { text: "Reference", link: "/actor-system/" },
    ],
    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Introduction", link: "/guide/" },
          { text: "Tour", link: "/guide/tour" },
        ],
      },
      {
        text: "Actor system",
        items: [
          { text: "Overview", link: "/actor-system/" },
          { text: "Logging", link: "/actor-system/logging" },
          { text: "Events and dead letters", link: "/actor-system/events" },
        ],
      },
      {
        text: "Actors",
        items: [
          { text: "Overview", link: "/actor/" },
          { text: "Spawning", link: "/actor/spawning" },
          { text: "Lifecycle", link: "/actor/lifecycle" },
          { text: "Messaging", link: "/actor/messaging" },
          { text: "PipeTo", link: "/actor/pipeto" },
          { text: "Scheduling", link: "/actor/scheduling" },
          { text: "Behaviors and stash", link: "/actor/behaviors" },
          { text: "Hierarchy and stop", link: "/actor/hierarchy" },
          { text: "Death watch", link: "/actor/death-watch" },
          { text: "Supervision", link: "/actor/supervision" },
          { text: "Mailboxes", link: "/actor/mailboxes" },
          { text: "Passivation", link: "/actor/passivation" },
          { text: "Reentrancy", link: "/actor/reentrancy" },
        ],
      },
      {
        text: "Multi-core",
        items: [{ text: "Overview", link: "/multi-core/" }],
      },
      {
        text: "Appendix",
        items: [{ text: "Errors", link: "/errors" }],
      },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/Tochemey/nodeakt" }],
    search: { provider: "local" },
    outline: { level: [2, 3] },
    editLink: {
      pattern: "https://github.com/Tochemey/nodeakt/edit/main/docs/:path",
      text: "Edit this page on GitHub",
    },
    footer: {
      message: "Released under the MIT License.",
    },
  },
});
