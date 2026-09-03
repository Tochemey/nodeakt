import { defineConfig } from "vitepress";

// The site is served from GitHub Pages under /nodeakt/.
// Diagrams are hand-authored inline SVGs in the markdown pages, colored with the
// theme's CSS variables (falling back to a light palette outside the site), so no
// diagram library or renderer is involved.
export default defineConfig({
  title: "NodeAkt",
  description:
    "Zero-dependency distributed actor framework for TypeScript: typed actors, supervision, mailboxes, behaviors, an event stream, a multi-core runtime, remoting across nodes, and clustering with discovery, placement, singletons, and relocation",
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
        text: "🚀 Guide",
        items: [
          { text: "Introduction", link: "/guide/" },
          { text: "Tour", link: "/guide/tour" },
        ],
      },
      {
        text: "⚙️ Actor system",
        items: [
          { text: "Overview", link: "/actor-system/" },
          { text: "Logging", link: "/actor-system/logging" },
          { text: "Events and dead letters", link: "/actor-system/events" },
          { text: "Extensions", link: "/actor-system/extensions" },
          { text: "Metrics", link: "/actor-system/metrics" },
        ],
      },
      {
        text: "👥 Actors",
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
          { text: "Routers", link: "/actor/routers" },
          { text: "Mailboxes", link: "/actor/mailboxes" },
          { text: "Passivation", link: "/actor/passivation" },
          { text: "Reentrancy", link: "/actor/reentrancy" },
        ],
      },
      {
        text: "⚡ Multi-core",
        items: [{ text: "Overview", link: "/multi-core/" }],
      },
      {
        text: "📡 Remoting",
        items: [
          { text: "Overview", link: "/remoting/" },
          { text: "TLS", link: "/remoting/tls" },
        ],
      },
      {
        text: "🌐 Clustering",
        items: [
          { text: "Overview", link: "/clustering/" },
          { text: "Discovery", link: "/clustering/discovery" },
          { text: "Membership", link: "/clustering/membership" },
          { text: "Placement", link: "/clustering/placement" },
          { text: "Singletons", link: "/clustering/singletons" },
          { text: "Messaging", link: "/clustering/messaging" },
          { text: "Relocation", link: "/clustering/relocation" },
          { text: "Events", link: "/clustering/events" },
        ],
      },
      {
        text: "📖 Appendix",
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
