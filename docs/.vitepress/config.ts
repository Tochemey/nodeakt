import { defineConfig } from "vitepress";

// The site is served from GitHub Pages under /nodeakt/.
export default defineConfig({
  title: "nodeakt",
  description:
    "Actor framework for Node.js: typed actors, supervision, mailboxes, behaviors, an event stream, and a multi-core runtime",
  base: "/nodeakt/",
  cleanUrls: true,
  lastUpdated: true,
  sitemap: { hostname: "https://tochemey.github.io/nodeakt/" },
  head: [["link", { rel: "icon", type: "image/svg+xml", href: "/nodeakt/logo.svg" }]],
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
          { text: "Getting started", link: "/guide/" },
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
          { text: "Messaging", link: "/actor/messaging" },
          { text: "Behaviors and stash", link: "/actor/behaviors" },
          { text: "Hierarchy, watch, and stop", link: "/actor/hierarchy" },
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
