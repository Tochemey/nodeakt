---
layout: home

hero:
  name: NodeAkt
  text: Actor framework for Node.js, Bun, and Deno
  tagline: "Typed actors, supervision, mailboxes, behaviors, an event stream, and a multi-core runtime. No locks, just messages."
  image:
    src: /logo.svg
    alt: NodeAkt
  actions:
    - theme: brand
      text: Introduction
      link: /guide/
    - theme: alt
      text: Reference
      link: /actor-system/
    - theme: alt
      text: GitHub
      link: https://github.com/Tochemey/nodeakt

features:
  - title: One message at a time
    details: An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock.
  - title: Typed messages
    details: Messages are classes narrowed with instanceof. tell is fire-and-forget; ask waits for a reply.
  - title: Supervision
    details: Stop, resume, restart, or escalate on failure. One-for-one or one-for-all, restart budgets, exponential backoff.
  - title: Behaviors and stash
    details: become and becomeStacked swap the handler at runtime; the stash replays deferred messages after a switch.
  - title: Mailboxes
    details: Unbounded and bounded FIFO, segmented, fair per-sender, and priority. Or implement your own.
  - title: Multi-core
    details: Spawn with Props and the runtime places actors across every core. Same PID API locally and across isolates.
---
