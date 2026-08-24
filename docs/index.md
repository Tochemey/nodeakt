---
layout: home

hero:
  name: NodeAkt
  text: Actor framework for Node, Bun, and Deno
  tagline: "Typed actors, supervision, behaviors, a multi-core runtime, and remoting across nodes. Zero dependencies. No locks, just messages."
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
    link: /actor/
  - title: Typed messages
    details: Messages are classes narrowed with instanceof. tell is fire-and-forget; ask waits for a reply.
    link: /actor/messaging
  - title: Supervision
    details: Stop, resume, restart, or escalate on failure. One-for-one or one-for-all, restart budgets, exponential backoff.
    link: /actor/supervision
  - title: Behaviors and stash
    details: become and becomeStacked swap the handler at runtime; the stash replays deferred messages after a switch.
    link: /actor/behaviors
  - title: Mailboxes
    details: Unbounded and bounded FIFO, segmented, fair per-sender, and priority. Or implement your own.
    link: /actor/mailboxes
  - title: Pipe, request, schedule
    details: pipeTo turns a promise's result into a message, reentrant requests keep the mailbox moving while a reply is in flight, and schedules deliver on a delay or an interval.
    link: /actor/pipeto
  - title: Multi-core
    details: Spawn with Props and the runtime places actors across every core. Same PID API locally and across isolates.
    link: /multi-core/
  - title: Remoting
    details: Look up, spawn, watch, and message actors on another machine over TCP. The same PID API across nodes, tell to death watch, failures included.
    link: /remoting/
  - title: Zero dependencies
    details: The runtime, the multi-core layer, and the wire protocol are built on the standard library alone. npm install brings exactly one package.
---
