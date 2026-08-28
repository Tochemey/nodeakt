---
layout: home

hero:
  name: NodeAkt
  text: Distributed Actor framework for TypeScript
  tagline: "Typed actors, supervision, behaviors, a multi-core runtime, remoting, and clustering across nodes. Zero dependencies. No locks, just messages."
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

runtime:
  kicker: Runtime
  title: Actors, on one machine.
  details: An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock.
  items:
    - title: One message at a time
      icon: actor
      details: An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock.
      link: /actor/
    - title: Typed messages
      icon: typed
      details: Messages are classes narrowed with instanceof. tell is fire-and-forget; ask waits for a reply.
      link: /actor/messaging
    - title: Supervision
      icon: supervision
      details: Stop, resume, restart, or escalate on failure. One-for-one or one-for-all, restart budgets, exponential backoff.
      link: /actor/supervision
    - title: Behaviors and stash
      icon: behaviors
      details: become and becomeStacked swap the handler at runtime; the stash replays deferred messages after a switch.
      link: /actor/behaviors
    - title: Mailboxes
      icon: mailbox
      details: Unbounded and bounded FIFO, segmented, fair per-sender, and priority. Or implement your own.
      link: /actor/mailboxes
    - title: Pipe, request, schedule
      icon: pipe
      wide: true
      details: pipeTo turns a promise's result into a message, reentrant requests keep the mailbox moving while a reply is in flight, and schedules deliver on a delay or an interval.
      link: /actor/pipeto
    - title: Multi-core
      icon: cores
      details: Spawn with Props and the runtime places actors across every core. Same PID API locally and across isolates.
      link: /multi-core/

networking:
  kicker: Networking
  details: Look up, spawn, and message actors on another node. TLS encrypts the carrier when you need it; enabling it hinders performance versus plaintext.
  items:
    - title: Remoting
      icon: tcp
      badge: TCP
      details: Look up, spawn, watch, and message actors on another node. Tell, ask, request, forward, pipeTo, and death watch all cross the wire, failures included.
      link: /remoting/
    - title: TLS
      icon: tls
      badge: TLS
      details: Encrypt remoting with a cert, key, and optional CA. Mutual TLS when you ask for it. All or nothing per system; the protocol is unchanged over the encrypted carrier.
      link: /remoting/tls

clustering:
  kicker: Clustering
  title: Actors, across many machines.
  details: Turn a set of nodes into one cluster. Spawn, address, and message actors by name across the cluster, and have a departed node's actors recreated on the survivors automatically.
  items:
    - title: Clustering
      icon: cluster
      wide: true
      details: Enable it with a discovery provider and a remote endpoint. Membership, a distributed name registry, and remoting join into one cluster of location-transparent actors, on the same PID API.
      link: /clustering/
    - title: Discovery and membership
      icon: discovery
      details: Find seed peers over DNS or a static list at boot, then track who is in the cluster with gossip and failure detection. A quorum guards against split brain.
      link: /clustering/discovery
    - title: Placement
      icon: placement
      details: spawn keeps an actor local; spawnOn places it on the node a strategy chooses. Top-level names are unique cluster-wide.
      link: /clustering/placement
    - title: Singletons
      icon: actor
      details: spawnSingleton gives a name exactly one live instance cluster-wide, pinned to the coordinator and idempotent to create.
      link: /clustering/singletons
    - title: Messaging
      icon: tcp
      details: actorOf and actorOfAsync return a PID that routes to the owning node, wherever it runs. tell, ask, and watch keep their call sites.
      link: /clustering/messaging
    - title: Relocation
      icon: relocation
      details: When a node departs, gracefully or by crash, the coordinator recreates its relocatable actors on the survivors from their recipes.
      link: /clustering/relocation

deps:
  title: Zero dependencies
  details: The whole library is built on the standard library alone. npm install brings exactly one package.
---
