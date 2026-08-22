# Scheduling

Actors constantly need a message later: a heartbeat, a retry after a delay, a periodic cleanup tick, a reminder to a peer. Holding a `setTimeout` or `setInterval` inside an actor works, but every author rewrites the same pattern: cancel it in `postStop` by hand, and take care not to fire into an actor that has already stopped. The scheduler is that pattern, built in.

```ts
class Reminder implements Actor {
  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof StartReminder) {
      void ctx.scheduleOnce(new Remind(), ctx.self as PID, ctx.message.delay, { reference: "remind" });
    }

    if (ctx.message instanceof Cancel) {
      void ctx.cancelSchedule("remind");
    }
  }
}
```

## API

The same six calls live on [`ActorSystem`](../actor-system/index.md) and on [`ReceiveContext`](messaging.md), because the most common scheduler user is an actor arranging its own future. All of them are asynchronous and reject with the sentinels listed under [Errors](#errors).

| Call                                       | Effect                                                                                                                  |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `schedule(message, pid, interval, opts?)`  | Deliver `message` to `pid` every `interval` milliseconds, the first time one interval from now.                         |
| `scheduleOnce(message, pid, delay, opts?)` | Deliver `message` to `pid` once, `delay` milliseconds from now.                                                         |
| `cancelSchedule(reference)`                | Cancel the schedule held under `reference`.                                                                             |
| `pauseSchedule(reference)`                 | Stop firing until resumed. Pausing a paused schedule is a no-op.                                                        |
| `resumeSchedule(reference)`                | Fire again. A resumed one-shot keeps the delay it had left; a resumed repeating schedule fires one full interval later. |

Cron-style schedules do not exist yet; `schedule` with an interval covers periodic delivery.

## Options

The options argument is a `ScheduleOptions` with two optional fields:

- `reference`: the identifier used to cancel, pause, and resume the schedule. References live in one flat, system-wide namespace: a schedule created inside an actor is still addressable by its reference from anywhere. A schedule registered without a reference cannot be addressed individually; it is still cancelled when its owner stops.
- `sender`: the actor recorded as the sender of every delivered message, seen by the receiving behavior as `ctx.sender`. Defaults to the scheduling actor for a schedule created through a receive context, and to the system's NoSender actor otherwise.

## Ownership

Schedules are owned by the system and die with it: `system.stop()` cancels everything, so no timer leaks.

A schedule created through `ReceiveContext` is additionally owned by the scheduling actor: when that actor fully stops, its schedules are cancelled with it. This replaces the `postStop` timer cleanup that is otherwise easy to forget. A [restart](supervision.md) is not a stop and keeps the schedule running.

## Delivery semantics

Every tick goes through the normal send path, which settles the edge cases the way ordinary sends already behave:

- A tick to an actor that has stopped by fire time becomes a [dead letter](../actor-system/events.md), like any other undeliverable send. A repeating schedule whose target has fully stopped is dropped after that tick, so it does not dead-letter forever; a target that is merely suspended keeps its schedule, because it may be [reinstated](supervision.md).
- A delivery counts as activity, so a repeating schedule keeps a time-based [passivation](passivation.md) target alive indefinitely. Cancel the schedule, or give the actor a different strategy, if it should still passivate.
- Every tick is an independent send: nothing suppresses a tick because the previous message is still queued. An actor that needs at-most-one-in-flight enforces that in its own state.
- A tick to an actor placed on another core routes like any other [cross-isolate send](../multi-core/index.md). The schedule lives on the isolate that created it.

Scheduling never reads the clock on the message hot path: schedules live in a deadline-ordered heap behind one shared timer, the same design the passivation scheduler uses, so a large number of schedules stays cheap.

> [!NOTE]
> The shared timer is unreferenced: pending schedules never keep the process alive on their own. A program whose only remaining work is a scheduled message will exit; keep the process alive with whatever it is actually serving, as any running worker pool or server already does.

## Errors

| Sentinel                   | Arises when                                                                                                      |
|----------------------------|------------------------------------------------------------------------------------------------------------------|
| `ErrActorSystemNotStarted` | Any scheduling call on a system that is not running.                                                             |
| `ErrInvalidInterval`       | A delay or interval that is not a positive number of milliseconds.                                               |
| `ErrScheduleAlreadyExists` | Registering a schedule under a reference that is already held. Cancel the existing schedule first to replace it. |
| `ErrScheduleNotFound`      | `cancelSchedule`, `pauseSchedule`, or `resumeSchedule` with a reference no schedule holds.                       |
| `ErrDead`                  | Registering a schedule whose target has already stopped.                                                         |
