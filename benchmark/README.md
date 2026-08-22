# Benchmarks

Throughput, latency, memory density, and multi-core scaling for the nodeakt messaging primitives, measured end to end: a run never stops the clock at the last send, it stops when the last message has actually been processed.

## How to run

```sh
# The whole suite, one file at a time so nothing competes for the machine.
pnpm bench

# One file.
pnpm exec vitest run --config vitest.bench.config.ts benchmark/baseline.bench.ts
```

Run through `pnpm bench` (or the command above): the config passes `--expose-gc`, which the allocation estimates need, and disables file parallelism so the numbers measure the runtime rather than sibling benchmark processes.

Every report starts with the machine it was measured on (CPU, core topology, memory, Node and V8 versions). Throughput numbers mean nothing without that context; when quoting a number, quote the machine line with it.

## The suite

| File                 | What it measures                                                                                                                      |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `baseline.bench.ts`  | The plain numbers: one sender, one receiver, messages per second for `tell` and `ask`.                                                |
| `tell.bench.ts`      | Fire-and-forget throughput across shapes: single pair, fan-in, independent pairs, and a sweep over every mailbox implementation.      |
| `ask.bench.ts`       | Request/response round trips: sequential, pipelined with thousands in flight, and concurrent askers sharing one receiver.             |
| `density.bench.ts`   | Resident heap per idle actor: the memory shape of holding very many light actors, not a throughput number.                            |
| `multicore.bench.ts` | The worker pool: CPU-bound scaling across isolates, the price of a cross-isolate message, and cumulative machine capacity per method. |
| `harness.ts`         | Shared measurement and reporting: warmup, timed samples, allocation estimates under forced GC, GC observation, and the table printer. |

## Baseline numbers

Apple M1 (4P + 4E, 16 GB), Node v24.19.0, darwin 25.5.0:

| primitive | time   | messages processed | msgs/sec |
|-----------|--------|--------------------|----------|
| `tell`    | 5.00 s | 119,500,000        | ~23.9M   |
| `ask`     | 5.01 s | 27,500,000         | ~5.5M    |

Methodology: each primitive runs continuously for at least five seconds and the rate is the messages fully processed divided by the wall time spent, so garbage collection and buffer churn are inside the measurement, not averaged away. `tell` counts a message only once the receiver has handled it, sent in batches of 100,000 with each batch fully processed before the next begins. `ask` round trips are sequential, each reply awaited before the next request. One second of unmeasured warmup precedes each clock so the JIT and mailbox buffers are at steady state.

Numbers age: regenerate them on your machine with the command above rather than trusting this table across runtime versions.

## Reading the numbers

**Sustained, not extrapolated.** The baseline's rate is measured across five full seconds of continuous work; it is what the runtime actually sustains, not a short burst divided into one second. That it lands where the tell suite's sampled single-pair rate lands is the two methodologies confirming each other. Burst shape still matters at the extremes: one contiguous burst of a million-plus messages holds every queued delivery live at once and pays cache-miss and GC costs that batches of 100,000 do not.

**Sequential ask is latency-bound.** One awaited round trip per iteration measures the full request/reply/timeout-bookkeeping path with nothing else in flight. Throughput-shaped ask workloads (many requests in flight) are measured separately in `ask.bench.ts` and are considerably faster.

**Spread is the noise band.** The suite reports the relative standard deviation of its sampled operations. A delta inside the spread is noise, not a regression; a real regression moves the mean well outside it.
