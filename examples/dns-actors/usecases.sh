#!/usr/bin/env bash
#
# Boots the three-node distributed-actor cluster with Docker Compose and asserts every
# use case against its HTTP API: cluster formation, a distributed spawn reached across
# nodes, strategy placement, a singleton, and relocation after a hard kill. Exits
# non-zero on the first failed assertion and always tears the cluster down.

set -euo pipefail

cd "$(dirname "$0")"

compose() { docker compose "$@"; }

cleanup() { compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }
pass() { echo "PASS: $1"; }

# One node's HTTP endpoint on the host, published as 3001/3002/3003.
url() { echo "http://localhost:300$1"; }

# Retries a curl until its output matches a pattern or the attempts run out.
await() {
  local endpoint="$1" pattern="$2" tries="${3:-30}" body=""
  for _ in $(seq 1 "$tries"); do
    body="$(curl -s --max-time 3 "$endpoint" 2>/dev/null || true)"
    if [[ "$body" == *"$pattern"* ]]; then
      echo "$body"
      return 0
    fi
    sleep 2
  done
  echo "$body"
  return 1
}

echo "booting the cluster..."
compose up --build -d >/dev/null

# Formation: every node sees all three members.
await "$(url 1)/health" '"members":3' 40 >/dev/null || fail "cluster did not form"
for n in 1 2 3; do
  await "$(url $n)/health" '"members":3' 5 >/dev/null || fail "node$n did not see 3 members"
done
pass "three nodes formed the cluster"

# Distributed spawn: place on node1, reach it by name from node2 and node3 in one call
# each (actorOfAsync resolves across nodes without a cold miss).
curl -s -X PUT "$(url 1)/workers/orders?region=eu" >/dev/null
[[ "$(curl -s "$(url 2)/where/orders")" == *'"host":"node1"'* ]] || fail "node2 could not locate orders"
[[ "$(curl -s "$(url 3)/greet/orders?who=ada")" == *'"greeting":"eu:ada"'* ]] || fail "node3 could not greet orders"
pass "a distributed actor is reached by name from every node"

# Strategy placement spreads workers across the cluster.
for name in alpha beta gamma; do curl -s -X PUT "$(url 1)/spread/$name" >/dev/null; done
hosts="$(for name in alpha beta gamma; do curl -s "$(url 2)/where/$name"; done)"
[[ "$hosts" == *node1* && "$hosts" == *node2* && "$hosts" == *node3* ]] || fail "spread did not use every node ($hosts)"
pass "spawnOn spread workers across all nodes"

# A singleton is the same instance from every node.
curl -s -X PUT "$(url 1)/singletons/sequencer?region=eu" >/dev/null
where2="$(curl -s "$(url 2)/where/sequencer")"
where3="$(curl -s "$(url 3)/where/sequencer")"
[[ "$where2" == "$where3" && "$where2" == *'"host":"node'* ]] || fail "singleton differed across nodes ($where2 vs $where3)"
pass "a singleton is reached by the same name from every node"

# Relocation after a crash: SIGKILL the node hosting orders so it dies with no graceful
# leave, its restart disabled first so it stays down. The survivors detect the failure
# and the coordinator recreates orders on one of them, its region carried in the recipe.
victim="$(curl -s "$(url 2)/where/orders" | sed 's/.*"host":"\([^"]*\)".*/\1/')"
echo "hard-killing $victim (hosts orders)..."
cid="$(compose ps -q "$victim")"
docker update --restart=no "$cid" >/dev/null
docker kill --signal=KILL "$cid" >/dev/null
survivor=2; [[ "$victim" == node2 ]] && survivor=1
await "$(url $survivor)/where/orders" '"host":"node' 40 | grep -qv "\"host\":\"$victim\"" || fail "orders was not relocated off $victim"
[[ "$(curl -s "$(url $survivor)/greet/orders?who=ada")" == *'"greeting":"eu:ada"'* ]] || fail "relocated orders lost its region"
pass "a crashed node's actor was recreated on a survivor with its configuration intact"

echo "all use cases passed"
