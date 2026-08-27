#!/usr/bin/env bash
# Boots the three-node DNS cluster with Docker Compose and drives every use case
# the example demonstrates, asserting each one, then tears the cluster down. It
# locates its own compose file, so it can be run from anywhere:
#
#   examples/dns-cluster/usecases.sh
#
# Use cases exercised, in order:
#   1. Formation: all three nodes join and agree on one coordinator.
#   2. Distributed placement: a value written through one node is read through another.
#   3. Cluster-wide scan: one node sees every node's keys.
#   4. Unique claim: a conditional write lets only one node claim a key.
#   5. Graceful leave: a stopped node hands its partitions to the survivors, which keep serving.
#   6. Rejoin: the departed node comes back and membership grows to three again.
#   7. Crash recovery: an abruptly killed node's keys stay readable from a promoted backup.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
compose_file="$here/docker-compose.yml"

# The published host ports, one per replica, from docker-compose.yml.
port1=3001
port2=3002
port3=3003

passed=0
failed=0

compose() {
  docker compose -f "$compose_file" "$@"
}

trap 'echo; echo "tearing down"; compose down --remove-orphans >/dev/null 2>&1 || true' EXIT

pass() {
  passed=$((passed + 1))
  printf '  \033[32mPASS\033[0m %s\n' "$1"
}

fail() {
  failed=$((failed + 1))
  printf '  \033[31mFAIL\033[0m %s\n' "$1"
}

# Assert that the actual text contains the needle, recording a pass or a fail.
expect_contains() {
  local label="$1" actual="$2" needle="$3"
  case "$actual" in
    *"$needle"*) pass "$label" ;;
    *) fail "$label (expected to contain '$needle', got: $actual)" ;;
  esac
}

get() {
  curl -s "localhost:$1$2"
}

put() {
  curl -s -X PUT "localhost:$1$2"
}

# Count the "host:port" members a node reports on /members.
members_count() {
  get "$1" /members | grep -o '"[^"]*:8080"' | wc -l | tr -d ' '
}

# Poll a node's /members until it reports the wanted count, or time out.
wait_members() {
  local portv="$1" want="$2" deadline=$((SECONDS + ${3:-60}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(members_count "$portv")" = "$want" ]; then
      return 0
    fi

    sleep 1
  done

  return 1
}

# Wait until a node's HTTP is up and it reports it has joined. Docker publishes the
# host port before the in-container listener is up, so early connections are reset
# rather than refused; --retry-all-errors retries through both.
wait_ready() {
  curl -s --retry 120 --retry-delay 1 --retry-all-errors "localhost:$1/health" >/dev/null
}

echo "building and starting the three-node cluster"
compose up -d --build

echo "waiting for the cluster to form"
wait_ready "$port1"
wait_ready "$port2"
wait_ready "$port3"

echo
echo "1. formation"
h1="$(get "$port1" /health)"
h2="$(get "$port2" /health)"
h3="$(get "$port3" /health)"
expect_contains "node1 joined" "$h1" '"joined":true'
expect_contains "node2 joined" "$h2" '"joined":true'
expect_contains "node3 joined" "$h3" '"joined":true'
coordinator="$(printf '%s' "$h1" | grep -o '"coordinator":"[^"]*"')"
expect_contains "node2 agrees on the coordinator" "$h2" "$coordinator"
expect_contains "node3 agrees on the coordinator" "$h3" "$coordinator"

echo
echo "2. distributed placement (write on node1, read on node3)"
put "$port1" '/kv?key=color&value=blue' >/dev/null
read_back="$(get "$port3" '/kv?key=color')"
expect_contains "node3 reads node1's write" "$read_back" '"value":"blue"'

echo
echo "3. cluster-wide scan (from node2)"
scan_deadline=$((SECONDS + 30))
keys=""
while [ "$SECONDS" -lt "$scan_deadline" ]; do
  keys="$(get "$port2" /keys)"
  if printf '%s' "$keys" | grep -q 'heartbeat:node1' &&
    printf '%s' "$keys" | grep -q 'heartbeat:node2' &&
    printf '%s' "$keys" | grep -q 'heartbeat:node3'; then
    break
  fi

  sleep 1
done
expect_contains "scan sees the distributed value" "$keys" '"key":"color"'
expect_contains "scan sees node1's heartbeat" "$keys" 'heartbeat:node1'
expect_contains "scan sees node2's heartbeat" "$keys" 'heartbeat:node2'
expect_contains "scan sees node3's heartbeat" "$keys" 'heartbeat:node3'

echo
echo "4. unique claim (only one node wins the key)"
claim1="$(put "$port1" '/kv?key=leader&value=node1&unique=1')"
claim2="$(put "$port2" '/kv?key=leader&value=node2&unique=1')"
winner="$(get "$port3" '/kv?key=leader')"
expect_contains "node1's claim applies" "$claim1" '"applied":true'
expect_contains "node2's claim is refused" "$claim2" '"applied":false'
expect_contains "the winner is node1 everywhere" "$winner" '"value":"node1"'

echo
echo "5. graceful leave (node1 leaves; survivors keep serving)"
compose stop node1 >/dev/null
if wait_members "$port2" 2 60; then
  pass "membership drops to two"
else
  fail "membership drops to two (still $(members_count "$port2"))"
fi

survivor_read="$(get "$port2" '/kv?key=color')"
expect_contains "a survivor still serves the value" "$survivor_read" '"value":"blue"'

echo
echo "6. rejoin (node1 comes back)"
compose start node1 >/dev/null
wait_ready "$port1"
if wait_members "$port2" 3 60; then
  pass "membership grows back to three"
else
  fail "membership grows back to three (still $(members_count "$port2"))"
fi

echo
echo "7. crash recovery (node2 killed; a backup is promoted)"
node2_container="$(compose ps -q node2)"
docker rm -f "$node2_container" >/dev/null
if wait_members "$port1" 2 60; then
  pass "survivors detect the crash"
else
  fail "survivors detect the crash (still $(members_count "$port1"))"
fi

recovered_read="$(get "$port1" '/kv?key=color')"
expect_contains "the killed node's value survives on a backup" "$recovered_read" '"value":"blue"'

echo
echo "$passed passed, $failed failed"
if [ "$failed" -ne 0 ]; then
  exit 1
fi
