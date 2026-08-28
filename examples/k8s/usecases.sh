#!/usr/bin/env bash
#
# Boots the distributed-actor cluster on a local kind Kubernetes cluster and asserts
# every scenario against its HTTP API: formation, a distributed spawn reached across
# pods, strategy placement, a singleton, elastic scaling, and recreation after a pod
# is hard-killed. Exits non-zero on the first failed assertion.
#
# Without arguments it provisions everything itself (kind cluster, image, deployment)
# and always tears down what it created. With --deployed it asserts against a cluster
# already rolled out on the current kubectl context and leaves it running; the drill
# actors it spawns carry a per-run suffix, so it can run repeatedly.

set -euo pipefail

cd "$(dirname "$0")"

CLUSTER_NAME="${CLUSTER_NAME:-nodeakt-k8s}"
IMAGE="${IMAGE:-nodeakt-k8s:dev}"

DEPLOYED=0
[[ "${1:-}" == "--deployed" ]] && DEPLOYED=1

# Provisioning pins every kubectl call to the kind cluster's context, so a current
# context that points somewhere else is never touched; --deployed uses it as-is.
KUBECTL=(kubectl)
[[ "$DEPLOYED" == 0 ]] && KUBECTL=(kubectl --context "kind-$CLUSTER_NAME")

created_cluster=0
forward_pids=()

cleanup() {
  local pid
  for pid in "${forward_pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  done

  if [[ "$DEPLOYED" == 1 ]]; then return; fi

  "${KUBECTL[@]}" delete -f deploy/k8s.yaml --ignore-not-found >/dev/null 2>&1 || true
  if [[ "$created_cluster" == 1 ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# On a failed assertion, the last lines of every pod's log go to stderr first, so
# the run leaves enough behind to diagnose.
fail() {
  echo "FAIL: $1" >&2
  local i
  for i in 0 1 2; do
    echo "--- nodeakt-$i log tail ---" >&2
    "${KUBECTL[@]}" logs "pod/nodeakt-$i" --tail=30 >&2 2>/dev/null || true
  done
  exit 1
}

pass() { echo "PASS: $1"; }

# Pod i's HTTP endpoint, port-forwarded to localhost as 3100 + i.
url() { echo "http://localhost:$((3100 + $1))"; }

# A pod's current IP, which is the host its member and its actors report.
pod_ip() { "${KUBECTL[@]}" get "pod/$1" -o jsonpath='{.status.podIP}'; }

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

if [[ "$DEPLOYED" == 0 ]]; then
  if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    echo "creating kind cluster $CLUSTER_NAME..."
    kind create cluster --name "$CLUSTER_NAME" --config deploy/kind-config.yaml --wait 2m >/dev/null
    created_cluster=1
  fi

  echo "building the image (a few minutes on the first run)..."
  docker build -q -f Dockerfile -t "$IMAGE" ../.. >/dev/null
  kind load docker-image "$IMAGE" --name "$CLUSTER_NAME" >/dev/null 2>&1

  echo "deploying..."
  "${KUBECTL[@]}" apply -f deploy/k8s.yaml >/dev/null
  "${KUBECTL[@]}" rollout status statefulset/nodeakt --timeout=300s >/dev/null
fi

# One port-forward per pod, so each assertion picks which pod it talks to.
forward() {
  "${KUBECTL[@]}" port-forward "pod/nodeakt-$1" "$((3100 + $1)):3000" >/dev/null 2>&1 &
  forward_pids+=("$!")
}

for i in 0 1 2; do forward "$i"; done

# The drill actors carry a per-run suffix so a rerun against a living deployment
# never collides with the cluster-wide unique names of an earlier one.
RUN="r$RANDOM"

# Formation: every pod sees all three members.
for i in 0 1 2; do
  await "$(url $i)/health" '"members":3' 40 >/dev/null || fail "pod $i did not see 3 members"
done
pass "three pods formed the cluster"

# Distributed spawn: place on pod 0, then reach it by name from pods 1 and 2. The
# first ask across a pod pair also opens that pair's remoting connection, so the
# lookups retry briefly rather than demand a cold hit.
curl -s -X PUT "$(url 0)/workers/orders-$RUN?region=eu" >/dev/null
await "$(url 1)/where/orders-$RUN" "\"host\":\"$(pod_ip nodeakt-0)\"" 10 >/dev/null || fail "pod 1 could not locate orders-$RUN"
await "$(url 2)/greet/orders-$RUN?who=ada" '"greeting":"eu:ada"' 10 >/dev/null || fail "pod 2 could not greet orders-$RUN"
pass "a distributed actor is reached by name from every pod"

# Strategy placement spreads workers across the cluster.
for name in alpha beta gamma; do curl -s -X PUT "$(url 0)/spread/$name-$RUN" >/dev/null; done
for name in alpha beta gamma; do
  await "$(url 1)/where/$name-$RUN" '"host":"' 10 >/dev/null || fail "pod 1 could not locate $name-$RUN"
done
hosts="$(for name in alpha beta gamma; do curl -s "$(url 1)/where/$name-$RUN"; done)"
for i in 0 1 2; do
  if [[ "$hosts" != *"$(pod_ip "nodeakt-$i")"* ]]; then fail "spread did not use nodeakt-$i ($hosts)"; fi
done
pass "spawnOn spread workers across all pods"

# A singleton is the same instance from every pod.
curl -s -X PUT "$(url 0)/singletons/sequencer-$RUN?region=eu" >/dev/null
await "$(url 1)/where/sequencer-$RUN" '"host":"' 10 >/dev/null || fail "pod 1 could not locate sequencer-$RUN"
await "$(url 2)/where/sequencer-$RUN" '"host":"' 10 >/dev/null || fail "pod 2 could not locate sequencer-$RUN"
where1="$(curl -s "$(url 1)/where/sequencer-$RUN")"
where2="$(curl -s "$(url 2)/where/sequencer-$RUN")"
[[ "$where1" == "$where2" && "$where1" == *'"host":"'* ]] || fail "singleton differed across pods ($where1 vs $where2)"
pass "a singleton is reached by the same name from every pod"

# Elasticity: a fourth pod appears in the headless service's DNS and joins; scaling
# back down makes it leave gracefully and the survivors converge on three members.
"${KUBECTL[@]}" scale statefulset/nodeakt --replicas=4 >/dev/null
await "$(url 0)/health" '"members":4' 40 >/dev/null || fail "the fourth pod did not join"
"${KUBECTL[@]}" scale statefulset/nodeakt --replicas=3 >/dev/null
await "$(url 0)/health" '"members":3' 40 >/dev/null || fail "the cluster did not shrink back to three members"
pass "scaling the StatefulSet grew and shrank the cluster"

# Recreation after a crash: force-delete the pod hosting the worker, so it dies with
# no graceful leave. The member dies with its IP: the survivors detect the death and
# the coordinator recreates the actor from its recipe on one of them, while the
# StatefulSet independently brings a replacement pod up as a brand-new member.
victim_ip="$(curl -s "$(url 1)/where/orders-$RUN" | sed 's/.*"host":"\([^"]*\)".*/\1/')"
victim=""
for i in 0 1 2; do
  if [[ "$(pod_ip "nodeakt-$i")" == "$victim_ip" ]]; then victim="nodeakt-$i"; fi
done

[[ -n "$victim" ]] || fail "could not map $victim_ip to a pod"
echo "hard-killing $victim at $victim_ip (hosts orders-$RUN)..."
"${KUBECTL[@]}" delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1
survivor=1
if [[ "$victim" == "nodeakt-1" ]]; then survivor=2; fi

await "$(url $survivor)/greet/orders-$RUN?who=ada" '"greeting":"eu:ada"' 40 >/dev/null || fail "orders-$RUN was not recreated after the crash"
relocated="$(curl -s "$(url $survivor)/where/orders-$RUN")"
[[ "$relocated" != *"\"host\":\"$victim_ip\""* ]] || fail "orders-$RUN still reports the dead member's address ($relocated)"
echo "orders-$RUN now at: $relocated"
pass "a crashed pod's actor was recreated on a survivor with its configuration intact"

echo "all use cases passed"
