#!/usr/bin/env bash
# Runs transport, remoting, and membership smoke tests under each
# supported runtime, straight from source: these layers live in src/, so unlike run.sh
# there is nothing to pack.
#
#   test/smoke/net.sh          # every runtime found on PATH
#   test/smoke/net.sh node     # just one of node | bun | deno
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
smoke="$here/net.smoke.ts"
remoting="$here/remoting.smoke.ts"
membership="$here/membership.smoke.ts"
only="${1:-}"

ran=0
failed=0

run_one() {
  local name="$1"
  shift
  if [ -n "$only" ] && [ "$only" != "$name" ]; then
    return 0
  fi

  if ! command -v "$1" >/dev/null 2>&1; then
    echo "SKIP: $name is not installed"
    return 0
  fi

  ran=$((ran + 1))
  if (cd "$root" && "$@" "$smoke" && "$@" "$remoting" && "$@" "$membership"); then
    :
  else
    failed=$((failed + 1))
  fi
}

run_one node pnpm exec tsx
run_one bun bun
run_one deno deno run --allow-net --allow-env --allow-sys --unstable-sloppy-imports

if [ "$ran" -eq 0 ]; then
  echo "FAIL: no requested runtime is installed"
  exit 1
fi

exit "$failed"
