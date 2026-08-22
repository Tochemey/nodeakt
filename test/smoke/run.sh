#!/usr/bin/env bash
# Packs the library and runs the consumer smoke test under the given
# runtime command:
#
#   test/smoke/run.sh node
#   test/smoke/run.sh bun
#   test/smoke/run.sh deno run -A
#
# The library is built, packed, and installed into a throwaway project,
# so the test exercises exactly what an npm consumer gets: the dist
# entry, the packaged worker entry, and module resolution by name.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

(cd "$root" && pnpm build >/dev/null && pnpm pack --pack-destination "$work" >/dev/null)

cp "$here/counter.actor.mjs" "$here/smoke.mjs" "$work"
cd "$work"
printf '{ "name": "nodeakt-smoke", "private": true, "type": "module" }\n' > package.json
npm install --no-audit --no-fund --loglevel=error ./tochemey-nodeakt-*.tgz >/dev/null

exec "$@" smoke.mjs
