# syntax=docker/dockerfile:1
#
# Builds one node of the distributed-actor cluster. Every service in
# docker-compose.yml runs this same image and is handed its identity through
# environment variables.

FROM node:22-slim

WORKDIR /app
RUN npm install -g pnpm@11

# The example imports the framework source directly, so the whole repository is
# copied in, then dependencies are installed against the committed lockfile. The
# lifecycle scripts are skipped: the only one is a git-hook installer for local
# development, which has no place in a container image.
COPY . .
RUN pnpm install --frozen-lockfile --ignore-scripts

# tsx runs the TypeScript entry point without a separate build step. Node is the
# process directly, not a pnpm or tsx child, so it is PID 1 in the container and
# receives SIGTERM itself: its handler leaves the cluster gracefully on `compose stop`.
CMD ["node", "--import", "tsx/esm", "examples/dns-actors/node.ts"]
