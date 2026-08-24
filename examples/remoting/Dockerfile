# The one image both nodes run. nodeakt has zero dependencies and the
# examples run straight from src/ under tsx, so the image is the Node
# runtime, the tsx loader, and the source: no install, no build step.
#
# Built by docker-compose.yml with the repository root as the context;
# Dockerfile.dockerignore next to this file keeps everything else out.
FROM node:22-alpine

RUN npm install -g tsx@4

WORKDIR /app
COPY src ./src
COPY examples/remoting ./examples/remoting

ENV NODE_ENV=production

# The payments node by default; compose overrides the command for the
# checkout node.
CMD ["tsx", "examples/remoting/payments.ts"]
