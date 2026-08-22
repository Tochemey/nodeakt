# nodeakt examples
#
# Each target is one capability of the runtime. See examples/README.md.
# Run one with `make helloworld`; list them with `make`.

TSX := node_modules/.bin/tsx

.DEFAULT_GOAL := help
.PHONY: help helloworld behaviors chat supervision reentrancy watch stash props multicore bench bench-baseline

help: ## list the available example targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

helloworld: ## actors own their state; tell and ask
	@$(TSX) examples/helloworld/main.ts

behaviors: ## an actor is a state machine (become / unBecome)
	@$(TSX) examples/behaviors/main.ts

chat: ## many actors, no shared memory; death-watch cleanup
	@$(TSX) examples/chat/main.ts

supervision: ## let it crash; restart with backoff and fresh state
	@$(TSX) examples/supervision/main.ts

reentrancy: ## ask without freezing (request + onReply)
	@$(TSX) examples/reentrancy/main.ts

watch: ## death is a message (watch / Terminated)
	@$(TSX) examples/watch/main.ts

stash: ## defer work until ready, then unstashAll
	@$(TSX) examples/stash/main.ts

props: ## construction is data (Props.create)
	@$(TSX) examples/props/main.ts

multicore: ## every core, invisibly (Props actors in parallel)
	@$(TSX) examples/multicore/main.ts

bench: ## run the full benchmark suite (see benchmark/README.md)
	@pnpm bench

bench-baseline: ## the plain tell and ask numbers, messages per second
	@pnpm exec vitest run --config vitest.bench.config.ts benchmark/baseline.bench.ts
