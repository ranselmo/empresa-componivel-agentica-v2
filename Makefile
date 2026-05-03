# Makefile — PoC ECI v2 (Go + Kafka)
# Facilita o gerenciamento das células Go localmente

.PHONY: help deps build up down logs test ff clean

help: ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

deps: ## Baixa dependências Go de todas as células
	@echo "→ cell-pedidos"
	cd cell-pedidos && go mod tidy
	@echo "→ cell-estoque"
	cd cell-estoque && go mod tidy
	@echo "→ cell-notificacoes"
	cd cell-notificacoes && go mod tidy

build: ## Build local das células (sem Docker)
	cd cell-pedidos && CGO_ENABLED=1 go build ./cmd/...
	cd cell-estoque && CGO_ENABLED=1 go build ./cmd/...
	cd cell-notificacoes && CGO_ENABLED=1 go build ./cmd/...
	@echo "✓ Build concluído"

up: ## Sobe o stack completo
	docker compose up -d --build
	@echo "Aguardando shard-router ficar saudável..."
	@until curl -sf http://localhost:8080/healthz/live >/dev/null 2>&1; do sleep 2; done
	@curl -sf http://localhost:8080/healthz/live && echo " ✓ shard-router OK"
	@curl -sf http://localhost:8080/router/cells | python3 -c \
	  "import sys,json; c=json.load(sys.stdin).get('cells',[]); print(f' ✓ {sum(1 for x in c if x[\"Healthy\"])}/{len(c)} células saudáveis')"

down: ## Para e remove todos os containers e volumes
	docker compose down -v

logs: ## Tail de todos os logs das células
	docker compose logs -f cell-pedidos cell-estoque cell-notificacoes

test: ## Executa as fitness functions (requer stack rodando)
	pip install httpx -q
	python3 fitness-functions/run_all.py

ff1: ## Apenas FF1 - boundary isolation (não precisa stack)
	python3 -c "\
import sys; exec(open('fitness-functions/run_all.py').read()); \
ok = run_ff1(); sys.exit(0 if ok else 1)"

demo: ## Executa a demo SAGA completa
	chmod +x demo.sh && ./demo.sh

rebuild: ## Rebuild e restart de uma célula específica (ex: make rebuild CELL=cell-pedidos)
	docker compose build $(CELL) && docker compose restart $(CELL)

kafka-topics: ## Lista os tópicos Kafka criados
	docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

kafka-lag: ## Mostra o consumer lag de todos os grupos
	docker compose exec kafka kafka-consumer-groups \
		--bootstrap-server kafka:29092 \
		--describe --all-groups 2>/dev/null || echo "Nenhum grupo ativo ainda"

clean: ## Remove binários e caches
	find . -name "*.test" -delete
	find . -name "main" -type f -not -path "*/cmd/*" -delete
	docker system prune -f
