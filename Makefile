SHELL := /bin/bash
.DEFAULT_GOAL := help

ROOT        := $(abspath .)
KUBECONFIG  := $(ROOT)/.kubeconfig
TF_CLUSTER  := infra/terraform/cluster
TF_PLATFORM := infra/terraform/platform
NAMESPACE   := banvic
MELTANO_IMG := banvic/meltano:local
CLUSTER     := banvic

ifneq (,$(wildcard .env))
include .env
export
endif

export KUBECONFIG
export TF_VAR_meltano_image=$(MELTANO_IMG)

.PHONY: help check-env cluster images platform up down status ui psql trigger logs

help: ## Lista os alvos
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

check-env:
	@test -f .env || { echo "Crie o .env a partir de .env.example"; exit 1; }

cluster: ## Etapa 1: cria o cluster kind via Terraform
	terraform -chdir=$(TF_CLUSTER) init -input=false
	terraform -chdir=$(TF_CLUSTER) apply -auto-approve -input=false

images: ## Etapa 2: build da imagem do Meltano e carga no kind
	docker build -t $(MELTANO_IMG) ingestion/meltano
	kind load docker-image $(MELTANO_IMG) --name $(CLUSTER)

platform: check-env ## Etapa 3: Postgres, segredos e Airflow (Helm) via Terraform
	terraform -chdir=$(TF_PLATFORM) init -input=false
	terraform -chdir=$(TF_PLATFORM) apply -auto-approve -input=false
	@echo "Aguardando o Airflow ficar pronto..."
	kubectl -n $(NAMESPACE) rollout status statefulset/airflow-scheduler --timeout=600s
	kubectl -n $(NAMESPACE) rollout status deploy/airflow-dag-processor --timeout=600s
	kubectl -n $(NAMESPACE) rollout status deploy/airflow-api-server --timeout=600s

up: cluster images platform status ## Sobe tudo (1 + 2 + 3)

down: check-env ## Destrói o cluster (e tudo dentro dele)
	-terraform -chdir=$(TF_PLATFORM) destroy -auto-approve -input=false
	terraform -chdir=$(TF_CLUSTER) destroy -auto-approve -input=false

status: ## Pods do namespace
	kubectl -n $(NAMESPACE) get pods

ui: ## Mostra a URL da UI do Airflow
	@echo "Airflow: http://localhost:8080  (usuário: admin)"

psql: ## Abre psql no banco analítico (pede a senha do usuário banvic)
	psql -h localhost -p 5432 -U banvic -d banvic

trigger: ## Dispara a DAG de ingestão manualmente
	kubectl -n $(NAMESPACE) exec deploy/airflow-dag-processor -c dag-processor -- bash -c "airflow dags unpause banvic_ingestion >/dev/null; airflow dags trigger banvic_ingestion"

logs: ## Logs do scheduler
	kubectl -n $(NAMESPACE) logs statefulset/airflow-scheduler -c scheduler -f
