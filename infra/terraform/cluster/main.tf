terraform {
  required_version = ">= 1.6"

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.11"
    }
  }
}

provider "kind" {}

locals {
  repo_root = abspath("${path.module}/../../..")
}

resource "kind_cluster" "banvic" {
  name            = var.cluster_name
  node_image      = var.node_image
  wait_for_ready  = true
  kubeconfig_path = "${local.repo_root}/.kubeconfig"

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      extra_mounts {
        host_path      = "${local.repo_root}/dags"
        container_path = "/mnt/banvic/dags"
      }
      extra_mounts {
        host_path      = "${local.repo_root}/data"
        container_path = "/mnt/banvic/data"
      }

      extra_port_mappings {
        container_port = 30080
        host_port      = var.airflow_host_port
        listen_address = "127.0.0.1"
        protocol       = "TCP"
      }
      extra_port_mappings {
        container_port = 30432
        host_port      = var.postgres_host_port
        listen_address = "127.0.0.1"
        protocol       = "TCP"
      }
    }
  }
}
