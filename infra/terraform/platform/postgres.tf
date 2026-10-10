resource "kubernetes_namespace_v1" "banvic" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret_v1" "postgres" {
  metadata {
    name      = "postgres-credentials"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  data = {
    POSTGRES_USER       = "postgres"
    POSTGRES_PASSWORD   = var.postgres_admin_password
    POSTGRES_DB         = "banvic"
    AIRFLOW_DB_PASSWORD = var.airflow_db_password
    BANVIC_DW_PASSWORD  = var.banvic_dw_password
  }
}

resource "kubernetes_config_map_v1" "postgres_init" {
  metadata {
    name      = "postgres-init"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  data = {
    "01-init.sh" = <<-EOT
      #!/bin/bash
      set -euo pipefail
      psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE ROLE airflow LOGIN PASSWORD '$AIRFLOW_DB_PASSWORD';
        CREATE DATABASE airflow OWNER airflow;

        CREATE ROLE banvic LOGIN PASSWORD '$BANVIC_DW_PASSWORD';
        GRANT ALL PRIVILEGES ON DATABASE banvic TO banvic;

        REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
        REVOKE CONNECT ON DATABASE banvic FROM PUBLIC;
        GRANT CONNECT ON DATABASE airflow TO airflow;
        GRANT CONNECT ON DATABASE banvic TO banvic;
        CREATE SCHEMA raw AUTHORIZATION banvic;
        CREATE SCHEMA stg AUTHORIZATION banvic;
      EOSQL
    EOT
  }
}

resource "kubernetes_persistent_volume_claim_v1" "postgres_data" {
  metadata {
    name      = "postgres-data"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = "2Gi"
      }
    }
  }
  wait_until_bound = false
}

resource "kubernetes_deployment_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
    labels    = { app = "postgres" }
  }
  spec {
    replicas = 1
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = { app = "postgres" }
    }
    template {
      metadata {
        labels = { app = "postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16"
          port {
            container_port = 5432
          }
          env_from {
            secret_ref {
              name = kubernetes_secret_v1.postgres.metadata[0].name
            }
          }
          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }
          volume_mount {
            name       = "init"
            mount_path = "/docker-entrypoint-initdb.d"
            read_only  = true
          }
          readiness_probe {
            exec {
              command = ["pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "banvic"]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
          resources {
            requests = { cpu = "250m", memory = "256Mi" }
            limits   = { memory = "1Gi" }
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.postgres_data.metadata[0].name
          }
        }
        volume {
          name = "init"
          config_map {
            name         = kubernetes_config_map_v1.postgres_init.metadata[0].name
            default_mode = "0755"
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  spec {
    selector = { app = "postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

resource "kubernetes_service_v1" "postgres_external" {
  metadata {
    name      = "postgres-external"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  spec {
    type     = "NodePort"
    selector = { app = "postgres" }
    port {
      port        = 5432
      target_port = 5432
      node_port   = 30432
    }
  }
}
