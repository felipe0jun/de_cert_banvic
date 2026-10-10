resource "kubernetes_secret_v1" "airflow_metadata" {
  metadata {
    name      = "airflow-metadata"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  data = {
    connection = "postgresql://airflow:${urlencode(var.airflow_db_password)}@${local.postgres_host}:5432/airflow"
  }
}

resource "kubernetes_secret_v1" "airflow_connections" {
  metadata {
    name      = "airflow-connections"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  data = {
    AIRFLOW_CONN_BANVIC_DW = jsonencode({
      conn_type = "postgres"
      host      = local.postgres_host
      port      = 5432
      login     = "banvic"
      password  = var.banvic_dw_password
      schema    = "banvic"
    })
    AIRFLOW_CONN_FS_DEFAULT = jsonencode({
      conn_type = "fs"
      extra     = { path = "/" }
    })
  }
}

resource "kubernetes_secret_v1" "meltano_target_postgres" {
  metadata {
    name      = "meltano-target-postgres"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  data = {
    TARGET_POSTGRES_HOST     = local.postgres_host
    TARGET_POSTGRES_PORT     = "5432"
    TARGET_POSTGRES_USER     = "banvic"
    TARGET_POSTGRES_PASSWORD = var.banvic_dw_password
    TARGET_POSTGRES_DATABASE = "banvic"
  }
}

resource "kubernetes_persistent_volume_v1" "dags" {
  metadata {
    name = "banvic-dags"
  }
  spec {
    capacity                         = { storage = "1Gi" }
    access_modes                     = ["ReadWriteOnce"]
    storage_class_name               = "manual"
    persistent_volume_reclaim_policy = "Retain"
    persistent_volume_source {
      host_path {
        path = "/mnt/banvic/dags"
        type = "Directory"
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "dags" {
  metadata {
    name      = "banvic-dags"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "manual"
    volume_name        = kubernetes_persistent_volume_v1.dags.metadata[0].name
    resources {
      requests = { storage = "1Gi" }
    }
  }
}

resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = var.airflow_chart_version
  namespace  = kubernetes_namespace_v1.banvic.metadata[0].name

  timeout       = 900
  wait          = false
  wait_for_jobs = false

  values = [
    templatefile("${path.module}/airflow-values.yaml", {
      metadata_secret    = kubernetes_secret_v1.airflow_metadata.metadata[0].name
      connections_secret = kubernetes_secret_v1.airflow_connections.metadata[0].name
      dags_claim         = kubernetes_persistent_volume_claim_v1.dags.metadata[0].name
      airflow_uid        = var.airflow_uid
      meltano_image      = var.meltano_image
      meltano_secret     = kubernetes_secret_v1.meltano_target_postgres.metadata[0].name
      namespace          = var.namespace
    })
  ]

  set_sensitive = [
    {
      name  = "createUserJob.defaultUser.password"
      value = var.airflow_admin_password
    }
  ]

  depends_on = [
    kubernetes_deployment_v1.postgres,
    kubernetes_service_v1.postgres,
  ]
}
