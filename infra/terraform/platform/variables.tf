variable "kubeconfig_path" {
  description = "Caminho do kubeconfig. Padrão: .kubeconfig na raiz do repositório."
  type        = string
  default     = null
}

variable "namespace" {
  description = "Namespace onde Postgres, Airflow e pods do Meltano rodam."
  type        = string
  default     = "banvic"
}

variable "airflow_chart_version" {
  description = "Versão do chart oficial apache-airflow/airflow."
  type        = string
  default     = "1.22.0"
}

variable "meltano_image" {
  description = "Imagem do Meltano carregada no kind (make images)."
  type        = string
  default     = "banvic/meltano:local"
}

variable "postgres_admin_password" {
  description = "Senha do superusuário postgres."
  type        = string
  sensitive   = true
}

variable "airflow_db_password" {
  description = "Senha do usuário airflow (banco de metadados do Airflow)."
  type        = string
  sensitive   = true
}

variable "banvic_dw_password" {
  description = "Senha do usuário banvic (banco analítico de destino)."
  type        = string
  sensitive   = true
}

variable "airflow_admin_password" {
  description = "Senha do usuário admin da UI do Airflow."
  type        = string
  sensitive   = true
}

variable "airflow_uid" {
  description = <<-EOT
    UID com que os containers do Airflow rodam. Use o UID do seu usuário no host
    (`id -u`, normalmente 1000): a pasta data/ é um hostPath e o scheduler precisa
    escrever em data/landing. A imagem oficial aceita UID arbitrário com GID 0.
  EOT
  type        = number
  default     = 1000
}
