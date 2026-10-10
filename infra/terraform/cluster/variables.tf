variable "cluster_name" {
  description = "Nome do cluster kind."
  type        = string
  default     = "banvic"
}

variable "node_image" {
  description = "Imagem do nó kind (kindest/node). Fixada para reprodutibilidade."
  type        = string
  default     = "kindest/node:v1.34.0"
}

variable "airflow_host_port" {
  description = "Porta no host que expõe a UI do Airflow."
  type        = number
  default     = 8080
}

variable "postgres_host_port" {
  description = "Porta no host que expõe o PostgreSQL."
  type        = number
  default     = 5432
}
