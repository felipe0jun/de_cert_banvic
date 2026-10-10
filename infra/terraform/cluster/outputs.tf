output "cluster_name" {
  value = kind_cluster.banvic.name
}

output "kubeconfig_path" {
  value = kind_cluster.banvic.kubeconfig_path
}

output "endpoint" {
  value = kind_cluster.banvic.endpoint
}
