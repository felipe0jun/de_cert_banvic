#!/usr/bin/env bash
set -u
export DEBIAN_FRONTEND=noninteractive
ARCH=amd64
OK=(); FAIL=()
step() { echo; echo "=================== $1 ==================="; }
done_ok() { OK+=("$1"); }
done_fail() { FAIL+=("$1"); echo "!! FALHOU: $1"; }

sudo -v || { echo "Preciso do sudo."; exit 1; }

step "1/8 apt: pacotes base"
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates gnupg lsb-release apt-transport-https \
  python3-pip python3-venv pipx postgresql-client \
  && done_ok "pacotes base (pip, venv, pipx, psql)" || done_fail "pacotes base"

step "2/8 Docker (repositório oficial)"
CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
sudo install -m 0755 -d /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.asc ]; then
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
fi
echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $CODENAME stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin \
  && done_ok "docker + compose" || done_fail "docker"
sudo systemctl enable --now docker >/dev/null 2>&1
sudo usermod -aG docker "$USER" && echo "usuario $USER adicionado ao grupo docker (requer novo login)"

step "3/8 Terraform (repositório HashiCorp)"
if [ ! -f /etc/apt/keyrings/hashicorp.gpg ]; then
  curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/hashicorp.gpg
fi
echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $CODENAME main" \
  | sudo tee /etc/apt/sources.list.d/hashicorp.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq terraform && done_ok "terraform" || done_fail "terraform"

step "4/8 kubectl"
KVER=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/${ARCH}/kubectl" \
  && sudo install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl \
  && done_ok "kubectl $KVER" || done_fail "kubectl"

step "5/8 kind (Kubernetes in Docker)"
KINDVER=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/kind/releases/latest | grep -oP '"tag_name":\s*"\K[^"]+')
curl -fsSLo /tmp/kind "https://kind.sigs.k8s.io/dl/${KINDVER}/kind-linux-${ARCH}" \
  && sudo install -o root -g root -m 0755 /tmp/kind /usr/local/bin/kind \
  && done_ok "kind $KINDVER" || done_fail "kind"

step "6/8 minikube (alternativa ao kind)"
curl -fsSLo /tmp/minikube.deb "https://storage.googleapis.com/minikube/releases/latest/minikube_latest_${ARCH}.deb" \
  && sudo dpkg -i /tmp/minikube.deb >/dev/null \
  && done_ok "minikube" || done_fail "minikube"

step "7/8 Helm"
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash \
  && done_ok "helm" || done_fail "helm"

step "8/8 uv (gerenciador Python, sem sudo)"
curl -LsSf https://astral.sh/uv/install.sh | sh && done_ok "uv" || done_fail "uv"
export PATH="$HOME/.local/bin:$PATH"
pipx ensurepath >/dev/null 2>&1 || true

step "RESUMO"
for i in "${OK[@]}"; do echo "  OK    $i"; done
for i in "${FAIL[@]}"; do echo "  FALHA $i"; done
echo
for c in docker terraform kubectl kind minikube helm uv psql pipx; do
  if command -v "$c" >/dev/null 2>&1; then printf "  %-10s %s\n" "$c" "$("$c" version --short 2>/dev/null | head -1 || "$c" --version 2>/dev/null | head -1)"; else printf "  %-10s NAO ENCONTRADO\n" "$c"; fi
done
echo
echo "Faça logout/login (ou rode: newgrp docker) para usar docker sem sudo."
