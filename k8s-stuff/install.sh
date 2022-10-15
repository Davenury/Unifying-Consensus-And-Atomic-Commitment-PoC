#!/usr/bin/env bash

echo "Installing kubectl..."

if ! command -v kubectl &> /dev/null
then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  echo "kubectl installed"
else
  echo "kubectl already installed"
fi

echo "Installing minikube..."
if ! command -v minikube &> /dev/null
then
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  rm minikube*
  echo "minikube installed"
else
  echo "minikube already installed"
fi

echo "Installing helm3..."
if ! command -v helm &> /dev/null
then
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
  echo "helm installed"
  rm get_helm.sh
else
  echo "helm already installed"
fi

echo "Installing asdf"
if ! command -v asdf &> /dev/null
then
  git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.10.2
  . $HOME/.asdf/asdf.sh
  . $HOME/.asdf/completions/asdf.bash
  echo "asdf installed"
else
  echo "asdf already installed"
fi

echo "Installing golang"
if ! command -v go &> /dev/null
then
  asdf plugin-add golang https://github.com/kennyp/asdf-golang.git
  asdf install golang 1.19.2
  echo "golang 1.19.2" > ~/.tool-versions
else
  echo "go already installed"
fi

echo "Adding helm repositories"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add stable https://charts.helm.sh/stable
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update