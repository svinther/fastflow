#!/bin/bash
# based on script from nolar/kopf

set -eu
set -x

: ${K8S:=latest}
if [[ "$K8S" == latest ]] ; then
    K8S="$( curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt )"
fi

curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

mkdir -p $HOME/.kube $HOME/.minikube
touch $HOME/.kube/config

sudo apt-get update -y
sudo apt-get install -y conntrack

minikube config set driver docker
minikube start \
    --extra-config=apiserver.authorization-mode=Node,RBAC \
    --extra-config=apiserver.runtime-config=events.k8s.io/v1beta1=false \
    --kubernetes-version="$K8S"
