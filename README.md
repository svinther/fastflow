# Fastflow

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Deploy to Kubernetes with helm chart

```shell
kubectl create ns fastflow
helm -n fastflow upgrade --install fastflow chart
```

To install in more namespasces, create the namespace and run the helm command again, this time with the
`--skip-crds` flag.

```shell
kubectl create ns fastflow-dev
helm -n fastflow-dev upgrade --install fastflow chart --skip-crds
```

## Run examples

Apply examples

```shell
kubectl -n fastflow create -f examples/01-helloworld/workflow.yaml
kubectl -n fastflow create -f examples/02-digraph/workflow.yaml
```

Inspect results

```shell
kubectl -n fastflow get workflows
kubectl -n fastflow get tasks -l workflow=helloworld
```

## Developer setup

For developing the fastflow project

### Generate CRDS

    python3 generate_crds.py

### Create CRDS

Can also be installed by applying the helm chart

    kubectl create -f chart/crds/kopfpeering-crd.yaml
    find chart/crds/generated -name *.yaml -exec kubectl create -f '{}' \;

### Delete CRDS

    find chart/crds/generated -name *.yaml -exec kubectl delete -f '{}' \;

### Virtual environment

```shell
python3 -m venv ~/venvs/fastflow
. ~/venvs/fastflow/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
```

### Run from outside cluster

Will use kubectl config for cluster access.
Greate for development, and can run with debugger attached.

Prepare namespace

```shell
kubectl create ns fastflow-dev
kubectl -n fastflow-dev apply -f - << EOYML
apiVersion: kopf.dev/v1
kind: KopfPeering
metadata:
  name: default
EOYML
```

Run as module (useful for debugger)

```shell
python3 -m fastflow \
--namespace fastflow-dev \
--dev
```

Run from cli

```shell
fastflow --namespace fastflow-dev --dev
```

## Run Tests

Install test-requirements

```shell
python3 -m pip install -e . -r test-requirements.txt
```

Prepare namespace

```shell
kubectl create ns fastflow-test
kubectl -n fastflow-test apply -f test/kopf-test-peering.yaml
```

Run the tests as module

```shell
python3 -m pytest --color=yes
```

Run the tests from cli

```shell
pytest --color=yes
```

### Building whl package and Docker image

Cleanup old packages

```shell
rm -Rf dist
```

Build package

```shell
python3 -m pip install build
python3 -m build
```

Build Docker image using minikube

```shell
eval $(minikube -p minikube docker-env)
DOCKER_BUILDKIT=1 docker build -t fastflow .
```

Use helm to run the image in Kubernetes, by specifying the image tag we just created

```shell
helm -n fastflow-dev upgrade --install --set imageOverride=fastflow fastflow chart
```

### Example using custom Task implementations

The Operator needs to be able to load the custom code. Here is an example using the `examples/03-farmlife` example
workflow. This workflow uses custom tasks implemented in `examples/03-farmlife/tasks-impl/farmlife.py`.

#### Run the operator locally

Run the operator so it can load the custom code

```shell
fastflow --dev --namespace fastflow-dev examples/03-farmlife/tasks-impl/farmlife.py
```

#### Apply the example workflow

```shell
kubectl -n fastflow-dev create -f examples/03-farmlife/workflow.yaml
```
