import logging
import os
import sys
from copy import deepcopy
from typing import List, Type, Union

from kopf._kits.runner import KopfRunner
from kubernetes import config
from kubernetes.client import ApiClient, ApiException, CustomObjectsApi

from fastflow.models import FastflowCRD, TaskCRD, WorkflowCRD

logger = logging.getLogger("kopf")
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)

config.load_kube_config()

OPERATOR_NAMESPACE = "fastflow-test"
os.environ["OPERATOR_NAMESPACE"] = OPERATOR_NAMESPACE


class AbstractOperatorTest(KopfRunner):
    def __init__(self, manifests: Union[dict, List[dict]] = None):
        super().__init__(
            [
                "run",
                "-m",
                "fastflow",
                "--peering=test",
                "--namespace",
                OPERATOR_NAMESPACE,
            ]
        )

        if manifests:
            if isinstance(manifests, List):
                self.manifests: List[dict] = manifests
            else:
                self.manifests = [manifests]

        if self.manifests:
            for manifest in self.manifests:
                if manifest.get("metadata", {}).get("name"):
                    name = manifest.get("metadata", {}).get("name")
                    with ApiClient() as api_client:
                        try:
                            CustomObjectsApi(
                                api_client
                            ).delete_namespaced_custom_object(
                                WorkflowCRD.group(),
                                WorkflowCRD.version(),
                                OPERATOR_NAMESPACE,
                                WorkflowCRD.plural(),
                                name,
                            )
                        except ApiException as e:
                            if e.status != 404:
                                raise e

    def __enter__(self) -> "KopfRunner":
        if self.manifests:
            for manifest in self.manifests:
                body = deepcopy(manifest)
                body.update(
                    {
                        "kind": WorkflowCRD.kind(),
                        "apiVersion": f"{WorkflowCRD.group()}/{WorkflowCRD.version()}",
                    }
                )
                with ApiClient() as api_client:
                    WorkflowCRD.kind(),

                    CustomObjectsApi(
                        api_client
                    ).create_namespaced_custom_object(
                        WorkflowCRD.group(),
                        WorkflowCRD.version(),
                        OPERATOR_NAMESPACE,
                        WorkflowCRD.plural(),
                        body,
                    )
        return super().__enter__()


def get_cr(name, namespace, crd: Type[FastflowCRD]):
    with ApiClient() as api_client:
        try:
            return CustomObjectsApi(api_client).get_namespaced_custom_object(
                crd.group(), crd.version(), namespace, crd.plural(), name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise e


def get_k8s_task_object(workflow_obj, task_local_name):
    child_status = next(
        filter(
            lambda c: c["task_local_name"] == task_local_name,
            workflow_obj["status"]["children"].values(),
        )
    )
    return get_cr(child_status["name"], OPERATOR_NAMESPACE, TaskCRD)
