import logging
import os
import sys
from copy import deepcopy
from time import sleep
from typing import List, Tuple, Type, Union

from kopf._kits.runner import KopfRunner
from kubernetes import config
from kubernetes.client import ApiClient, ApiException, CustomObjectsApi

from fastflow.models import (
    TASKSTATUS,
    WORKFLOWSTATUS,
    FastflowCRD,
    TaskCRD,
    WorkflowCRD,
)
from fastflow.setup import get_appsettings

logger = logging.getLogger("kopf")
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)

config.load_kube_config()

OPERATOR_NAMESPACE = "fastflow-test"
os.environ["OPERATOR_NAMESPACE"] = OPERATOR_NAMESPACE

_api_client = ApiClient()

# @atexit.register
# def cleanup():
#     api_client.close()


def create_k8s_workflows_from_manifests(manifests: List[dict]) -> List[dict]:
    workflow_objects = []
    for manifest in manifests:
        body = deepcopy(manifest)
        body.update(
            {
                "kind": WorkflowCRD.kind(),
                "apiVersion": f"{WorkflowCRD.group()}/{WorkflowCRD.version()}",
            }
        )

        WorkflowCRD.kind(),

        result = CustomObjectsApi(_api_client).create_namespaced_custom_object(
            WorkflowCRD.group(),
            WorkflowCRD.version(),
            OPERATOR_NAMESPACE,
            WorkflowCRD.plural(),
            body,
        )
        workflow_objects.append(result)
    return workflow_objects


def delete_k8s_workflow_by_manifest_names(manifests: List[dict], strip_finalizers: bool = False):
    for manifest in manifests:
        if manifest.get("metadata", {}).get("name"):
            name = manifest.get("metadata", {}).get("name")
            try:
                if strip_finalizers:
                    CustomObjectsApi(_api_client).patch_namespaced_custom_object(
                        WorkflowCRD.group(),
                        WorkflowCRD.version(),
                        OPERATOR_NAMESPACE,
                        WorkflowCRD.plural(),
                        name,
                        {"metadata": {"finalizers": []}},
                    )

                CustomObjectsApi(_api_client).delete_namespaced_custom_object(
                    WorkflowCRD.group(), WorkflowCRD.version(), OPERATOR_NAMESPACE, WorkflowCRD.plural(), name
                )
            except ApiException as e:
                if e.status != 404:
                    raise e


class AbstractOperatorTest(KopfRunner):
    def __init__(self, manifests: Union[dict, List[dict]] = None, paths: List[str] = None):
        get_appsettings().namespace = OPERATOR_NAMESPACE
        get_appsettings().kopf_handler_retry_default_delay = 0.2
        import fastflow  # noqa

        kopf_args = [
            "run",
            "--peering=test",
            "--namespace",
            OPERATOR_NAMESPACE,
        ] + (paths or [])

        super().__init__(kopf_args)

        if manifests:
            if isinstance(manifests, List):
                self.manifests: List[dict] = manifests
            else:
                self.manifests = [manifests]
        else:
            self.manifests = None

        if self.manifests:
            delete_k8s_workflow_by_manifest_names(self.manifests, strip_finalizers=True)

    def __enter__(self) -> "KopfRunner":
        if self.manifests:
            create_k8s_workflows_from_manifests(self.manifests)
        return super().__enter__()


def get_cr(name, crd: Type[FastflowCRD]):
    try:
        return CustomObjectsApi(_api_client).get_namespaced_custom_object(
            crd.group(), crd.version(), OPERATOR_NAMESPACE, crd.plural(), name
        )
    except ApiException as e:
        if e.status == 404:
            return None
        raise e


def get_crs(crd: Type[FastflowCRD], label_selector: str = None):
    return CustomObjectsApi(_api_client).list_namespaced_custom_object(
        crd.group(), crd.version(), OPERATOR_NAMESPACE, crd.plural(), label_selector=label_selector
    )


def cleanup_workflow_objects():
    CustomObjectsApi(_api_client).delete_collection_namespaced_custom_object(
        WorkflowCRD.group(),
        WorkflowCRD.version(),
        OPERATOR_NAMESPACE,
        WorkflowCRD.plural(),
    )


def get_k8s_task_object(workflow_obj, task_local_name):
    child_status = next(
        filter(
            lambda c: c["task_local_name"] == task_local_name,
            workflow_obj["status"]["children"].values(),
        )
    )
    return get_cr(child_status["name"], TaskCRD)


def wait_for_workflow_status(wf_name: str, wait_for_status: Tuple[WORKFLOWSTATUS, ...]) -> WorkflowCRD:
    while True:
        sleep(2)
        wf = get_cr(wf_name, WorkflowCRD)
        wf_status_str = wf.get("status", {}).get("workflow_status", None)
        if wf_status_str:
            wf_status = WORKFLOWSTATUS(wf_status_str)
            if wf_status in wait_for_status:
                return wf


def set_task_status(task_name: str, status: TASKSTATUS):
    CustomObjectsApi(_api_client).patch_namespaced_custom_object(
        TaskCRD.group(),
        TaskCRD.version(),
        OPERATOR_NAMESPACE,
        TaskCRD.plural(),
        task_name,
        {"status": {TaskCRD.STATUS_TASK_STATUS: status.value}},
    )


def get_api_client():
    return _api_client
