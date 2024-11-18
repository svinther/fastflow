import asyncio
from copy import copy, deepcopy
from typing import Dict, List, Optional, Type, Union

import kopf
import yaml
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from yaml import Loader

from .models import FastflowCRD, WorkflowCRD, get_crd_by_kind
from .setup import logger


async def create_cr_as_child(namespace, crd: Type[FastflowCRD], body: Optional[dict] = None):
    body = copy(body or {})
    kopf.adopt(body)
    body.update(
        {
            "kind": crd.kind(),
            "apiVersion": f"{crd.group()}/{crd.version()}",
        }
    )

    create_result = await get_custom_objects_api().create_namespaced_custom_object(
        crd.group(),
        crd.version(),
        namespace,
        crd.plural(),
        body=body,
        _request_timeout=30,
    )
    return create_result


async def set_child_status_on_parent_cr(child_obj, success: bool, finished: bool, extra_fields=None, message=None):
    owner_ref = child_obj["metadata"]["ownerReferences"][0]
    namespace = child_obj["metadata"]["namespace"]
    parent_crd = get_crd_by_kind(owner_ref["kind"])

    body = {
        "status": {
            "children": create_status_patch(
                child_obj,
                success,
                finished,
                extra_fields=extra_fields,
                message=message,
            )
        }
    }
    result = await get_custom_objects_api(mergepatch=True).patch_namespaced_custom_object(
        parent_crd.group(),
        parent_crd.version(),
        namespace,
        parent_crd.plural(),
        owner_ref["name"],
        body=body,
        _request_timeout=30,
    )
    return result


def create_workflow_crd_object(
    name,
    dag: str,
    labels: Optional[Dict[str, str]] = None,
    global_inputs: Optional[Union[dict, str]] = None,
    generateName=False,
    workflow_dependencies: Optional[List[str]] = None,
):
    if global_inputs is None:
        global_inputs = {}
    elif type(global_inputs) is str:
        global_inputs = yaml.load(global_inputs, Loader=Loader)

    labels = labels or {}

    metadata = {"labels": labels}
    if generateName:
        metadata["generateName"] = name
    else:
        metadata["name"] = name

    return {
        "apiVersion": f"{WorkflowCRD.group()}/{WorkflowCRD.version()}",
        "kind": WorkflowCRD.kind(),
        "metadata": metadata,
        "spec": {
            "global_inputs": global_inputs,
            "dag": dag,
            "dependencies": workflow_dependencies,
        },
    }


def create_status_patch(
    obj,
    success: bool,
    finished: bool,
    extra_fields: Optional[dict] = None,
    message: Optional[str] = None,
) -> dict:
    patch = {
        obj["metadata"]["uid"]: {
            "kind": obj["kind"],
            "name": obj["metadata"]["name"],
            "success": success,
            "finished": finished,
            "message": message,
        }
    }
    if extra_fields:
        patch[obj["metadata"]["uid"]].update(extra_fields)
    return patch


def get_namespaced_custom_object_lookup_args(name, namespace, crd: Type[FastflowCRD]):
    return {
        "group": crd.group(),
        "version": crd.version(),
        "plural": crd.plural(),
        "name": name,
        "namespace": namespace,
    }


def get_parent_custom_object_lookup_args(child_obj: dict):
    owner_ref = child_obj["metadata"]["ownerReferences"][0]
    parent_crd = get_crd_by_kind(owner_ref["kind"])
    return get_namespaced_custom_object_lookup_args(owner_ref["name"], child_obj["metadata"]["namespace"], parent_crd)


def get_create_namespaced_custom_object_lookup_args(namespace, crd: Type[FastflowCRD], body: Optional[dict] = None):
    body = deepcopy(body or {})

    body.update(
        {
            "kind": crd.kind(),
            "apiVersion": f"{crd.group()}/{crd.version()}",
        }
    )

    return {
        "group": crd.group(),
        "version": crd.version(),
        "plural": crd.plural(),
        "kind": crd.kind(),
        "namespace": namespace,
        "body": body,
    }


_custom_objects_api: CustomObjectsApi | None = None
_api_client: ApiClient | None = None
_custom_objects_api_mergepatch: CustomObjectsApi | None = None
_api_client_mergepatch: ApiClient | None = None


def get_custom_objects_api(mergepatch: bool = False):
    if mergepatch:
        return _custom_objects_api_mergepatch
    return _custom_objects_api


@kopf.on.startup()
async def init(**_):
    # Initialize the kubernetes_asyncio api client
    try:
        load_incluster_config()
    except ConfigException:
        logger.info("Failed loading incluster config, trying config file")
        await load_kube_config()

    global _custom_objects_api, _api_client, _custom_objects_api_mergepatch, _api_client_mergepatch

    _api_client = ApiClient()
    _custom_objects_api = CustomObjectsApi(_api_client)

    _api_client_mergepatch = ApiClient()
    _api_client_mergepatch.set_default_header("Content-Type", "application/merge-patch+json")
    _custom_objects_api_mergepatch = CustomObjectsApi(_api_client_mergepatch)


@kopf.on.cleanup()
async def cleanup_dependencies(**_):
    await asyncio.gather(_api_client.close(), _api_client_mergepatch.close())
