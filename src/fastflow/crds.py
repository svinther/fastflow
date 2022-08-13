from copy import copy
from typing import Type

import kopf
from kubernetes_asyncio.client import ApiClient, ApiException, CustomObjectsApi

from .models import FastflowCRD, create_status_patch, get_crd_by_kind


async def create_cr_as_child(
    namespace, crd: Type[FastflowCRD], body: dict = None
):
    body = copy(body or {})
    kopf.adopt(body)
    body.update(
        {
            "kind": crd.kind(),
            "apiVersion": f"{crd.group()}/{crd.version()}",
        }
    )

    async with ApiClient() as api_client:
        create_result = await CustomObjectsApi(
            api_client
        ).create_namespaced_custom_object(
            crd.group(), crd.version(), namespace, crd.plural(), body=body
        )
        return create_result


async def set_child_status_on_parent_cr(
    child_obj, success: bool, finished: bool, extra_fields=None, message=None
):
    owner_ref = child_obj["metadata"]["ownerReferences"][0]
    namespace = child_obj["metadata"]["namespace"]
    parent_crd = get_crd_by_kind(owner_ref["kind"])

    # Check that we have a status for this child on the parent already
    # othwer wise this could be a race condition
    # parent = ksession.get_custom_resource(**lookup_args)
    # wait_for_status_on_parent(parent, child_obj["metadata"]["uid"])

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

    async with ApiClient() as api_client:
        api_client.set_default_header(
            "Content-Type", "application/merge-patch+json"
        )
        return await CustomObjectsApi(
            api_client
        ).patch_namespaced_custom_object(
            parent_crd.group(),
            parent_crd.version(),
            namespace,
            parent_crd.plural(),
            owner_ref["name"],
            body=body,
        )
