import json

import kopf
from kubernetes_asyncio.client import ApiException

from fastflow.engine.models import TaskResult, get_class
from fastflow.engine.utils import _get_owner_digraph_name
from fastflow.kubernetes import (
    get_custom_objects_api,
    set_child_status_on_parent_cr,
)
from fastflow.models import (
    TASKSTATUS,
    UNRESOLVED,
    Task,
    TaskCRD,
    WorkflowCRD,
    WorkflowMalformed,
)
from fastflow.setup import get_appsettings

DEFAULT_TASK_RESULT = TaskResult(success=True, finished=True)


@kopf.on.create(
    TaskCRD.plural(),
)
async def execute_task(
    meta,
    body,
    spec,
    status,
    patch,
    logger,
    **kwargs,
):
    """Execute the task implementation and update the task status.

    As soon as a task is created, we execute the task implementation.

    It is assumed that the task is fully ready for this (DAG wise).

    But there can still be cases where the task is not ready to execute:

    * Inputs for the task can not be fully resolved
    * The task implementation can not be found or is invalid

    In those cases the task will be marked with status = failed. And it will not be retried.

    """
    assert status[TaskCRD.STATUS_TASK_STATUS] == TASKSTATUS.executing.value
    index = kwargs["workflow_idx"]

    task_model = Task(**spec)
    workflow_body, *_ = index[_get_owner_digraph_name(meta)]

    # Check for unresolved references
    def check_unresolved(inputs, path):
        inputs_type = type(inputs)
        if inputs_type == str:
            if UNRESOLVED.MARKER in inputs:
                raise WorkflowMalformed(
                    f"Task '{task_model.name}' impl: '{task_model.impl}' has unresolved input for input '{path}'"
                )

        elif inputs_type == list:
            for idx, li in enumerate(inputs):
                check_unresolved(li, path + f"[{idx}]")
        elif inputs_type == dict:
            for k, v in inputs.items():
                check_unresolved(v, path + "." + k)

    try:
        check_unresolved(task_model.inputs, "")
        task_impl_class = get_class(task_model.impl)
        task_impl = task_impl_class(task_model, task_model.inputs)
    except WorkflowMalformed as e:
        patch.status[TaskCRD.STATUS_TASK_STATUS] = TASKSTATUS.failed.value
        raise kopf.PermanentError(f"WorkflowMalformed {str(e)}")

    try:
        logger.info(
            f"Completing workflow '{_get_owner_digraph_name(meta)}'/'{task_model.name}' - "
            f"task_status '{status[TaskCRD.STATUS_TASK_STATUS]}' - "
            f"class '{task_model.impl}'"
        )
        task_result = await task_impl.complete(
            meta=meta,
            body=body,
            spec=spec,
            status=status,
            patch=patch,
            logger=logger,
            **kwargs,
        )
        if not task_result:
            task_result = DEFAULT_TASK_RESULT
    except kopf.PermanentError as e:
        patch.status[TaskCRD.STATUS_TASK_STATUS] = TASKSTATUS.failed.value
        raise e

    if not type(task_result) is TaskResult:
        logger.info(f"Set permanent task_status = failed for task '{task_model.name}'")
        patch.status[TaskCRD.STATUS_TASK_STATUS] = TASKSTATUS.failed.value
        raise kopf.PermanentError(
            f"Task '{task_model.name}' returned result that was not class TaskResult: '{task_result}'"
        )

    # Ignore repeated messages
    if task_result.message:
        task_messages = status.get(TaskCRD.STATUS_TASK_MESSAGES, [])
        if not task_messages or task_messages[-1] != task_result.message:
            task_messages.append(task_result.message)
            patch.status[TaskCRD.STATUS_TASK_MESSAGES] = task_messages

    if not task_result.finished:
        delay_retry = task_result.delay_retry
        if delay_retry is None:
            delay_retry = get_appsettings().kopf_handler_retry_default_delay

        raise kopf.TemporaryError(
            f"Task '{task_model.name}' impl: '{task_model.impl}' did not complete"
            f", reschedule in {delay_retry} secs",
            delay=delay_retry,
        )

    task_outputs = task_result.outputs or {}

    for declared_output_name, declared_output_type in task_impl_class.produces_outputs() or []:
        if declared_output_name not in task_outputs.keys():
            logger.warn(
                f"Task '{task_model.name}' class '{task_model.impl}'"
                f" declares output '{declared_output_name}' - was not found"
            )

    # The outputs from the task impl comes with keys as class TaskOutput
    # Convert keys to their string repr
    patch.status.setdefault("outputs", {}).update(task_outputs)

    task_status = TASKSTATUS.complete if task_result.success else TASKSTATUS.failed
    logger.info(
        f"Patching task status '{_get_owner_digraph_name(meta)}'/'{spec['name']}':"
        f" '{status[TaskCRD.STATUS_TASK_STATUS]}'-->'{task_status.value}'"
    )

    patch.status[TaskCRD.STATUS_TASK_STATUS] = task_status.value
    if task_status == TASKSTATUS.failed:
        raise kopf.PermanentError("Task finished as failed")


@kopf.on.update(
    TaskCRD.plural(),
    field=f"status.{TaskCRD.STATUS_TASK_STATUS}",
    new=kopf.PRESENT,
)
async def task_status_updated(body, meta, spec, logger, old, new, **_):
    """Update the parent Workflow status based on the task status.

    We do this in a separate handler for atomicity.

    In the handler where the task is completed, we only patch the task status.

    In this handler we notify the parent workflow of what happened, by patching the parent workflow.
    """
    workflow_name = _get_owner_digraph_name(meta)
    new_task_status = TASKSTATUS(new)
    finished = new_task_status in (TASKSTATUS.failed, TASKSTATUS.complete)
    success = new_task_status == TASKSTATUS.complete

    logger.info(
        f" Task status transition '{workflow_name}'/'{spec['name']}':"
        f" '{old}'-->'{new}'"
        f" success: {success} finished: {finished}"
    )
    try:
        await set_child_status_on_parent_cr(
            body,
            success,
            finished,
            extra_fields={
                "task_local_name": spec["name"],
                "task_status": new,
            },
        )
    except ApiException as e:
        if e.status != 404:
            raise e
        logger.warn(f"Our parent was deleted, owner ref: {json.dumps(body['metadata']['ownerReferences'])}")


@kopf.on.delete(TaskCRD.plural())
async def task_deleted(meta, namespace, body, logger, **_):
    try:
        owner_ref = meta["ownerReferences"][0]

        result = await get_custom_objects_api(mergepatch=True).patch_namespaced_custom_object(
            WorkflowCRD.group(),
            WorkflowCRD.version(),
            namespace,
            WorkflowCRD.plural(),
            owner_ref["name"],
            body={"status": {"children": {meta["uid"]: None}}},
            _request_timeout=30,
        )
        return result

    except ApiException as e:
        if e.status != 404:
            raise e
        logger.warning(f"Our parent was deleted, owner ref: {json.dumps(body['metadata']['ownerReferences'])}")
