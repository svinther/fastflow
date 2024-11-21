import asyncio
from collections import defaultdict
from typing import DefaultDict, Optional

import kopf
from kopf import Body, Logger, Patch
from networkx import descendants, topological_sort

from fastflow.kubernetes import create_cr_as_child, get_custom_objects_api
from fastflow.models import (
    TASKSTATUS,
    WORKFLOWSTATUS,
    TaskCRD,
    WorkflowCRD,
    WorkflowCRDModel,
    WorkflowMalformed,
    build_nxdigraph,
    render_workflow_with_jinja2,
)
from fastflow.setup import get_appsettings


def _format_statuscounter(statuscounter: dict[TASKSTATUS, int]) -> str:
    return ", ".join([f"{k.value if k else None}:{v}" for k, v in statuscounter.items()])


def _get_blocking_workflows(workflow_crd_model: WorkflowCRDModel, index: kopf.Index, logger):
    blockers = []
    # Check for workflows we depend on being complete
    if workflow_crd_model.dependencies:
        for dependency in workflow_crd_model.dependencies:
            if index.get(dependency) is None:
                logger.warn(f"Ignoring workflow dependency for unknown workflow: '{dependency}'")
                continue

            indexed = index.get(dependency)
            assert indexed is not None
            dep_wf_body, *_ = indexed
            dep_wf_status = dep_wf_body.status.get(WorkflowCRD.STATUS_WORKFLOW_STATUS)
            if dep_wf_status != WORKFLOWSTATUS.complete.value:
                blockers.append((dependency, dep_wf_status))
    return blockers


_GLOBAL_WORKFLOW_LOCK: asyncio.Lock
_GLOBAL_ACTIVE_WORKFLOWS: set[tuple[str, str]] = set()


@kopf.on.startup()
async def startup_fn(logger, **kwargs):
    global _GLOBAL_WORKFLOW_LOCK
    _GLOBAL_WORKFLOW_LOCK = asyncio.Lock()

    """Count active workflows"""
    workflows = await get_custom_objects_api().list_namespaced_custom_object(
        WorkflowCRD.group(),
        WorkflowCRD.version(),
        get_appsettings().namespace,
        WorkflowCRD.plural(),
    )
    for wf in workflows["items"]:
        status = wf.get("status", {})
        wf_status = status.get(WorkflowCRD.STATUS_WORKFLOW_STATUS)
        if wf_status and wf_status == WORKFLOWSTATUS.executing.value:
            _GLOBAL_ACTIVE_WORKFLOWS.add((wf["metadata"]["namespace"], wf["metadata"]["name"]))


@kopf.index(WorkflowCRD.group(), WorkflowCRD.version(), WorkflowCRD.plural())
async def workflow_idx(name, body, **_):
    return {name: body}


@kopf.index(TaskCRD.group(), TaskCRD.version(), TaskCRD.plural())
async def task_idx(body: kopf.Body, spec, meta, **_):
    owner_digraph = next(
        filter(
            lambda owner: owner["kind"] == WorkflowCRD.kind(),
            meta["ownerReferences"],
        )
    )
    digraph_name = owner_digraph.get("name")

    index_key = (digraph_name, spec["name"])
    index_value = body
    return {index_key: index_value}


@kopf.on.create(WorkflowCRD.group(), WorkflowCRD.version(), WorkflowCRD.plural())
async def workflow_create(
    namespace: str | None,
    name: str | None,
    body: Body,
    logger: Logger,
    patch: Patch,
    **kwargs,
):
    """Event handler for handling the event of a new Workflow being created.

    If the workflow is blocked by dependencies to other workflows, or if the maximum number of parallel workflows
    is reached, the workflow will be set to `blocked` state.

    If the workflow is not blocked, it will be set to `executing` state.

    This handler has no other side effects than setting the status of the newly created Workflow.
    """
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        logger.info(f"Create new Workflow: '{name}'")
        index = kwargs["workflow_idx"]
        workflow_crd_model = WorkflowCRDModel(**body.spec)
        initial_workflow_status = WORKFLOWSTATUS.executing

        blocked_messages = []
        if len(_GLOBAL_ACTIVE_WORKFLOWS) >= get_appsettings().max_parallel_workflows:
            blocked_messages.append(
                f"Execution blocked by max workflows executing: {get_appsettings().max_parallel_workflows}"
            )

        # Check for workflows we depend on being complete
        blockers = _get_blocking_workflows(workflow_crd_model, index, logger)
        if blockers:
            messages = [
                f"Execution blocked by workflow we depend on:" f" '{wf}' with status '{wfstatus}'"
                for wf, wfstatus in blockers
            ]
            blocked_messages.extend(messages)

        if blocked_messages:
            patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS_MSG] = blocked_messages
            initial_workflow_status = WORKFLOWSTATUS.blocked
            logger.info(
                f"Initial status for workflow '{name}': '{initial_workflow_status}', messages={blocked_messages}"
            )
        else:
            assert isinstance(name, str)
            assert isinstance(namespace, str)
            _GLOBAL_ACTIVE_WORKFLOWS.add((namespace, name))
            logger.info(f"Initial status for workflow '{name}': '{initial_workflow_status}'")

        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = initial_workflow_status.value


@kopf.on.update(
    WorkflowCRD.group(),
    WorkflowCRD.version(),
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    new=WORKFLOWSTATUS.executing.value,
    param="workflow_status",
)  # noqa
@kopf.on.update(
    WorkflowCRD.group(), WorkflowCRD.version(), WorkflowCRD.plural(), field="status.children", param="sts_children"
)
async def workflow_children_update(name, meta, namespace, body, patch, **kwargs):
    if "deletion_timestamp" in meta:
        assert False, "Should not be called on deleted objects"

    index = kwargs["task_idx"]

    workflow_crd_model = WorkflowCRDModel(**body.spec)
    workflow_model = render_workflow_with_jinja2(name, workflow_crd_model, task_idx=index)
    try:
        G = build_nxdigraph(workflow_model.tasks)
    except WorkflowMalformed as e:
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.failed.value
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS_MSG] = str(e)
        raise kopf.PermanentError(e)

    # organize tasks
    task_status_sets: DefaultDict[TASKSTATUS | None, set[str]] = defaultdict(set)
    for task_item in workflow_model.tasks:
        task_name = task_item.name
        if (name, task_name) not in index:
            task_status_sets[None].add(task_name)
        else:
            task_body, *_ = index[(name, task_name)]
            task_status = TASKSTATUS(task_body.status.get(TaskCRD.STATUS_TASK_STATUS))
            task_status_sets[task_status].add(task_name)

    for task_name in reversed(list(topological_sort(G))):
        # check if there is no dependent tasks left to wait for
        if not descendants(G, task_name) - task_status_sets[TASKSTATUS.complete]:
            # Schedule if not already scheduled
            if task_name in task_status_sets[None]:
                # task_body, *_ = index[(name, task_name)]
                task_item = next(filter(lambda t: t.name == task_name, workflow_model.tasks))

                # Create the Task object, it will start executing on the create event handler
                _ = await create_cr_as_child(
                    namespace,
                    TaskCRD,
                    {
                        "metadata": {
                            "labels": {
                                TaskCRD.LABEL_WORKFLOW: name,
                                TaskCRD.LABEL_TASK_LOCAL_NAME: task_item.name,
                            }
                        },
                        "spec": task_item.model_dump(),
                        "status": {TaskCRD.STATUS_TASK_STATUS: TASKSTATUS.executing.value},
                    },
                )

                task_status_sets[None].remove(task_name)
                task_status_sets[TASKSTATUS.executing].add(task_name)

    statuscounter = {k: len(v) for k, v in task_status_sets.items() if v}
    if len(statuscounter) == 1 and TASKSTATUS.complete in statuscounter:
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.complete.value
    elif TASKSTATUS.failed in statuscounter:
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.failed.value
    patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(statuscounter)


async def _activate_blocked_workflows(index: kopf.Index, namespace, logger: Logger):
    unblock_wfs: list[str] = []
    for wfname, (wfbody, *_) in index.items():
        if len(unblock_wfs) + len(_GLOBAL_ACTIVE_WORKFLOWS) >= get_appsettings().max_parallel_workflows:
            break
        wfstatus = wfbody.status.get(WorkflowCRD.STATUS_WORKFLOW_STATUS)
        if wfstatus == WORKFLOWSTATUS.blocked.value:
            blockers = _get_blocking_workflows(WorkflowCRDModel(**wfbody.spec), index, logger)
            if not blockers:
                unblock_wfs.append(wfname)

    async def unblock(name: str):
        await get_custom_objects_api(mergepatch=True).patch_namespaced_custom_object(
            WorkflowCRD.group(),
            WorkflowCRD.version(),
            namespace,
            WorkflowCRD.plural(),
            name,
            {
                "status": {WorkflowCRD.STATUS_WORKFLOW_STATUS: WORKFLOWSTATUS.executing.value},
            },
        )
        _GLOBAL_ACTIVE_WORKFLOWS.add((namespace, name))

    await asyncio.gather(*[unblock(wfname) for wfname in unblock_wfs])


@kopf.on.update(
    WorkflowCRD.group(),
    WorkflowCRD.version(),
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    new=WORKFLOWSTATUS.complete.value,
)
async def workflow_completed(
    namespace: Optional[str],
    name,
    logger: Logger,
    **kwargs,
):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        index = kwargs["workflow_idx"]

        # Check if we can move any dependant workflows from blocked to pending
        assert isinstance(namespace, str)
        _GLOBAL_ACTIVE_WORKFLOWS.discard((namespace, name))
        await _activate_blocked_workflows(index, namespace, logger)


@kopf.on.delete(
    WorkflowCRD.group(),
    WorkflowCRD.version(),
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    value=lambda val, **_: val == WORKFLOWSTATUS.executing.value,
)
async def workflow_delete(namespace, name, logger, **kwargs):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        _GLOBAL_ACTIVE_WORKFLOWS.discard((namespace, name))
        index = kwargs["workflow_idx"]
        await _activate_blocked_workflows(index, namespace, logger)
