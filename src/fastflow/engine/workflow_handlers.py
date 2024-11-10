import asyncio
from collections import defaultdict
from typing import Optional

import kopf
from kopf import Body, Logger, Patch
from networkx import descendants, topological_sort

from fastflow.kubernetes import (
    create_cr_as_child,
    create_status_patch,
    get_custom_objects_api,
)
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
    return ", ".join([f"{k.value}:{v}" for k, v in statuscounter.items()])


def _get_num_workflows_executing(index: kopf.Index):
    answer = 0
    for wfname, (wfbody, *_) in index.items():
        wf_status = wfbody.status.get(WorkflowCRD.STATUS_WORKFLOW_STATUS)
        if wf_status and wf_status in (
            WORKFLOWSTATUS.executing.value,
            WORKFLOWSTATUS.pending.value,
        ):
            answer += 1
    return answer


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
        if wf_status and wf_status in (
            WORKFLOWSTATUS.executing.value,
            WORKFLOWSTATUS.pending.value,
        ):
            _GLOBAL_ACTIVE_WORKFLOWS.add((wf["metadata"]["namespace"], wf["metadata"]["name"]))


@kopf.index(WorkflowCRD.plural())
async def workflow_idx(name, body, **_):
    return {name: body}


@kopf.on.create(WorkflowCRD.plural())
async def workflow_create(
    namespace: str | None,
    name: str | None,
    body: Body,
    logger: Logger,
    patch: Patch,
    **kwargs,
):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        logger.info(f"Create new Workflow: '{name}'")
        index = kwargs["workflow_idx"]
        workflow_crd_model = WorkflowCRDModel(**body.spec)
        initial_workflow_status = WORKFLOWSTATUS.pending

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
                "status": {WorkflowCRD.STATUS_WORKFLOW_STATUS: WORKFLOWSTATUS.pending.value},
            },
        )
        _GLOBAL_ACTIVE_WORKFLOWS.add((namespace, name))

    await asyncio.gather(*[unblock(wfname) for wfname in unblock_wfs])


@kopf.on.update(
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    new=WORKFLOWSTATUS.complete.value,
)
async def workflow_completed(
    namespace: Optional[str],
    name,
    logger: Logger,
    memo: kopf.Memo,
    **kwargs,
):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        index = kwargs["workflow_idx"]

        # Check if we can move any dependant workflows from blocked to pending
        assert isinstance(namespace, str)
        _GLOBAL_ACTIVE_WORKFLOWS.discard((namespace, name))
        await _activate_blocked_workflows(index, namespace, logger)


@kopf.on.update(
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    new=WORKFLOWSTATUS.pending.value,
)
async def workflow_update(
    name,
    body,
    meta,
    namespace,
    logger,
    patch,
    **kwargs,
):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        logger.info(f"Create new Workflow: '{meta.name}'")
        index = kwargs["task_idx"]

        workflow_crd_model = WorkflowCRDModel(**body.spec)
        workflow_model = render_workflow_with_jinja2(name, workflow_crd_model, task_idx=index)

        statuscounter = defaultdict(int)

        for taskitem in workflow_model.tasks:
            task_status = TASKSTATUS.pending
            statuscounter[task_status] += 1

            task_cr = await create_cr_as_child(
                namespace,
                TaskCRD,
                {
                    "metadata": {
                        "labels": {
                            TaskCRD.LABEL_WORKFLOW: name,
                            TaskCRD.LABEL_TASK_LOCAL_NAME: taskitem.name,
                        }
                    },
                    "spec": taskitem.model_dump(),
                    "status": {TaskCRD.STATUS_TASK_STATUS: task_status.value},
                },
            )

            patch.status.setdefault("children", {}).update(
                create_status_patch(
                    task_cr,
                    False,
                    False,
                    extra_fields={
                        "task_local_name": taskitem.name,
                        "task_status": task_status.value,
                    },
                )
            )

        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.executing.value
        patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(statuscounter)


@kopf.on.update(WorkflowCRD.plural(), field="status.children")
async def workflow_children_update(memo: kopf.Memo, name, namespace, body, patch, logger, **kwargs):
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
    task_status_sets = defaultdict(set)
    for task_item in workflow_model.tasks:
        task_name = task_item.name
        task_body, *_ = index[(name, task_name)]

        task_status = TASKSTATUS(task_body.status[TaskCRD.STATUS_TASK_STATUS])
        task_status_sets[task_status].add(task_name)

    # fail fast
    if task_status_sets[TASKSTATUS.failed]:
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.failed.value
        statuscounter = {k: len(v) for k, v in task_status_sets.items() if v}
        patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(statuscounter)
        raise kopf.PermanentError("Failed tasks")

    for task_name in reversed(list(topological_sort(G))):
        # check if there is no dependent tasks left to wait for
        if not descendants(G, task_name) - task_status_sets[TASKSTATUS.complete]:
            # Schedule if not already scheduled
            if task_name in task_status_sets[TASKSTATUS.pending]:
                task_body, *_ = index[(name, task_name)]
                task_model = next(filter(lambda t: t.name == task_name, workflow_model.tasks))
                logger.info(
                    f"Task status for '{name}'/'{task_body.spec['name']}':"
                    f" '{task_body.status[TaskCRD.STATUS_TASK_STATUS]}'"
                    f"-->'{TASKSTATUS.ready.value}'"
                )

                await get_custom_objects_api(mergepatch=True).patch_namespaced_custom_object(
                    TaskCRD.group(),
                    TaskCRD.version(),
                    namespace,
                    TaskCRD.plural(),
                    task_body.metadata.name,
                    body={
                        "spec": task_model.model_dump(),
                        "status": {TaskCRD.STATUS_TASK_STATUS: TASKSTATUS.ready.value},
                    },
                )
                task_status_sets[TASKSTATUS.pending].remove(task_name)
                task_status_sets[TASKSTATUS.ready].add(task_name)

    statuscounter = {k: len(v) for k, v in task_status_sets.items() if v}
    if len(statuscounter) == 1 and TASKSTATUS.complete in statuscounter:
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS] = WORKFLOWSTATUS.complete.value
    patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(statuscounter)


@kopf.on.delete(
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    value=lambda val, **_: val in (WORKFLOWSTATUS.pending.value, WORKFLOWSTATUS.executing.value),
)
async def workflow_delete(namespace, name, logger, **kwargs):
    global _GLOBAL_WORKFLOW_LOCK
    async with _GLOBAL_WORKFLOW_LOCK:
        _GLOBAL_ACTIVE_WORKFLOWS.discard((namespace, name))
        index = kwargs["workflow_idx"]
        await _activate_blocked_workflows(index, namespace, logger)
