from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Type, Union

import kopf
from kopf import Body, Logger, Meta, Patch
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi
from networkx import descendants, topological_sort

from .crds import create_cr_as_child, set_child_status_on_parent_cr
from .models import (
    TASKSTATUS,
    UNRESOLVED,
    WORKFLOWSTATUS,
    Task,
    TaskCRD,
    WorkflowCRD,
    WorkflowCRDModel,
    WorkflowMalformed,
    build_nxdigraph,
    create_status_patch,
    render_workflow_with_jinja2,
)


class TaskInput:
    pass


class TaskInputDict(dict, TaskInput):
    pass


class TaskInputList(list, TaskInput):
    pass


class TaskInputInt(int, TaskInput):
    pass


class TaskInputStr(str, TaskInput):
    pass


class TaskInputFloat(float, TaskInput):
    pass


class CancelledTaskInput:
    pass


# To be used as key for outputs
class TaskOutput(str):
    key: str


@dataclass()
class TaskResult:
    success: bool = True
    finished: bool = True
    outputs: Optional[Dict[TaskOutput, Any]] = None
    message: Optional[str] = None


DEFAULT_TASK_RESULT = TaskResult(success=True, finished=True)


# Abstract class for implementing arbitrary logic (tasks)
class TaskImpl:
    def __init__(self, taskmodel: Task, final_inputs: Dict[str, Any]):
        self.taskmodel = taskmodel
        self.final_inputs = deepcopy(final_inputs)

        for req_input, req_input_type in self.__class__.requires_inputs():
            if req_input in final_inputs:
                self.__setattr__(req_input, final_inputs[req_input])
            else:
                try:
                    default_value = getattr(self.__class__, req_input)
                    self.__setattr__(req_input, default_value)
                    self.final_inputs[req_input] = default_value
                except AttributeError:
                    raise WorkflowMalformed(
                        f"Required input '{req_input}' with no default value defined,"
                        f" for task '{taskmodel.name}' not found"
                    )

        # Initialize object to be used as a key
        for req_output, req_output_type in self.__class__.produces_outputs():
            self.__setattr__(req_output, req_output)

    @classmethod
    def requires_inputs(cls):
        return get_task_annotations(cls, TaskInput)

    @classmethod
    def produces_outputs(cls):
        return get_task_annotations(cls, TaskOutput)

    async def complete(self, **_) -> Optional[TaskResult]:
        pass


def get_task_annotations(
    clazz: Type[TaskImpl], inout_type: Union[Type[TaskInput], Type[TaskOutput]]
):
    # @todo in python 3.10 this can be replaced with inspect.get_annotations()
    # https://docs.python.org/3/library/inspect.html#inspect.get_annotations
    result: Dict[str, Tuple] = {}
    for c in reversed(clazz.mro()[:-1]):
        for n, t in c.__dict__.get("__annotations__", {}).items():
            if inout_type == TaskInput and t == CancelledTaskInput:
                del result[n]
            elif issubclass(t, inout_type):
                result[n] = (n, t)
    return result.values()


def try_get_class(kls: str) -> Type[TaskImpl]:
    parts = kls.split(".")
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    assert isinstance(m, type)
    if not issubclass(m, TaskImpl):
        raise WorkflowMalformed(
            f"impl must be a subclass of TaskImpl: '{kls}'"
        )
    return m


def get_class(kls: str) -> Type[TaskImpl]:
    default_module_prefixes = ["", "fastflow.tasks.", "fastflow.tasks.stdlib."]

    for prefix in default_module_prefixes:
        try:
            return try_get_class(prefix + kls)
        except (ValueError, AttributeError, ModuleNotFoundError) as e:
            pass

    raise WorkflowMalformed(f"No TaskImpl class found for '{kls}'")


def _get_dict_value(
    data: Dict[str, Any], valuepath: Tuple, fullpath: Tuple[str]
):
    if not isinstance(data, Dict):
        raise WorkflowMalformed(f"path '{fullpath}' not found")
    if len(valuepath) == 1:
        return data.get(valuepath[0])
    nested = data.get(valuepath[0])
    if not isinstance(nested, Dict):
        raise WorkflowMalformed(f"path '{fullpath}' not found")
    return _get_dict_value(nested, valuepath[1:], fullpath)


def _get_owner_digraph_name(task_metadata):
    return next(
        filter(
            lambda owner: owner["kind"] == WorkflowCRD.kind(),
            task_metadata["ownerReferences"],
        )
    ).get("name")


@kopf.index(WorkflowCRD.plural())
async def workflow_idx(name, body, **_):
    return {name: body}


@kopf.index(TaskCRD.plural())
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


def _format_statuscounter(statuscounter: Dict[TASKSTATUS, int]) -> str:
    return ", ".join([f"{k.value}:{v}" for k, v in statuscounter.items()])


def _get_blocking_workflows(
    workflow_crd_model: WorkflowCRDModel, index: kopf.Index, logger
):
    blockers = []
    # Check for workflows we depend on being complete
    if workflow_crd_model.dependencies:
        for dependency in workflow_crd_model.dependencies:
            if index.get(dependency) is None:
                logger.warn(
                    f"Ignoring workflow dependency for unknown workflow: '{dependency}'"
                )
                continue

            indexed = index.get(dependency)
            assert indexed is not None
            dep_wf_body, *_ = indexed
            dep_wf_status = dep_wf_body.status.get(
                WorkflowCRD.STATUS_WORKFLOW_STATUS
            )
            if dep_wf_status != WORKFLOWSTATUS.complete.value:
                blockers.append((dependency, dep_wf_status))
    return blockers


@kopf.on.create(WorkflowCRD.plural())
async def workflow_create(
    name: Optional[str],
    body: Body,
    meta: Meta,
    logger: Logger,
    patch: Patch,
    **kwargs,
):
    logger.info(f"Create new Workflow: '{meta.name}'")
    index = kwargs["workflow_idx"]
    workflow_crd_model = WorkflowCRDModel(**body.spec)
    initial_workflow_status = WORKFLOWSTATUS.pending

    # Check for workflows we depend on being complete
    blockers = _get_blocking_workflows(workflow_crd_model, index, logger)
    if blockers:
        messages = [
            f"Execution blocked by workflow we depend on:"
            f" '{wf}' with status '{wfstatus}'"
            for wf, wfstatus in blockers
        ]
        patch.status[WorkflowCRD.STATUS_WORKFLOW_STATUS_MSG] = messages
        initial_workflow_status = WORKFLOWSTATUS.blocked

    logger.info(
        f"Initial status for workflow '{name}': '{initial_workflow_status}'"
    )
    patch.status[
        WorkflowCRD.STATUS_WORKFLOW_STATUS
    ] = initial_workflow_status.value


@kopf.on.update(
    WorkflowCRD.plural(),
    field=f"status.{WorkflowCRD.STATUS_WORKFLOW_STATUS}",
    new=WORKFLOWSTATUS.complete.value,
)
async def workflow_completed(
    namespace: Optional[str],
    logger: Logger,
    **kwargs,
):
    index = kwargs["workflow_idx"]

    # Check if we can move any dependant workflows from blocked to pending
    for wfname, (wfbody, *_) in index.items():
        if (
            WORKFLOWSTATUS(wfbody.status[WorkflowCRD.STATUS_WORKFLOW_STATUS])
            == WORKFLOWSTATUS.blocked
        ):
            blockers = _get_blocking_workflows(
                WorkflowCRDModel(**wfbody.spec), index, logger
            )
            if not blockers:
                async with ApiClient() as api_client:
                    api_client.set_default_header(
                        "Content-Type", "application/merge-patch+json"
                    )
                    await CustomObjectsApi(
                        api_client
                    ).patch_namespaced_custom_object(
                        WorkflowCRD.group(),
                        WorkflowCRD.version(),
                        namespace,
                        WorkflowCRD.plural(),
                        wfbody.metadata.name,
                        {
                            "status": {
                                WorkflowCRD.STATUS_WORKFLOW_STATUS: WORKFLOWSTATUS.pending.value
                            },
                        },
                    )


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
    logger.info(f"Create new Workflow: '{meta.name}'")
    index = kwargs["task_idx"]

    workflow_crd_model = WorkflowCRDModel(**body.spec)
    workflow_model = render_workflow_with_jinja2(
        name, workflow_crd_model, task_idx=index
    )

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
                "spec": taskitem.dict(),
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

    patch.status[
        WorkflowCRD.STATUS_WORKFLOW_STATUS
    ] = WORKFLOWSTATUS.executing.value
    patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(
        statuscounter
    )


@kopf.on.update(WorkflowCRD.plural(), field="status.children")
async def workflow_children_update(
    name, namespace, body, patch, logger, **kwargs
):
    index = kwargs["task_idx"]

    workflow_crd_model = WorkflowCRDModel(**body.spec)
    workflow_model = render_workflow_with_jinja2(
        name, workflow_crd_model, task_idx=index
    )
    try:
        G = build_nxdigraph(workflow_model.tasks)
    except WorkflowMalformed as e:
        patch.status[
            WorkflowCRD.STATUS_WORKFLOW_STATUS
        ] = WORKFLOWSTATUS.failed.value
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
        patch.status[
            WorkflowCRD.STATUS_WORKFLOW_STATUS
        ] = WORKFLOWSTATUS.failed.value
        statuscounter = {k: len(v) for k, v in task_status_sets.items() if v}
        patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(
            statuscounter
        )
        raise kopf.PermanentError("Failed tasks")

    for task_name in reversed(list(topological_sort(G))):
        # check if there is no dependent tasks left to wait for
        if (
            not descendants(G, task_name)
            - task_status_sets[TASKSTATUS.complete]
        ):
            # Schedule if not already scheduled
            if task_name in task_status_sets[TASKSTATUS.pending]:
                task_body, *_ = index[(name, task_name)]
                task_model = next(
                    filter(lambda t: t.name == task_name, workflow_model.tasks)
                )
                logger.info(
                    f"Task status for '{name}'/'{task_body.spec['name']}':"
                    f" '{task_body.status[TaskCRD.STATUS_TASK_STATUS]}'"
                    f"-->'{TASKSTATUS.ready.value}'"
                )

                async with ApiClient() as api_client:
                    api_client.set_default_header(
                        "Content-Type", "application/merge-patch+json"
                    )

                    await CustomObjectsApi(
                        api_client
                    ).patch_namespaced_custom_object(
                        TaskCRD.group(),
                        TaskCRD.version(),
                        namespace,
                        TaskCRD.plural(),
                        task_body.metadata.name,
                        body={
                            "spec": task_model.dict(),
                            "status": {
                                TaskCRD.STATUS_TASK_STATUS: TASKSTATUS.ready.value
                            },
                        },
                    )
                task_status_sets[TASKSTATUS.pending].remove(task_name)
                task_status_sets[TASKSTATUS.ready].add(task_name)

    statuscounter = {k: len(v) for k, v in task_status_sets.items() if v}
    if len(statuscounter) == 1 and TASKSTATUS.complete in statuscounter:
        patch.status[
            WorkflowCRD.STATUS_WORKFLOW_STATUS
        ] = WORKFLOWSTATUS.complete.value
    patch.status[WorkflowCRD.STATUS_TASKS_SUMMARY] = _format_statuscounter(
        statuscounter
    )


@kopf.on.update(
    TaskCRD.plural(),
    field=f"status.{TaskCRD.STATUS_TASK_STATUS}",
    new=TASKSTATUS.ready.value,
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
    assert status[TaskCRD.STATUS_TASK_STATUS] == TASKSTATUS.ready.value
    index = kwargs["workflow_idx"]

    task_model = Task(**spec)
    workflow_body, *_ = index[_get_owner_digraph_name(meta)]
    if (
        workflow_body.status[WorkflowCRD.STATUS_WORKFLOW_STATUS]
        == WORKFLOWSTATUS.failed.value
    ):
        patch.status[TaskCRD.STATUS_TASK_STATUS] = TASKSTATUS.blocked.value
        raise kopf.PermanentError(
            f"Task '{task_model.name}' blocked because workflow has status failed"
        )

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

    if not type(task_result) == TaskResult:
        logger.info(
            f"Set permanent task_status = failed for task '{task_model.name}'"
        )
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
        raise kopf.TemporaryError(
            f"Task '{task_model.name}' impl: '{task_model.impl}' did not complete, reschedule in 5 secs",
            delay=5.0,
        )

    task_outputs = task_result.outputs or {}

    for declared_output_name, declared_output_type in (
        task_impl_class.produces_outputs() or []
    ):
        if declared_output_name not in task_outputs.keys():
            logger.warn(
                f"Task '{task_model.name}' class '{task_model.impl}'"
                f" declares output '{declared_output_name}' - was not found"
            )

    # The outputs from the task impl comes with keys as class TaskOutput
    # Convert keys to their string repr
    patch.status.setdefault("outputs", {}).update(task_outputs)

    task_status = (
        TASKSTATUS.complete if task_result.success else TASKSTATUS.failed
    )
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
    old=kopf.PRESENT,
    new=kopf.PRESENT,
)
async def task_status_updated(body, meta, spec, logger, old, new, **_):
    workflow_name = _get_owner_digraph_name(meta)
    new_task_status = TASKSTATUS(new)
    finished = new_task_status in (TASKSTATUS.failed, TASKSTATUS.complete)
    success = new_task_status == TASKSTATUS.complete

    logger.info(
        f" Task status transition '{workflow_name}'/'{spec['name']}':"
        f" '{old}'-->'{new}'"
        f" success: {success} finished: {finished}"
    )
    await set_child_status_on_parent_cr(
        body,
        success,
        finished,
        extra_fields={
            "task_local_name": spec["name"],
            "task_status": new,
        },
    )
