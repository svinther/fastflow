import json
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Type

import networkx as nx
import yaml
from jinja2 import Template, Undefined
from networkx import DiGraph, is_directed_acyclic_graph
from pydantic import BaseModel, Field
from yaml import Loader


class FastflowCRD:
    PERMANENT_ERROR = "permanent_error"

    @classmethod
    def group(cls) -> str:
        return "fastflow.dev"

    @classmethod
    def version(cls) -> str:
        return "beta-1"

    @classmethod
    def kind(cls) -> str:
        cls_name = cls.__name__
        if cls_name.lower().endswith("crd"):
            cls_name = cls_name[:-3]
        return cls_name

    @classmethod
    def model(cls) -> Optional[Type[BaseModel]]:
        return None

    @classmethod
    def singular(cls) -> str:
        return cls.kind().lower()

    @classmethod
    def plural(cls) -> str:
        return f"{cls.singular()}s"

    @classmethod
    def printer_columns(cls) -> Optional[List[Dict[str, str]]]:
        return None


@unique
class WORKFLOWSTATUS(Enum):
    blocked = "blocked"
    executing = "executing"
    complete = "complete"
    failed = "failed"


@unique
class TASKSTATUS(Enum):
    executing = "executing"
    complete = "complete"
    failed = "failed"
    blocked = "blocked"


class TaskCRDModel(BaseModel):
    name: str = Field(pattern="^[a-zA-Z_][a-zA-Z0-9_]+$")


class TaskCRD(FastflowCRD):
    LABEL_WORKFLOW = "workflow"
    LABEL_TASK_LOCAL_NAME = "task_local_name"
    STATUS_TASK_STATUS = "task_status"
    STATUS_TASK_MESSAGES = "task_messages"

    @classmethod
    def printer_columns(cls) -> List[Dict[str, str]]:
        return [
            {
                "name": "Local Name",
                "type": "string",
                "description": "Name of task, only locally uniq within parent Workflow",
                "jsonPath": ".spec.name",
            },
            {
                "name": "Status",
                "type": "string",
                "description": "Task status, one of [pending, ready, completed, failed]",
                "jsonPath": ".status.task_status",
            },
            {
                "name": "Age",
                "type": "date",
                "jsonPath": ".metadata.creationTimestamp",
            },
            {
                "name": "Workflow",
                "type": "string",
                "jsonPath": ".metadata.labels.workflow",
            },
        ]


class WorkflowCRDModel(BaseModel):
    global_inputs: Optional[Dict[str, Any]] = {}
    dag: str
    dependencies: Optional[List[str]] = None


class WorkflowCRD(FastflowCRD):
    STATUS_TASKS_SUMMARY = "tasks_summary"
    STATUS_WORKFLOW_STATUS = "workflow_status"
    STATUS_WORKFLOW_STATUS_MSG = "workflow_status_msg"

    @classmethod
    def printer_columns(cls) -> List[Dict[str, str]]:
        return [
            {
                "name": "Tasks Summary",
                "type": "string",
                "description": "Summary of task statuses",
                "jsonPath": ".status.tasks_summary",
            },
            {
                "name": "Status",
                "type": "string",
                "description": "Task status, one of [pending, ready, completed, failed]",
                "jsonPath": ".status.workflow_status",
            },
            {
                "name": "Age",
                "type": "date",
                "jsonPath": ".metadata.creationTimestamp",
            },
        ]


all_crds: List[Type[FastflowCRD]] = [TaskCRD, WorkflowCRD]


class UnknownCRD(BaseException):
    pass


class Task(BaseModel):
    name: str = Field(pattern="^[a-zA-Z_][a-zA-Z0-9_]+$")
    impl: str
    inputs: Dict[str, Any] = {}
    dependencies: Optional[List[str]] = None


class Workflow(BaseModel):
    global_inputs: Optional[Dict[str, Any]] = {}
    tasks: List[Task]


class WorkflowMalformed(Exception):
    pass


def build_nxdigraph(task_items: List[Task]) -> DiGraph:
    G = nx.DiGraph()

    task_names = {task_item.name for task_item in task_items}

    for ti in task_items:
        G.add_node(ti.name)
        for dep_ti_tname in ti.dependencies or []:
            if dep_ti_tname not in task_names:
                raise WorkflowMalformed(f"Task '{ti.name}' declares dependency to unknown task '{dep_ti_tname}'")
            G.add_edge(ti.name, dep_ti_tname)

    if not is_directed_acyclic_graph(G):
        raise WorkflowMalformed("Not a directed acyclic graph")
    return G


class UNRESOLVED(Undefined):
    MARKER = "_U-N_R_E_S_O_L_V_E_D_"

    def __getattr__(self, _: str) -> Any:
        return self

    def __str__(self) -> str:
        return UNRESOLVED.MARKER

    __getitem__ = __getattr__  # type: ignore


# output __str__ as json, so it can be loaded by standard yaml parser
class jsondict(dict):
    def __str__(self) -> str:
        return json.dumps(self)


def to_jsondict(o):
    if isinstance(o, dict):
        new_d = jsondict()
        for k, v in o.items():
            new_d[k] = to_jsondict(v)
        return new_d
    elif isinstance(o, list):
        return [to_jsondict(li) for li in o]
    else:
        return o


def render_multiple_passes(template: str, **kwargs):
    while True:
        template_result_ = Template(template, undefined=UNRESOLVED).render(**kwargs)
        if template_result_ == template:
            return template_result_
        template = template_result_


def render_workflow_with_jinja2(
    workflow_name: str,
    workflow_model_crd: WorkflowCRDModel,
    task_idx=None,  # <-- Optional[kopf.Index] with no real runtime dependency to any kopf stuff
) -> Workflow:
    # The dag is not valid yaml because it contains jinja2 template expressions
    # a: {{ expr }}
    #
    # So we render the dag string, replacing all the jinja2 exprs with empty strings, as this produces valid yaml
    # a: "_U-N_R_E_S_O_L_V_E_D_"
    #
    # Then the tasks in the dag can be known, and the references between tasks can be established
    # The references can then be fed to a 2.nd render of the dag string

    # The global variables, these will not be rendered by templating
    global_inputs = to_jsondict(workflow_model_crd.global_inputs)

    # (1) Render a valid yaml list of tasks, from the workflow dag
    tasks = yaml.load(
        render_multiple_passes(workflow_model_crd.dag, **global_inputs),
        Loader=Loader,
    )
    # (2) Use the list of tasks as the initial version of a workflow
    workflow = Workflow(tasks=tasks, global_inputs=global_inputs)

    # (3) Collect outputs of tasks that was already executed
    task_outputs: Dict[str, dict] = {}
    if task_idx:
        for task in workflow.tasks:
            try:
                ref_task_body, *_ = task_idx[(workflow_name, task.name)]
                # Update with actual the actual task output values
                for k, v in ref_task_body.status.get("outputs", {}).items():
                    task_outputs.setdefault(task.name, {}).setdefault("outputs", {})[k] = v
            except KeyError:
                pass

    # (4) Render new valid yaml list of tasks, now with the task outputs known
    tasks = yaml.load(
        render_multiple_passes(
            workflow_model_crd.dag,
            tasks=to_jsondict(task_outputs),
            **global_inputs,
        ),
        Loader=Loader,
    )

    # (5) New version of the workflow
    return Workflow(
        tasks=tasks,
        global_inputs=global_inputs,
    )


def get_crd_by_kind(kind: str) -> Type[FastflowCRD]:
    for crd in all_crds:
        if crd.kind() == kind:
            return crd
    raise UnknownCRD(f"Unknown kind '{kind}'")
