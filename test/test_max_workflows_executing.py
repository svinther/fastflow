import uuid
from collections import defaultdict
from time import sleep
from typing import Optional

from testutil import (
    AbstractOperatorTest,
    cleanup_workflow_objects,
    create_k8s_workflows_from_manifests,
    delete_k8s_workflow_by_manifest_names,
    get_crs,
)

from fastflow import TaskImpl, TaskResult
from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD
from fastflow.setup import get_appsettings

_NUM_WORKFLOWS = 25
_PARALLEL_WORKFLOWS = 10
_CONTINUE = False


class TaskWaitForAllWorkflowsStatus(TaskImpl):
    """This task just returns all its inputs as outputs"""

    async def complete(self, logger, **_) -> Optional[TaskResult]:
        global _CONTINUE
        if not _CONTINUE:
            return TaskResult(finished=False)


dag_yaml = """\
  - name: waitforallworkflowsstatus
    impl: MODULE.TaskWaitForAllWorkflowsStatus
""".replace(
    "MODULE", __name__
)


def create_the_workflows():
    global _CONTINUE
    _CONTINUE = False

    cleanup_workflow_objects()

    randomlabel = str(uuid.uuid4())

    workflows = [
        create_workflow_crd_object(
            "maxparallel",
            dag_yaml,
            generateName=True,
            labels={"randomlabel": randomlabel},
        )
        for _ in range(_NUM_WORKFLOWS)
    ]

    create_k8s_workflows_from_manifests(workflows)

    return randomlabel


def get_workflow_status(randomlabel) -> dict[WORKFLOWSTATUS | None, list[dict]]:
    workflow_status = defaultdict(list)
    test_workflows = get_crs(WorkflowCRD, label_selector=f"randomlabel={randomlabel}")
    for wf in test_workflows["items"]:
        wf_status_str = wf.get("status", {}).get(WorkflowCRD.STATUS_WORKFLOW_STATUS)
        if wf_status_str:
            workflow_status[WORKFLOWSTATUS(wf_status_str)].append(wf)
        else:
            workflow_status[None].append(wf)
    return workflow_status


def monitor_the_workflows(randomlabel):
    global _NUM_WORKFLOWS, _PARALLEL_WORKFLOWS, _CONTINUE
    get_appsettings().max_parallel_workflows = _PARALLEL_WORKFLOWS

    for i in range(600):
        sleep(0.5)
        workflow_status = get_workflow_status(randomlabel)
        if len(workflow_status[None]) == 0:
            running = len(workflow_status[WORKFLOWSTATUS.executing])
            completed = len(workflow_status[WORKFLOWSTATUS.complete])
            remaining = _NUM_WORKFLOWS - completed
            if remaining == 0:
                break

            assert (
                min(_PARALLEL_WORKFLOWS, remaining) >= running
            ), f"running={running}, completed={completed}, remaining={remaining}"

            _CONTINUE = True


def test_operator_starting_after_workflow_objects_created():
    randomlabel = create_the_workflows()
    with AbstractOperatorTest() as _:
        monitor_the_workflows(randomlabel)


def test_operator_starting_before_workflow_objects_created():
    with AbstractOperatorTest() as _:
        randomlabel = create_the_workflows()
        monitor_the_workflows(randomlabel)


def test_delete_executing_workflow():
    with AbstractOperatorTest() as _:
        randomlabel = create_the_workflows()
        deleted = 0
        for i in range(600):
            sleep(0.5)
            workflow_status = get_workflow_status(randomlabel)
            if len(workflow_status[None]) == 0:
                executing_wfs = [
                    wf for wf in workflow_status[WORKFLOWSTATUS.executing] if "deletionTimestamp" not in wf["metadata"]
                ]
                delete_k8s_workflow_by_manifest_names(executing_wfs)
                deleted += len(executing_wfs)

                completed = len(workflow_status[WORKFLOWSTATUS.complete])
                if deleted + completed == _NUM_WORKFLOWS:
                    break
