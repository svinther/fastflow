import asyncio
from time import sleep
from typing import Any, Optional

from testutil import AbstractOperatorTest, get_cr

from fastflow import TaskImpl, TaskInputInt, TaskOutput, TaskResult
from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD


class TaskThatTakesMultipletAttemptsToComplete(TaskImpl):
    complete_in_retries: TaskInputInt = 0  # type: ignore
    retries: TaskOutput

    async def complete(self, name, meta, logger, retry, patch, workflow_idx, **_) -> Optional[TaskResult]:
        logger.info(f"Task {self.taskmodel.name}: Retry #{retry} - waiting for #{self.complete_in_retries}")
        await asyncio.sleep(1.0)

        if retry < self.complete_in_retries:
            return TaskResult(finished=False, message="Im not done yet")

        if meta["ownerReferences"][0]["name"] == "workflow-multiworkflow-1":
            wf2body, *_ = workflow_idx["workflow-multiworkflow-2"]
            wf2status = wf2body.status["workflow_status"]
            assert wf2status == WORKFLOWSTATUS.blocked.value, f"workflow2 status should be blocked, it is {wf2status}"

            wf3body, *_ = workflow_idx["workflow-multiworkflow-3"]
            wf3status = wf3body.status["workflow_status"]
            assert wf3status == WORKFLOWSTATUS.blocked.value, f"workflow3 status should be blocked, it is {wf3status}"

        elif meta["ownerReferences"][0]["name"] == "workflow-multiworkflow-2":
            wf1body, *_ = workflow_idx["workflow-multiworkflow-1"]
            wf1status = wf1body.status["workflow_status"]
            assert wf1status == WORKFLOWSTATUS.complete.value, f"workflow1 status should be complete, it is {wf1status}"

            wf3body, *_ = workflow_idx["workflow-multiworkflow-3"]
            wf3status = wf3body.status["workflow_status"]
            assert wf3status == WORKFLOWSTATUS.blocked.value, f"workflow3 status should be blocked, it is {wf3status}"

        elif meta["ownerReferences"][0]["name"] == "workflow-multiworkflow-3":
            wf1body, *_ = workflow_idx["workflow-multiworkflow-1"]
            wf1status = wf1body.status["workflow_status"]
            assert wf1status == WORKFLOWSTATUS.complete.value, f"workflow1 status should be complete, it is {wf1status}"

            wf2body, *_ = workflow_idx["workflow-multiworkflow-2"]
            wf2status = wf2body.status["workflow_status"]
            assert wf2status == WORKFLOWSTATUS.complete.value, f"workflow2 status should be complete, it is {wf2status}"

        return TaskResult(outputs={self.retries: retry})


global_inputs: dict[str, Any] = {}

# language=yaml
dag1_yaml = """\
- name: dummy001
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
  inputs:
    complete_in_retries: 1
{% for n in range(5) %}
- name: dummy{{ n }}
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
{% endfor %}
""".replace(
    "MODULE", __name__
)

workflow1 = create_workflow_crd_object(
    "workflow-multiworkflow-1",
    dag1_yaml,
    global_inputs=global_inputs,
    labels={"customer_name": "dummy"},
)

# language=yaml
dag2_yaml = """\
- name: dummy001
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
  inputs:
    complete_in_retries: 0
{% for n in range(10) %}
- name: dummy{{ n }}
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
{% endfor %}

""".replace(
    "MODULE", __name__
)

workflow2 = create_workflow_crd_object(
    "workflow-multiworkflow-2",
    dag2_yaml,
    global_inputs=global_inputs,
    labels={"customer_name": "dummy"},
    workflow_dependencies=["workflow-multiworkflow-1"],
)

# language=yaml
dag3_yaml = """\
- name: dummy001
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
  inputs:
    complete_in_retries: 0
{% for n in range(10) %}
- name: dummy{{ n }}
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
{% endfor %}

""".replace(
    "MODULE", __name__
)

workflow3 = create_workflow_crd_object(
    "workflow-multiworkflow-3",
    dag3_yaml,
    global_inputs=global_inputs,
    labels={"customer_name": "dummy"},
    workflow_dependencies=[
        "workflow-multiworkflow-1",
        "workflow-multiworkflow-2",
    ],
)


def test_it():
    with AbstractOperatorTest([workflow1, workflow2, workflow3]) as _:
        while True:
            sleep(2)
            wf = get_cr("workflow-multiworkflow-3", WorkflowCRD)
            wf_status_str = wf.get("status", {}).get("workflow_status", None)
            if wf_status_str:
                wf_status = WORKFLOWSTATUS(wf_status_str)
                if wf_status in (
                    WORKFLOWSTATUS.complete,
                    WORKFLOWSTATUS.failed,
                ):
                    break

        assert WORKFLOWSTATUS.complete == wf_status
