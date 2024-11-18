from kubernetes.client import CustomObjectsApi
from testutil import (
    OPERATOR_NAMESPACE,
    AbstractOperatorTest,
    get_api_client,
    get_cr,
    wait_for_workflow_status,
)

from fastflow import TaskImpl, TaskResult
from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import TASKSTATUS, WORKFLOWSTATUS, TaskCRD, WorkflowCRD

_TASK_RESPONSE = TaskResult(finished=True, success=False, message="Permanent failure")


class FailingTask(TaskImpl):
    async def complete(self, **_) -> TaskResult:
        global _TASK_RESPONSE
        return _TASK_RESPONSE


_NEVER_COMPLETE = True


class NeverCompleteTask(TaskImpl):
    async def complete(self, **_) -> TaskResult:
        global _NEVER_COMPLETE
        if _NEVER_COMPLETE:
            return TaskResult(finished=False)
        return TaskResult(finished=True)


# language=yaml
workflow_yaml = """\
- name: failing_task
  impl: MODULE.FailingTask
  inputs:
     customer_name: "nosuchcustomer"

- name: never_complete_task
  impl: MODULE.NeverCompleteTask


""".replace(
    "MODULE", __name__
)

WORKFLOW_NAME = "resurrect"

workflow = create_workflow_crd_object(
    WORKFLOW_NAME,
    workflow_yaml,
)


def test_it():
    with AbstractOperatorTest(workflow) as _:
        wf = wait_for_workflow_status(WORKFLOW_NAME, (WORKFLOWSTATUS.failed, WORKFLOWSTATUS.complete))
        wf_status_str = wf.get("status", {}).get("workflow_status", None)
        assert WORKFLOWSTATUS.failed.value == wf_status_str

        restart_task = None
        for task in wf["status"]["children"].values():
            if task["task_local_name"] == "failing_task":
                assert task["task_status"] == TASKSTATUS.failed.value
                restart_task = task
            if task["task_local_name"] == "never_complete_task":
                assert task["task_status"] == TASKSTATUS.ready.value
        assert restart_task is not None

        # Restart the failed task
        global _TASK_RESPONSE
        _TASK_RESPONSE = TaskResult(finished=True, success=True)
        restart_task_name = restart_task["name"]
        CustomObjectsApi(get_api_client()).delete_namespaced_custom_object(
            TaskCRD.group(), TaskCRD.version(), OPERATOR_NAMESPACE, TaskCRD.plural(), restart_task_name
        )
        # set_task_status(restart_task_name, TASKSTATUS.executing)

        # Allow the never complete task to complete
        global _NEVER_COMPLETE
        _NEVER_COMPLETE = False

        wf = get_cr(WORKFLOW_NAME, WorkflowCRD)
        wf = wait_for_workflow_status(WORKFLOW_NAME, (WORKFLOWSTATUS.complete,))
        assert wf["status"]["workflow_status"] == WORKFLOWSTATUS.complete.value
