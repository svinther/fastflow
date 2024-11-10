from time import sleep
from typing import Any

from testutil import AbstractOperatorTest, get_cr

from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD

global_inputs: dict[str, Any] = {}

# list of tasks, yaml + jinja2 expressions
# language=yaml
workflow_yaml = """\
- name: some_task
  impl: LookupCustomer
  inputs:
     customer_name: "nosuchcustomer"
  dependencies:
    - wrong_dependency
"""

WORKFLOW_NAME = "missing-dep"

workflow = create_workflow_crd_object(
    WORKFLOW_NAME,
    workflow_yaml,
    global_inputs=global_inputs,
    labels={"customer_name": "xxxxx"},
)


def test_it():
    with AbstractOperatorTest(workflow) as _:
        while True:
            sleep(2)
            wf = get_cr(WORKFLOW_NAME, WorkflowCRD)
            wf_status_str = wf.get("status", {}).get("workflow_status", None)
            if wf_status_str:
                wf_status = WORKFLOWSTATUS(wf_status_str)
                if wf_status in (
                    WORKFLOWSTATUS.complete,
                    WORKFLOWSTATUS.failed,
                ):
                    break

    assert WORKFLOWSTATUS.failed == wf_status
