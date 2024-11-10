"""Kopf handlers for Workflow CRD"""

import pathlib
from time import sleep

from testutil import AbstractOperatorTest, get_cr

from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD

# language=yaml
dag_yaml = """\
- name: testcustomtasks
  impl: testtasks.Sleep
  inputs:
    howlong: 1
"""

WFNAME = "testcustomtasks"

workflow = create_workflow_crd_object(
    WFNAME,
    dag_yaml,
    labels={"customer_nam": "dummy"},
)


def test_it():
    with AbstractOperatorTest(
        workflow, paths=[str(pathlib.Path(__file__).parents[0] / "customtasks" / "testtasks.py")]
    ) as _:
        while True:
            sleep(2)
            wf = get_cr(WFNAME, WorkflowCRD)
            wf_status_str = wf.get("status", {}).get("workflow_status", None)
            if wf_status_str:
                wf_status = WORKFLOWSTATUS(wf_status_str)
                if wf_status in (
                    WORKFLOWSTATUS.complete,
                    WORKFLOWSTATUS.failed,
                ):
                    break

        assert wf_status == WORKFLOWSTATUS.complete
