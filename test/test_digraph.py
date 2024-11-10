import base64
from time import sleep
from typing import Optional

from testutil import AbstractOperatorTest, get_cr, get_k8s_task_object

from fastflow import (
    TaskImpl,
    TaskInputInt,
    TaskInputStr,
    TaskOutput,
    TaskResult,
)
from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD


class TaskThatOutputsFavoriteFruit(TaskImpl):
    """Whatever fruit is set as inputs, it is output as favorite_fruit"""

    fruit: TaskInputStr
    favorite_fruit: TaskOutput

    async def complete(self, **_) -> Optional[TaskResult]:
        return TaskResult(outputs={self.favorite_fruit: self.fruit})


class TaskThatOutputsAllInputs(TaskImpl):
    """This task just returns all its inputs as outputs"""

    optional_input: TaskInputStr = "default_value"  # type: ignore

    async def complete(self, **_) -> Optional[TaskResult]:
        return TaskResult(outputs={TaskOutput(k): v for k, v in self.final_inputs.items()})


class TaskThatTakesMultipletAttemptsToComplete(TaskImpl):
    complete_in_retries: TaskInputInt
    retries: TaskOutput

    async def complete(self, name, logger, retry, patch, **_) -> Optional[TaskResult]:
        logger.info(f"Task {self.taskmodel.name}: Retry #{retry} - waiting for #{self.complete_in_retries}")
        if retry < self.complete_in_retries:
            return TaskResult(finished=False, message="Im not done yet")

        return TaskResult(outputs={self.retries: retry})


# language=yaml
global_inputs = {
    "customer": {"name": "customer001", "region": "we"},
    "fruit": "pommegrenade",
}

# language=yaml
dag_yaml = """\
- name: dummy001
  impl: MODULE.TaskThatOutputsFavoriteFruit
  inputs:
    fruit: {{ fruit }}


- name: dummy002
  impl: MODULE.TaskThatOutputsAllInputs
  inputs:
    favorite_fruit: {{ tasks.dummy001.outputs.favorite_fruit }}
    customer: {{ customer }}
    multiline: |-
      RXhhbXBsZXM6CiAgIyBTaG93IG1lcmdlZCBrdWJlY29uZmlnIHNldHRpbmdzCiAga3ViZWN0bCBj
      b25maWcgdmlldwoKICAjIFNob3cgbWVyZ2VkIGt1YmVjb25maWcgc2V0dGluZ3MgYW5kIHJhdyBj
      ZXJ0aWZpY2F0ZSBkYXRhCiAga3ViZWN0bCBjb25maWcgdmlldyAtLXJhdwoKICAjIEdldCB0aGUg
      cGFzc3dvcmQgZm9yIHRoZSBlMmUgdXNlcgogIGt1YmVjdGwgY29uZmlnIHZpZXcgLW8ganNvbnBh
      dGg9J3sudXNlcnNbPyhALm5hbWUgPT0gImUyZSIpXS51c2VyLnBhc3N3b3JkfScK
  dependencies:
    - "dummy001"

- name: dummy003
  impl: MODULE.TaskThatTakesMultipletAttemptsToComplete
  inputs:
    complete_in_retries: 2
  dependencies:
    - "dummy001"

- name: dummy004
  impl: MODULE.TaskThatOutputsAllInputs
  inputs:
    listitems:
      - {{ tasks.dummy002.outputs.customer.name }}
      - {{ tasks.dummy002.outputs.customer }}
      - {{ tasks.dummy003.outputs.retries }}
      - "{{ tasks.dummy003.outputs.retries }} {{tasks.dummy002.outputs.customer.region}}"
    multiline: |-
{{ tasks.dummy002.outputs.multiline | default('TO_BE_RESOLVED') | indent(width=6, first=True) }}
    lbr_replaced: {{ tasks.dummy002.outputs.multiline | default('TO_BE_RESOLVED') | replace('\\n', '') }}
  dependencies:
    - "dummy002"
    - "dummy003"

""".replace(
    "MODULE", __name__
)

WFNAME = "workflow-digraph-diamond"

workflow = create_workflow_crd_object(
    WFNAME,
    dag_yaml,
    global_inputs=global_inputs,
    labels={"customer_nam": "dummy"},
)


def test_it():
    with AbstractOperatorTest(workflow) as _:
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

        assert WORKFLOWSTATUS.complete == wf_status
        assert global_inputs["fruit"] == get_k8s_task_object(wf, "dummy001")["status"]["outputs"]["favorite_fruit"]

        # Dummy02 should output its inputs
        assert (
            get_k8s_task_object(wf, "dummy001")["status"]["outputs"]["favorite_fruit"]
            == get_k8s_task_object(wf, "dummy002")["status"]["outputs"]["favorite_fruit"]
        )

        # Check dummy02 complex output
        assert global_inputs["customer"] == get_k8s_task_object(wf, "dummy002")["status"]["outputs"]["customer"]

        # Check using complex output as input 1
        assert (
            global_inputs["customer"]["name"]
            == get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["listitems"][0]
        )

        # Check using complex output as input 2
        assert global_inputs["customer"] == get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["listitems"][1]

        # Check using output from another ancestor
        assert (
            get_k8s_task_object(wf, "dummy003")["spec"]["inputs"]["complete_in_retries"]
            == get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["listitems"][2]
        )

        # Check string interpolation
        dummy03_retries = get_k8s_task_object(wf, "dummy003")["spec"]["inputs"]["complete_in_retries"]
        assert (
            f'{dummy03_retries} {global_inputs["customer"]["region"]}'
            == get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["listitems"][3]
        )

        # Check we can handle multiline
        dummy002_multiline_input = get_k8s_task_object(wf, "dummy002")["spec"]["inputs"]["multiline"]
        dummy002_multiline_input_b64dec = base64.b64decode(dummy002_multiline_input)

        # This should be fine, as it never passed throught jinja2 rendering
        dummy002_multiline_output = get_k8s_task_object(wf, "dummy002")["status"]["outputs"]["multiline"]
        dummy002_multiline_output_b64dec = base64.b64decode(dummy002_multiline_output)
        assert dummy002_multiline_input_b64dec == dummy002_multiline_output_b64dec

        # This output was jinja2 rendered
        dummy004_multiline_output = get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["multiline"]
        dummy004_multiline_output_b64dec = base64.b64decode(dummy004_multiline_output)
        assert dummy002_multiline_input_b64dec == dummy004_multiline_output_b64dec

        # Check that the data is still intact, after using jinja2 to remove linebreaks
        dummy004_lbr_replaced_output = get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["lbr_replaced"]
        dummy004_lbr_replaced_output_b64dec = base64.b64decode(dummy004_lbr_replaced_output)
        assert dummy002_multiline_input_b64dec == dummy004_lbr_replaced_output_b64dec

        dummy004_optional_input_value = get_k8s_task_object(wf, "dummy004")["status"]["outputs"]["optional_input"]
        assert TaskThatOutputsAllInputs.optional_input == dummy004_optional_input_value
