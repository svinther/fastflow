from time import sleep

from testutil import AbstractOperatorTest, get_cr, get_k8s_task_object

from fastflow.kubernetes import create_workflow_crd_object
from fastflow.models import WORKFLOWSTATUS, WorkflowCRD

global_inputs = {
    "siblings": [
        "anna",
        "lotti",
        "dinari",
        "juliari",
        "kana",
    ]
}

# language=yaml
dag_yaml = """\
{% for x in siblings %}
- name: emitter_{{ x }}
  impl: Echo
  inputs:
    args: [{{ x }}]
{% endfor %}

- name: collector
  impl: Echo
  inputs:
    args:
      - "Hello from:"
{% for x in siblings %}
      - " "
      - {{ "{{ tasks.emitter_" + x + ".outputs.stdout }}" }}
{% endfor %}
  dependencies:
{% for x in siblings %}
  - emitter_{{ x }}
{% endfor %}
"""

WFNAME = "advanced-template"

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

        # Check that the data is still intact, after using jinja2 to remove linebreaks
        collector_output = get_k8s_task_object(wf, "collector")["status"]["outputs"]["stdout"]
        assert collector_output == f"Hello from: {' '.join(global_inputs['siblings'])}"
