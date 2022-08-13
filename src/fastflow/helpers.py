from typing import Dict, List, Union

import yaml
from yaml import Loader

from fastflow.models import WorkflowCRD


def create_workflow_crd_object(
    name,
    dag: str,
    labels: Dict[str, str] = None,
    global_inputs: Union[dict, str] = None,
    generateName=False,
    workflow_dependencies: List[str] = None,
):
    if global_inputs is None:
        global_inputs = {}
    elif type(global_inputs) == str:
        global_inputs = yaml.load(global_inputs, Loader=Loader)

    labels = labels or {}

    metadata = {"labels": labels}
    if generateName:
        metadata["generateName"] = name
    else:
        metadata["name"] = name

    return {
        "apiVersion": f"{WorkflowCRD.group()}/{WorkflowCRD.version()}",
        "kind": WorkflowCRD.kind(),
        "metadata": metadata,
        "spec": {
            "global_inputs": global_inputs,
            "dag": dag,
            "dependencies": workflow_dependencies,
        },
    }
