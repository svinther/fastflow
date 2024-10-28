import json
import os.path
from typing import Any, List, Type

import yaml
from pydantic import BaseModel
from yaml import Dumper, Loader

from fastflow.models import FastflowCRD, TaskCRD, WorkflowCRD


def _get_schema_norefs(model: Type[BaseModel]) -> dict[str, Any]:
    """This was copied from https://github.com/samuelcolvin/pydantic/issues/889#issuecomment-850312496
    The purpose is to eliminate the json_schema $ref references in the generated schema
    , as this is not allowed in kubernetes crds
    """

    def replace_value_in_dict(item, original_schema):
        if isinstance(item, list):
            return [replace_value_in_dict(i, original_schema) for i in item]
        elif isinstance(item, dict):
            if list(item.keys()) == ["$ref"]:
                definitions = item["$ref"][2:].split("/")
                res = original_schema.copy()
                for definition in definitions:
                    res = res[definition]
                return res
            else:
                return {key: replace_value_in_dict(i, original_schema) for key, i in item.items()}
        else:
            return item

    MAX_TRIES = 100
    schema: dict[str, Any] = model.model_json_schema()
    for i in range(MAX_TRIES):
        if "$ref" not in json.dumps(schema):
            break
        schema = replace_value_in_dict(schema.copy(), schema.copy())

    if "definitions" in schema:
        del schema["definitions"]
    return schema


builddir = os.path.join("chart/crds", "generated")
os.makedirs(builddir, exist_ok=True)

render_targets: List[Type[FastflowCRD]] = [TaskCRD, WorkflowCRD]

crd_template_yaml = """\
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: XXX.fastflow.dev
spec:
  scope: Namespaced
  group: XXX
  names:
    kind: XXX
    plural: XXX
    singular: XXX
    shortNames: []
  versions:
  - name: XXX
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            x-kubernetes-preserve-unknown-fields: true
          status:
            type: object
            x-kubernetes-preserve-unknown-fields: true

"""


def render():
    for target in render_targets:
        crd_template: dict = yaml.load(crd_template_yaml, Loader=Loader)
        crd_template["metadata"]["name"] = f"{target.plural()}.{target.group()}"
        crd_template["spec"]["group"] = target.group()
        crd_template["spec"]["names"]["kind"] = target.kind()
        crd_template["spec"]["names"]["plural"] = target.plural()
        crd_template["spec"]["names"]["singular"] = target.singular()
        crd_template["spec"]["versions"][0]["name"] = target.version()
        if target.model():
            crd_template["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"] = _get_schema_norefs(
                target.model()
            )
        if target.printer_columns():
            crd_template["spec"]["versions"][0]["additionalPrinterColumns"] = target.printer_columns()
        with open(os.path.join(builddir, f"{target.kind()}.yaml"), "w") as crd_final_open:
            yaml.dump(crd_template, crd_final_open, Dumper=Dumper)


if __name__ == "__main__":
    render()
