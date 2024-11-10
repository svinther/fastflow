from typing import Any, Dict, Tuple

from fastflow.models import WorkflowCRD, WorkflowMalformed


def _get_dict_value(data: Dict[str, Any], valuepath: Tuple, fullpath: Tuple[str]):
    if not isinstance(data, Dict):
        raise WorkflowMalformed(f"path '{fullpath}' not found")
    if len(valuepath) == 1:
        return data.get(valuepath[0])
    nested = data.get(valuepath[0])
    if not isinstance(nested, Dict):
        raise WorkflowMalformed(f"path '{fullpath}' not found")
    return _get_dict_value(nested, valuepath[1:], fullpath)


def _get_owner_digraph_name(task_metadata):
    return next(
        filter(
            lambda owner: owner["kind"] == WorkflowCRD.kind(),
            task_metadata["ownerReferences"],
        )
    ).get("name")
