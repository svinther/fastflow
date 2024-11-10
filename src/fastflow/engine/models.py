from abc import abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Type, Union

from fastflow.models import Task, WorkflowMalformed


class TaskInput:
    pass


class TaskInputDict(dict, TaskInput):
    pass


class TaskInputList(list, TaskInput):
    pass


class TaskInputInt(int, TaskInput):
    pass


class TaskInputStr(str, TaskInput):
    pass


class TaskInputFloat(float, TaskInput):
    pass


class CancelledTaskInput:
    pass


class TaskOutput(str):
    key: str


@dataclass()
class TaskResult:
    success: bool = True
    finished: bool = True
    outputs: Optional[Dict[TaskOutput, Any]] = None
    message: Optional[str] = None
    delay_retry: float | None = None


class TaskImpl:
    def __init__(self, taskmodel: Task, final_inputs: Dict[str, Any]):
        self.taskmodel = taskmodel
        self.final_inputs = deepcopy(final_inputs)

        for req_input, req_input_type in self.__class__.requires_inputs():
            if req_input in final_inputs:
                self.__setattr__(req_input, final_inputs[req_input])
            else:
                try:
                    default_value = getattr(self.__class__, req_input)
                    self.__setattr__(req_input, default_value)
                    self.final_inputs[req_input] = default_value
                except AttributeError:
                    raise WorkflowMalformed(
                        f"Required input '{req_input}' with no default value defined,"
                        f" for task '{taskmodel.name}' not found"
                    )

        # Initialize object to be used as a key
        for req_output, req_output_type in self.__class__.produces_outputs():
            self.__setattr__(req_output, req_output)

    @classmethod
    def requires_inputs(cls):
        return get_task_annotations(cls, TaskInput)

    @classmethod
    def produces_outputs(cls):
        return get_task_annotations(cls, TaskOutput)

    @abstractmethod
    async def complete(self, *args, **kwargs) -> Optional[TaskResult]:
        raise NotImplementedError


def get_task_annotations(clazz: Type[TaskImpl], inout_type: Union[Type[TaskInput], Type[TaskOutput]]):
    # @todo in python 3.10 this can be replaced with inspect.get_annotations()
    # https://docs.python.org/3/library/inspect.html#inspect.get_annotations
    result: Dict[str, Tuple] = {}
    for c in reversed(clazz.mro()[:-1]):
        for n, t in c.__dict__.get("__annotations__", {}).items():
            if inout_type == TaskInput and t == CancelledTaskInput:
                del result[n]
            elif issubclass(t, inout_type):
                result[n] = (n, t)
    return result.values()


def try_get_class(kls: str) -> Type[TaskImpl]:
    parts = kls.split(".")
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    assert isinstance(m, type)
    if not issubclass(m, TaskImpl):
        raise WorkflowMalformed(f"impl must be a subclass of TaskImpl: '{kls}'")
    return m


def get_class(kls: str) -> Type[TaskImpl]:
    default_module_prefixes = ["", "fastflow.tasks.", "fastflow.tasks.stdlib."]

    for prefix in default_module_prefixes:
        try:
            return try_get_class(prefix + kls)
        except (ValueError, AttributeError, ModuleNotFoundError):
            pass

    raise WorkflowMalformed(f"No TaskImpl class found for '{kls}'")
