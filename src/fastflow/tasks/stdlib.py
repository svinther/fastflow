from typing import Optional

from fastflow.engine import (
    TaskImpl,
    TaskInputList,
    TaskInputStr,
    TaskOutput,
    TaskResult,
)


class Echo(TaskImpl):
    args: TaskInputList
    stdout: TaskOutput

    async def complete(self, **_) -> Optional[TaskResult]:
        return TaskResult(outputs={self.stdout: "".join(self.args)})
