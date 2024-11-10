import asyncio
from typing import Optional

from fastflow.engine.models import (
    TaskImpl,
    TaskInputFloat,
    TaskInputList,
    TaskOutput,
    TaskResult,
)


class Echo(TaskImpl):
    args: TaskInputList
    stdout: TaskOutput

    async def complete(self, **_) -> Optional[TaskResult]:
        return TaskResult(outputs={self.stdout: "".join(self.args)})


class Sleep(TaskImpl):
    seconds: TaskInputFloat

    async def complete(self, **_) -> Optional[TaskResult]:
        await asyncio.sleep(self.seconds)
        return None
