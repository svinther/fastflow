import asyncio
from typing import Optional

from fastflow.engine import TaskImpl, TaskInputInt, TaskResult


class Sleep(TaskImpl):
    howlong: TaskInputInt

    async def complete(self, meta, status, patch, logger, retry, **_) -> Optional[TaskResult]:
        logger.warn(f"Sleeping for {self.howlong} seconds")
        await asyncio.sleep(self.howlong)
        return None
