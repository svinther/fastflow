import asyncio
from typing import Optional

from fastflow.engine import TaskImpl, TaskInputFloat, TaskResult


class Sleep(TaskImpl):
    howlong: TaskInputFloat

    async def complete(self, meta, status, patch, logger, retry, **_) -> Optional[TaskResult]:
        logger.warning(f"Sleeping for {self.howlong} seconds")
        await asyncio.sleep(self.howlong)
        return None
