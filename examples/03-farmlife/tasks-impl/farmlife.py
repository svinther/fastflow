import asyncio
import logging
from datetime import datetime
from typing import Optional

from fastflow import TaskImpl, TaskInputList, TaskOutput, TaskResult

log = logging.getLogger(__name__)


class DoSomeWork(TaskImpl):
    """This task does some work. Lots of farm animals are involved."""

    complexinput: TaskInputList
    complexoutput: TaskOutput

    async def complete(self, meta, status, patch, logger, retry, **_) -> Optional[TaskResult]:
        internalstate = status.get("internalstate", {})
        attempts = internalstate.get("attempts", "")
        if retry < 2:
            logger.warning(f"More ducks needed {attempts}")
            if attempts == "":
                attempts = "🦆 🦆 🦆"
            else:
                attempts = attempts + " 🦆 🦆 🦆" * (retry + 2)
            patch.status["internalstate"] = {"attempts": attempts}
            return TaskResult(
                finished=False,
                message=f"Did not do any work, but will try again" f"Current retries: {retry}",
            )

        thelist = self.complexinput
        if len(thelist) >= 2:
            thelist.append(thelist[:2])
            thelist.append({"Dogs": "🐶 🐶" * (retry + 1), "Cats": "🐱 🐱", "Birds": "🐦 🐦"})
        await asyncio.sleep(1.0)
        tstart = datetime.fromisoformat(meta["creationTimestamp"]).replace(tzinfo=None)
        tfinish = datetime.utcnow().replace(tzinfo=None)

        log.info(
            f"Farmlife Task {self.taskmodel.name}: "
            f"Did some work. Start: {tstart} Finish: {tfinish} "
            f"Duration: {(tfinish - tstart).total_seconds()}"
        )

        return TaskResult(
            outputs={self.complexoutput: thelist},
            message="Successfully did some work",
        )
