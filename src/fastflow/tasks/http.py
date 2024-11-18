import asyncio
from typing import Any, Optional

from aiohttp import ClientSession

from fastflow import (
    TaskImpl,
    TaskInputDict,
    TaskInputList,
    TaskInputStr,
    TaskOutput,
    TaskResult,
)


class Request(TaskImpl):
    urls: TaskInputList
    headers: TaskInputDict = TaskInputDict()
    query_params: TaskInputDict = TaskInputDict()
    body: TaskInputStr = TaskInputStr("")
    method: TaskInputStr = TaskInputStr("GET")

    responses: TaskOutput

    async def complete(self, **_) -> Optional[TaskResult]:
        async with ClientSession() as client:
            responses = await asyncio.gather(
                *[
                    client.request(
                        method=self.method,
                        url=url,
                        headers=self.headers,
                        params=self.query_params,
                        data=self.body if self.body else None,
                    )
                    for url in self.urls
                ]
            )

            result_responses: dict[TaskOutput, Any] = {
                self.responses: [
                    {
                        "url": url,
                        "status_code": response.status,
                        "content_type": response.content_type,
                        "reason": response.reason,
                        "content": await (
                            response.json() if response.content_type == "application/json" else response.text()
                        ),
                        "response_headers": dict(response.headers),
                    }
                ]
                for url, response in zip(self.urls, responses)
            }

            return TaskResult(outputs=result_responses)
