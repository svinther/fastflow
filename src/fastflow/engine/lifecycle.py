import asyncio

import kopf

_healthloop: asyncio.Task


async def _healthloop_fn():
    while True:
        # Do the health checks here. Check if the list of workflows executing, is actually executing ?
        await asyncio.sleep(5)


@kopf.on.startup()
async def startup_fn(logger, **kwargs):
    global _healthloop
    _healthloop = asyncio.create_task(_healthloop_fn())


@kopf.on.cleanup()
async def cleanup_fn(**kwargs):
    global _healthloop
    _healthloop.cancel()
