import asyncio
import contextlib
import logging
import threading
import time
from typing import List, Optional

import click
import kopf
from kopf._cogs.helpers import loaders
from kopf._core.actions import loggers

from fastflow.setup import get_appsettings

logger = logging.getLogger(__name__)


def kopf_thread(
    ready_flag: threading.Event,
    stop_flag: threading.Event,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with contextlib.closing(loop):
        loop.run_until_complete(
            kopf.operator(ready_flag=ready_flag, stop_flag=stop_flag, **get_appsettings().get_kopf_kwargs())
        )


def run_kopf_in_separate_thread():
    """see https://kopf.readthedocs.io/en/stable/embedding/#manual-execution"""
    ready_flag = threading.Event()
    stop_flag = threading.Event()
    thread = threading.Thread(
        target=kopf_thread,
        kwargs=dict(
            stop_flag=stop_flag,
            ready_flag=ready_flag,
        ),
    )
    thread.start()
    ready_flag.wait()
    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            stop_flag.set()
            break

    thread.join()


@click.group(name="fastflow")
@click.version_option(prog_name="fastflow")
def main() -> None:
    pass


@main.command()
@click.option("-n", "--namespace", "namespace", multiple=False, required=True)
@click.option("--dev", "priority", type=int, is_flag=True, flag_value=666)
@click.option("-p", "--priority", type=int)
@click.option("--peering", type=str, required=False)
@click.option("-m", "--module", "modules", multiple=True)
@click.option("-r", "--handler-retry-delay", "kopf_handler_retry_default_delay", required=False, type=float)
@click.argument("paths", nargs=-1)
def run(
    priority: Optional[int],
    peering: Optional[str],
    namespace: str,
    paths: List[str],
    modules: List[str],
    kopf_handler_retry_default_delay: Optional[float] = None,
) -> None:
    get_appsettings().namespace = namespace
    if priority is not None:
        get_appsettings().kopf_priority = priority
    if kopf_handler_retry_default_delay is not None:
        get_appsettings().kopf_handler_retry_default_delay = kopf_handler_retry_default_delay
    if peering is not None:
        get_appsettings().kopf_peering = peering

    # kopf.configure(verbose=True)  # log formatting
    loggers.configure(log_format=get_appsettings().log_format)

    loaders.preload(
        paths=paths,
        modules=modules,
    )

    run_kopf_in_separate_thread()
