import asyncio
import contextlib
import logging
import threading
import time
from typing import Any, Dict, List, Optional

import click
import kopf

_kopf_kwargs: Dict[str, Any] = {}
_kopg_args: List[str]

logger = logging.getLogger(__name__)


def kopf_thread(
    ready_flag: threading.Event,
    stop_flag: threading.Event,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with contextlib.closing(loop):
        loop.run_until_complete(kopf.operator(ready_flag=ready_flag, stop_flag=stop_flag, **_kopf_kwargs))


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
    logger.info("Kopf is running...")
    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Kopf is stopping...")
            stop_flag.set()
            break

    thread.join()


@click.group(name="fastflow")
@click.version_option(prog_name="fastflow")
def main() -> None:
    pass


@main.command()
@click.option("-A", "--all-namespaces", "clusterwide", is_flag=True)
@click.option("-n", "--namespace", "namespaces", multiple=True)
@click.option("--dev", "priority", type=int, is_flag=True, flag_value=666)
@click.option("-p", "--priority", type=int)
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "-L",
    "--liveness",
    "liveness_endpoint",
    type=str,
    default="http://0.0.0.0:8080/healthz",
)
def run(
    priority: Optional[int],
    namespaces: List[str],
    clusterwide: bool,
    verbose: bool,
    liveness_endpoint: str,
) -> None:
    _kopf_kwargs.update(
        dict(
            priority=priority,
            namespaces=namespaces,
            clusterwide=clusterwide,
            liveness_endpoint=liveness_endpoint,
        )
    )

    if verbose:
        kopf.configure(verbose=True)

    run_kopf_in_separate_thread()
