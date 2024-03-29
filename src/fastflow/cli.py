import asyncio
import threading
from typing import Any, Dict, List, Optional

import click
import kopf

_kopf_kwargs: Dict[str, Any] = {}
_kopg_args: List[str]


def run_kopf():
    asyncio.run(kopf.operator(**_kopf_kwargs))


def run_kopf_in_separate_thread():
    thread = threading.Thread(target=run)
    thread.start()
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
    run_kopf()
