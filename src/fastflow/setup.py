import logging
from importlib.metadata import PackageNotFoundError, version

import kopf
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from pydantic import BaseSettings, Field

logging.basicConfig(level=logging.INFO)

logging.getLogger("kubernetes").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    OPERATOR_VERSION: str = Field(env="OPERATOR_VERSION", default="unknown")

    class Config:
        env_file = (
            ".env"  # only used or development, do not commit this file to git
        )
        env_file_encoding = "utf-8"
        case_sensitive = True


appsettings = Settings()

try:
    __version__ = version("package-name")
except PackageNotFoundError:
    # package is not installed
    __version__ = "unknown"


@kopf.on.probe(id="version")
def get_operator_version(**kwargs):
    return __version__


@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **_):
    settings.posting.level = logging.WARNING
    # settings.execution.max_workers = 20
    settings.networking.connect_timeout = 10
    settings.networking.request_timeout = 30
    settings.watching.server_timeout = 30
    settings.watching.client_timeout = 45

    settings.peering.mandatory = True


@kopf.on.startup()
async def init(**_):
    # Initialize the kubernetes_asyncio api client
    try:
        load_incluster_config()
    except ConfigException:
        logger.info("Failed loading incluster config, trying config file")
        await load_kube_config()
