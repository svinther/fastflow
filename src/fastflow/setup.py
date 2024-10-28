import asyncio
import logging
from importlib.metadata import PackageNotFoundError, version

import kopf
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from pydantic import Field
from pydantic_settings import BaseSettings

logging.basicConfig(level=logging.INFO)

logging.getLogger("kubernetes").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


_custom_objects_api: CustomObjectsApi | None = None
_api_client: ApiClient | None = None

_custom_objects_api_mergepatch: CustomObjectsApi | None = None
_api_client_mergepatch: ApiClient | None = None


def get_custom_objects_api(mergepatch: bool = False):
    if mergepatch:
        return _custom_objects_api_mergepatch
    return _custom_objects_api


class Settings(BaseSettings):
    OPERATOR_VERSION: str = Field("unknown")

    class Config:
        env_file = ".env"  # only used or development, do not commit this file to git
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
    settings.posting.level = logging.INFO
    settings.execution.max_workers = 50
    settings.networking.connect_timeout = 20
    settings.networking.request_timeout = 90
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

    global _custom_objects_api, _api_client, _custom_objects_api_mergepatch, _api_client_mergepatch

    _api_client = ApiClient()
    _custom_objects_api = CustomObjectsApi(_api_client)

    _api_client_mergepatch = ApiClient()
    _api_client_mergepatch.set_default_header("Content-Type", "application/merge-patch+json")
    _custom_objects_api_mergepatch = CustomObjectsApi(_api_client_mergepatch)


@kopf.on.cleanup()
async def cleanup_dependencies(**_):
    await asyncio.gather(_api_client.close(), _api_client_mergepatch.close())
