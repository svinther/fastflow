import logging
from importlib.metadata import PackageNotFoundError, version

import kopf
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)

logging.getLogger("kubernetes").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    OPERATOR_VERSION: str = Field("unknown")
    max_parallel_workflows: int = 12

    namespace: str = "default"
    kopf_liveness_endpoint: str = "http://0.0.0.0:8080/healthz"
    kopf_priority: int = 666

    kopf_handler_retry_default_delay: float = 15.0

    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    def get_kopf_kwargs(self) -> dict:
        return dict(
            priority=self.kopf_priority,
            namespaces=[self.namespace],
            clusterwide=False,
            liveness_endpoint=self.kopf_liveness_endpoint,
        )


_appsettings = Settings()


def get_appsettings() -> Settings:
    global _appsettings
    return _appsettings


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
    settings.execution.max_workers = 50
    settings.networking.connect_timeout = 20
    settings.networking.request_timeout = 90
    settings.watching.server_timeout = 30
    settings.watching.client_timeout = 45

    settings.peering.mandatory = True
