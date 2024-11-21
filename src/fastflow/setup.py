import logging
from importlib.metadata import version

import kopf
from kopf import LogFormat
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.getLogger("kubernetes").setLevel(logging.INFO)

logger = logging.getLogger(__name__)

__fastflow_version__ = version("python-fastflow")


class Settings(BaseSettings):
    OPERATOR_VERSION: str = Field("unknown")
    max_parallel_workflows: int = 12

    namespace: str = "default"
    kopf_liveness_endpoint: str = "http://0.0.0.0:8080/healthz"
    kopf_priority: int = 666
    kopf_peering: str = "default"
    log_format: LogFormat = LogFormat.FULL

    kopf_handler_retry_default_delay: float = 15.0

    model_config = SettingsConfigDict(env_file_encoding="utf-8", case_sensitive=True)

    def get_kopf_kwargs(self) -> dict:
        return dict(
            priority=self.kopf_priority,
            namespaces=[self.namespace],
            clusterwide=False,
            liveness_endpoint=self.kopf_liveness_endpoint,
            peering_name=self.kopf_peering,
        )


_appsettings = Settings()


def get_appsettings() -> Settings:
    global _appsettings
    return _appsettings


@kopf.on.probe(id="version")
def get_operator_version(**kwargs):
    return __fastflow_version__


@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **_):
    logger.info("Fastflow version: %s", __fastflow_version__)

    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("kopf.activities.probe").setLevel(logging.WARNING)

    # settings.posting.level = logging.FATAL
    settings.posting.enabled = False

    settings.execution.max_workers = 5

    settings.networking.connect_timeout = 20
    settings.networking.request_timeout = 90
    settings.watching.server_timeout = 30
    settings.watching.client_timeout = 45

    settings.peering.mandatory = True
    settings.peering.stealth = True
