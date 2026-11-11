from __future__ import annotations

import logging
import sys
from os import path

import pytest

from linkr import App, LocalBackend
from tests import utils

CWD = path.dirname(path.abspath(__file__))

logger = logging.getLogger("linkr")
log_frmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s lineno:%(lineno)4d -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stdout)
log_hndl.setFormatter(log_frmt)

logger.addHandler(log_hndl)
logger.setLevel(logging.DEBUG)


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests that require external services like Docker")


@pytest.fixture(autouse=True)
def reset_local_backend():
    LocalBackend.reset()
    yield
    LocalBackend.reset()


@pytest.fixture
async def local_backend():
    backend = LocalBackend()
    await backend.init()
    yield backend
    await backend.close()


@pytest.fixture
async def app(local_backend: LocalBackend):
    instance = App("app", backend=local_backend)
    await instance.init()
    yield instance
    await instance.close()


@pytest.fixture(scope="class")
def container_executor():
    pytest.importorskip("docker")

    try:
        yield utils.ContainerExecutor()
    except Exception as exc:  # pragma: no cover - depends on environment
        pytest.skip(f"Docker daemon is not available: {exc}")


@pytest.fixture(scope="class")
def rabbitmq(container_executor, request):
    ports = {
        "5672": "5672",
        "15672": "15672",
    }
    environment = {
        "RABBITMQ_DEFAULT_USER": "guest",
        "RABBITMQ_DEFAULT_PASS": "guest",
    }
    volumes = {}
    if getattr(request, "param", {}).get("tls") is True:
        ports.update(
            {
                "5671": "5671",
                "15671": "15671",
            },
        )
    with container_executor.run_wait_up(
        "rabbitmq:3-management",
        name="rabbitmq",
        ports=ports,
        environment=environment,
        volumes=volumes,
        healthcheck={
            "test": ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"],
            "interval": 1_000_000_000,  # 1s
            "timeout": 1_000_000_000,  # 1s
            "retries": 30,
        },
    ) as container:
        yield container
