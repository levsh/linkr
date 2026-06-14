from __future__ import annotations

import logging
import sys
from os import path

import pytest
import rmqaio
from docker.errors import DockerException

from linkr import MockTransport, RpcApp
from tests import utils


@pytest.fixture
async def transport():
    t = MockTransport()
    await t.init()
    yield t
    await t.close()


@pytest.fixture
async def app(transport: MockTransport):
    a = RpcApp(transport=transport)
    await a.init()
    yield a
    await a.close()


CWD = path.dirname(path.abspath(__file__))

logger = logging.getLogger("linkr")
log_frmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s lineno:%(lineno)4d -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stdout)
log_hndl.setFormatter(log_frmt)

logger.addHandler(log_hndl)
logger.setLevel(logging.DEBUG)

rmqaio.logger.addHandler(log_hndl)
rmqaio.logger.setLevel(logging.DEBUG)


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests that require external services like Docker")


@pytest.fixture(scope="class")
def container_executor():
    try:
        yield utils.ContainerExecutor()
    except DockerException:
        pytest.skip("Docker daemon is not available")


@pytest.fixture(scope="function")
def rabbitmq(container_executor):
    with container_executor.run_wait_up(
        "rabbitmq:3-management",
        ports={"5672": "5672", "15672": "15672"},
        healthcheck={
            "test": ["CMD", "rabbitmq-diagnostics", "-q", "ping"],
            "interval": 2_000_000_000,  # 2s
            "timeout": 1_000_000_000,  # 1s
            "retries": 30,
        },
        environment={
            "RABBITMQ_DEFAULT_USER": "guest",
            "RABBITMQ_DEFAULT_PASS": "guest",
        },
    ) as container:
        ip = utils.get_ip(container)
        port = 5672
        yield {"container": container, "ip": ip, "port": port}
