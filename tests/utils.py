from __future__ import annotations

import platform
import time
from contextlib import contextmanager, suppress
from typing import Any, Iterator


def get_ip(container: Any, internal: bool = False) -> str:
    if internal or platform.system() != "Darwin":
        if "IPAddress" in container.attrs["NetworkSettings"]:
            ip = container.attrs["NetworkSettings"]["IPAddress"]
        else:
            ip = container.attrs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
    else:
        ip = "127.0.0.1"
    return ip


class ContainerExecutor:
    def __init__(self) -> None:
        import docker

        self.client = docker.from_env()
        self.kwds: dict[str, Any] = {"detach": True}

    def dump_logs(self, container: Any) -> str:
        try:
            container.reload()
            return container.logs().decode()
        except Exception as e:
            return f"failed to read logs: {e}"

    @contextmanager
    def create(self, image: str, **kwds: Any) -> Iterator[Any]:
        container_kwds = self.kwds.copy()
        container_kwds.update(kwds)
        container = self.client.containers.create(image, **container_kwds)  # type: ignore[arg-type]
        try:
            yield container
        except Exception:
            print("=== CONTAINER LOGS ===")
            print(self.dump_logs(container))
            raise
        finally:
            with suppress(Exception):
                container.stop(timeout=10)
            with suppress(Exception):
                container.remove(force=True, v=True)

    @contextmanager
    def run(self, image: str, **kwds: Any) -> Iterator[Any]:
        with self.create(image, **kwds) as container:
            container.start()
            yield container

    def wait_healthy(self, container: Any, timeout: int = 60) -> Any:
        start = time.monotonic()

        while time.monotonic() - start < timeout:
            container.reload()

            status = container.status
            health = container.attrs.get("State", {}).get("Health", {}).get("Status")

            if status == "running" and (health in (None, "healthy")):
                return container

            if status == "exited":
                print("=== CONTAINER LOGS (EXITED EARLY) ===")
                print(self.dump_logs(container))
                break

            time.sleep(1)

        raise RuntimeError(self.dump_logs(container))

    @contextmanager
    def run_wait_up(self, image: str, **kwds: Any) -> Iterator[Any]:
        with self.run(image, **kwds) as container:
            container = self.wait_healthy(container)
            yield container

    @contextmanager
    def run_wait_exit(self, image: str, **kwds: Any) -> Iterator[Any]:
        with self.run(image, **kwds) as container:
            try:
                container.wait()
                yield container
            except Exception:
                print("=== CONTAINER LOGS ===")
                print(self.dump_logs(container))
                raise
