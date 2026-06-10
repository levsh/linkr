import platform
import time
from contextlib import contextmanager

import docker


def get_ip(container, internal: bool = False):
    if internal or platform.system() != "Darwin":
        if "IPAddress" in container.attrs["NetworkSettings"]:
            ip = container.attrs["NetworkSettings"]["IPAddress"]
        else:
            ip = container.attrs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
    else:
        ip = "127.0.0.1"
    return ip


class ContainerExecutor:
    def __init__(self):
        self.client = docker.from_env()
        self.kwds = {"detach": True}

    def dump_logs(self, container):
        try:
            container.reload()
            return container.logs().decode()
        except Exception as e:
            return f"failed to read logs: {e}"

    @contextmanager
    def create(self, image, **kwds):
        container_kwds = self.kwds.copy()
        container_kwds.update(kwds)
        container = self.client.containers.create(image, **container_kwds)
        try:
            yield container
        except Exception:
            print("=== CONTAINER LOGS ===")
            print(self.dump_logs(container))
            raise
        finally:
            try:
                container.stop(timeout=10)
            except Exception:
                pass
            try:
                container.remove(force=True, v=True)
            except Exception:
                pass

    @contextmanager
    def run(self, image, **kwds):
        with self.create(image, **kwds) as container:
            container.start()
            yield container

    def wait_healthy(self, container, timeout=60):
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
    def run_wait_up(self, image, **kwds):
        with self.run(image, **kwds) as container:
            container = self.wait_healthy(container)
            yield container

    @contextmanager
    def run_wait_exit(self, image, **kwds):
        with self.run(image, **kwds) as container:
            try:
                yield container
            except Exception:
                print("=== CONTAINER LOGS ===")
                print(self.dump_logs(container))
                raise
            finally:
                container.wait()
