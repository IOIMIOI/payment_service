#!/usr/bin/env python3
"""Проверка unread persistent-сообщения после настоящего Docker Compose down/up.

Нужны только Python stdlib, Docker Engine и Compose v2. Запуск:
    python scripts/check_rabbitmq_persistence.py
--negative-control убирает hostname/nodename и должен завершиться ошибкой,
воспроизводя исходный дефект; рабочая compose.yaml при этом не меняется.
"""

import argparse
import base64
import copy
import json
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4


def docker(*arguments: str, timeout: int = 180) -> str:
    result = subprocess.run(
        ["docker", *arguments],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Docker command failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout.strip()


def isolated_config(source: dict, *, negative_control: bool) -> dict:
    service = copy.deepcopy(source["services"]["rabbitmq"])
    # Реальная конфигурация сервиса, но без подключения к исходным контейнерам,
    # сетям, портам или внешним volumes пользователя.
    for key in ("container_name", "networks", "depends_on", "profiles"):
        service.pop(key, None)
    service["ports"] = [{"target": 15672, "published": "0", "host_ip": "127.0.0.1"}]
    if negative_control:
        service.pop("hostname", None)
        service["environment"].pop("RABBITMQ_NODENAME", None)
    volumes = {}
    for mount in service.get("volumes", []):
        if mount.get("type") != "volume" or not mount.get("source"):
            raise RuntimeError("Persistence regression requires named volumes, no bind mounts")
        # Не переносим resolved name/external исходного compose config: Compose
        # создаст новый volume с уникальным project prefix.
        volumes[mount["source"]] = {}
    if not any(mount["target"] == "/var/lib/rabbitmq" for mount in service.get("volumes", [])):
        raise RuntimeError("RabbitMQ must mount a named volume at /var/lib/rabbitmq")
    return {"services": {"rabbitmq": service}, "volumes": volumes}


class ManagementAPI:
    def __init__(self, port: str, environment: dict):
        self.url = f"http://127.0.0.1:{port}/api"
        credentials = (
            f"{environment['RABBITMQ_DEFAULT_USER']}:{environment['RABBITMQ_DEFAULT_PASS']}"
        )
        self.authorization = "Basic " + base64.b64encode(credentials.encode()).decode("ascii")
        # Системный HTTP proxy не должен отправлять credentials или запросы
        # локального тестового контейнера за пределы хоста.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, method: str, path: str, body=None):
        request = urllib.request.Request(
            self.url + path,
            data=None if body is None else json.dumps(body).encode("utf-8"),
            method=method,
            headers={"Authorization": self.authorization, "Content-Type": "application/json"},
        )
        with self.opener.open(request, timeout=5) as response:
            data = response.read()
            return json.loads(data) if data else None

    def wait(self, path: str, predicate=lambda response: bool(response), timeout: int = 60):
        deadline = time.monotonic() + timeout
        last_error = "condition not yet met"
        while time.monotonic() < deadline:
            try:
                response = self.request("GET", path)
                if predicate(response):
                    return response
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise RuntimeError(f"RabbitMQ did not become ready at {path}: {last_error}")


def inspect_container(container_id: str, project_name: str) -> dict:
    details = json.loads(docker("inspect", container_id))[0]
    labels = details["Config"]["Labels"]
    if labels.get("com.docker.compose.project") != project_name:
        raise RuntimeError("Refusing to inspect an unrelated Compose project")
    if labels.get("com.docker.compose.service") != "rabbitmq":
        raise RuntimeError("Expected the isolated RabbitMQ service")
    return details


def volume_identity(details: dict) -> str:
    mounts = [mount for mount in details["Mounts"] if mount["Destination"] == "/var/lib/rabbitmq"]
    if len(mounts) != 1 or mounts[0]["Type"] != "volume":
        raise RuntimeError("Expected exactly one named RabbitMQ data volume")
    return mounts[0]["Name"]


def run(negative_control: bool) -> None:
    project_root = Path(__file__).resolve().parents[1]
    source = json.loads(
        docker(
            "compose", "--file", str(project_root / "compose.yaml"), "config", "--format", "json"
        )
    )
    configuration = isolated_config(source, negative_control=negative_control)
    project_name = "rabbit-persistence-" + uuid4().hex
    project_filter = f"label=com.docker.compose.project={project_name}"
    # Даже невероятная коллизия случайного имени не должна затронуть чужие данные.
    if docker("ps", "-aq", "--filter", project_filter) or docker(
        "volume", "ls", "-q", "--filter", project_filter
    ):
        raise RuntimeError("Generated Compose project already exists; refusing to reuse it")

    with tempfile.TemporaryDirectory(prefix="rabbitmq-persistence-") as directory:
        config_path = Path(directory) / "compose.json"
        config_path.write_text(json.dumps(configuration), encoding="utf-8")
        prefix = ("compose", "--file", str(config_path), "--project-name", project_name)

        def compose(*arguments: str) -> str:
            return docker(*prefix, *arguments)

        environment = configuration["services"]["rabbitmq"]["environment"]

        def start():
            compose("up", "--detach", "--wait", "--wait-timeout", "120", "rabbitmq")
            details = inspect_container(compose("ps", "-q", "rabbitmq"), project_name)
            ports = details["NetworkSettings"]["Ports"]["15672/tcp"]
            if len(ports) != 1 or ports[0]["HostIp"] != "127.0.0.1":
                raise RuntimeError("Management port must be bound only to localhost")
            api = ManagementAPI(ports[0]["HostPort"], environment)
            api.wait("/overview")
            return details, api

        print(f"Isolated project: {project_name}", flush=True)
        if negative_control:
            print("Negative control: hostname/nodename removed; check must fail.", flush=True)
        try:
            before, api = start()
            volume_before = volume_identity(before)
            node_before = api.request("GET", "/nodes")[0]["name"]
            event_id = str(uuid4())
            queue = "persistence-" + uuid4().hex
            vhost = urllib.parse.quote(environment.get("RABBITMQ_DEFAULT_VHOST", "/"), safe="")
            queue_path = f"/queues/{vhost}/{queue}"
            payload = json.dumps({"event_id": event_id, "cycle": 0}, sort_keys=True)
            api.request("PUT", queue_path, {"durable": True, "auto_delete": False, "arguments": {}})
            published = api.request(
                "PUT",
                f"/exchanges/{vhost}/amq.default/publish",
                {
                    "properties": {"delivery_mode": 2, "message_id": event_id},
                    "routing_key": queue,
                    "payload": payload,
                    "payload_encoding": "string",
                },
            )
            if not published.get("routed"):
                raise RuntimeError("Persistent test message was not routed")
            # GET статистики не читает сообщение: до down/up нет consumer/get/ack.
            api.wait(queue_path, lambda data: data.get("messages_ready") == 1)
            print(
                f"Unread persistent message ready; container={before['Id']} "
                f"node={node_before} volume={volume_before}",
                flush=True,
            )

            # Не restart: старый контейнер действительно удаляется, volume остаётся.
            compose("down", "--timeout", "30")
            after, api = start()
            node_after = api.request("GET", "/nodes")[0]["name"]
            if before["Id"] == after["Id"]:
                raise RuntimeError("Container ID did not change; recreate was not exercised")
            if volume_before != volume_identity(after):
                raise RuntimeError("The recreated broker uses a different data volume")
            print(f"Recreated container={after['Id']} node={node_after}; same volume", flush=True)

            try:
                # Management API может ответить раньше завершения восстановления
                # очереди и обновления статистики. Временный 404 ещё не доказывает
                # потерю данных; ждём ограниченное время без повторной публикации.
                api.wait(
                    queue_path,
                    lambda data: data.get("durable") and data.get("messages_ready") == 1,
                    timeout=30,
                )
            except RuntimeError as exc:
                raise RuntimeError(
                    "Durable queue or unread message unavailable after recreate: "
                    f"{node_before} -> {node_after} ({exc})"
                ) from exc
            received = api.request(
                "POST",
                queue_path + "/get",
                {"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50000},
            )
            if len(received) != 1 or received[0]["payload"] != payload:
                raise RuntimeError("Unread persistent message did not survive container recreation")
            properties = received[0]["properties"]
            if properties.get("delivery_mode") != 2 or properties.get("message_id") != event_id:
                raise RuntimeError("Persistent message identity or delivery_mode changed")
            if node_before != node_after:
                raise RuntimeError(f"RabbitMQ node changed: {node_before} -> {node_after}")
            print("PASS: durable queue and unchanged unread message survived down/up.", flush=True)
        except BaseException:
            try:
                print(compose("logs", "--no-color", "--tail", "80", "rabbitmq"), file=sys.stderr)
            except Exception as exc:
                print(f"Could not collect isolated broker logs: {exc}", file=sys.stderr)
            raise
        finally:
            # Только наш уникальный проект. Исходный compose используется лишь
            # для чтения config, никогда для down/rm. После cleanup проверяем labels.
            compose("down", "--volumes", "--timeout", "30")
            if docker("ps", "-aq", "--filter", project_filter) or docker(
                "volume", "ls", "-q", "--filter", project_filter
            ):
                raise RuntimeError(f"Isolated project cleanup was incomplete: {project_name}")
            print(f"Removed isolated project and volumes: {project_name}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--negative-control", action="store_true")
    args = parser.parse_args()
    try:
        run(args.negative_control)
    except (RuntimeError, subprocess.TimeoutExpired, urllib.error.URLError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
