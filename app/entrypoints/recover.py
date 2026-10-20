"""Ручное восстановление нужного этапа: python -m app.entrypoints.recover list|replay ID."""

import argparse
import asyncio
import json

from app.application.recovery import list_dead, replay
from app.bootstrap import uow_factory
from app.infrastructure.persistence.db import engine


async def run_cli(args: argparse.Namespace) -> None:
    try:
        result = (
            await list_dead(uow_factory)
            if args.command == "list"
            else await replay(args.event_id, uow_factory)
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Источником состояния DLQ служит БД. Старые сообщения очередей .dlq — "
            "аудит; их не нужно перепубликовывать вручную."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="Показать этапы в состоянии dead")
    replay_parser = commands.add_parser("replay", help="Повторить только указанный этап")
    replay_parser.add_argument("event_id")
    args = parser.parse_args()
    try:
        asyncio.run(run_cli(args))
    except ValueError as exc:
        parser.exit(2, f"Ошибка: {exc}\n")


if __name__ == "__main__":
    main()
