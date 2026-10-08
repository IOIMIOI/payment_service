"""Ручное восстановление нужного этапа: python -m app.recover list|replay ID."""

import argparse
import asyncio
import json
from uuid import uuid4

from sqlalchemy import select

from app.db import Session, engine
from app.models import Outbox, WorkItem

DESTINATIONS = {"payment": "payments.new", "webhook": "payments.webhook"}


async def list_dead(session_factory=Session) -> list[dict]:
    async with session_factory() as session:
        items = (
            await session.scalars(
                select(WorkItem).where(WorkItem.state == "dead").order_by(WorkItem.created_at)
            )
        ).all()
        return [
            {
                "event_id": item.event_id,
                "payment_id": item.payment_id,
                "stage": item.kind,
                "cycle": item.cycle,
                "attempts": item.attempts,
                "last_error": item.last_error,
            }
            for item in items
        ]


async def replay(event_id: str, session_factory=Session) -> dict:
    async with session_factory() as session, session.begin():
        item = await session.scalar(
            select(WorkItem).where(WorkItem.event_id == event_id).with_for_update()
        )
        if item is None:
            raise ValueError(f"Событие {event_id} не найдено")
        if item.state != "dead":
            raise ValueError(f"Восстанавливать можно только dead, сейчас: {item.state}")

        # Уточнение к ТЗ: ручное восстановление начинает новый ограниченный цикл.
        # Меняется номер цикла, но event_id, payload и provider_key сохраняются.
        # Поэтому уже завершённая оплата не повторяется при восстановлении webhook.
        item.cycle += 1
        item.attempts = 0
        item.state = "ready"
        item.not_before = None
        item.last_error = None
        session.add(
            Outbox(
                id=str(uuid4()),
                event_id=item.event_id,
                destination=DESTINATIONS[item.kind],
                cycle=item.cycle,
            )
        )
        return {"event_id": item.event_id, "stage": item.kind, "cycle": item.cycle}


async def run_cli(args: argparse.Namespace) -> None:
    try:
        result = await list_dead() if args.command == "list" else await replay(args.event_id)
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
