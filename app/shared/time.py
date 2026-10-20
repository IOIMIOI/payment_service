from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def aware(value: datetime) -> datetime:
    # SQLite test storage loses timezone information; PostgreSQL uses timestamptz.
    return value.replace(tzinfo=value.tzinfo or timezone.utc)


def iso_utc(value: datetime | None) -> str | None:
    return aware(value).astimezone(timezone.utc).isoformat() if value is not None else None
