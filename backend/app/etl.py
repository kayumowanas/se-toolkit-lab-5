"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


def _api_auth() -> tuple[str, str]:
    """Return credentials for HTTP Basic Auth."""
    return (settings.autochecker_email, settings.autochecker_password)


def _parse_api_datetime(value: str) -> datetime:
    """Parse API timestamp and store it as a naive UTC datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url.rstrip('/')}/api/items"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=_api_auth())

    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected /api/items response shape: expected JSON list")
    return payload


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    url = f"{settings.autochecker_api_url.rstrip('/')}/api/logs"
    all_logs: list[dict[str, Any]] = []
    next_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, Any] = {"limit": 500}
            if next_since is not None:
                params["since"] = f"{next_since.isoformat()}Z"

            response = await client.get(url, auth=_api_auth(), params=params)
            response.raise_for_status()

            payload = response.json()
            logs = payload.get("logs", [])
            has_more = payload.get("has_more", False)

            all_logs.extend(logs)

            if not has_more or not logs:
                break

            last_submitted = logs[-1].get("submitted_at")
            if not isinstance(last_submitted, str):
                break
            next_since = _parse_api_datetime(last_submitted)

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    created = 0
    lab_by_short_id: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    for lab in labs:
        lab_title = lab.get("title")
        lab_short_id = lab.get("lab")
        if not isinstance(lab_title, str) or not isinstance(lab_short_id, str):
            continue

        existing_lab = (
            await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab", ItemRecord.title == lab_title
                )
            )
        ).first()

        if existing_lab is None:
            existing_lab = ItemRecord(type="lab", title=lab_title)
            session.add(existing_lab)
            await session.flush()
            created += 1

        lab_by_short_id[lab_short_id] = existing_lab

    tasks = [item for item in items if item.get("type") == "task"]
    for task in tasks:
        task_title = task.get("title")
        lab_short_id = task.get("lab")
        if not isinstance(task_title, str) or not isinstance(lab_short_id, str):
            continue

        parent_lab = lab_by_short_id.get(lab_short_id)
        if parent_lab is None or parent_lab.id is None:
            continue

        existing_task = (
            await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == task_title,
                    ItemRecord.parent_id == parent_lab.id,
                )
            )
        ).first()

        if existing_task is None:
            session.add(
                ItemRecord(type="task", title=task_title, parent_id=parent_lab.id)
            )
            created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    created = 0
    title_by_short_ids: dict[tuple[str, str | None], str] = {}

    for item in items_catalog:
        lab_short_id = item.get("lab")
        item_type = item.get("type")
        item_title = item.get("title")
        if not isinstance(lab_short_id, str) or not isinstance(item_title, str):
            continue

        if item_type == "lab":
            title_by_short_ids[(lab_short_id, None)] = item_title
        if item_type == "task":
            task_short_id = item.get("task")
            if isinstance(task_short_id, str):
                title_by_short_ids[(lab_short_id, task_short_id)] = item_title

    for log in logs:
        student_id = log.get("student_id")
        if not isinstance(student_id, str):
            continue

        learner = (
            await session.exec(select(Learner).where(Learner.external_id == student_id))
        ).first()
        if learner is None:
            student_group = log.get("group")
            learner = Learner(
                external_id=student_id,
                student_group=student_group if isinstance(student_group, str) else "",
            )
            session.add(learner)
            await session.flush()

        lab_short_id = log.get("lab")
        task_short_id = log.get("task")
        if not isinstance(lab_short_id, str):
            continue
        if task_short_id is not None and not isinstance(task_short_id, str):
            continue

        item_title = title_by_short_ids.get((lab_short_id, task_short_id))
        if item_title is None:
            continue

        item = (
            await session.exec(select(ItemRecord).where(ItemRecord.title == item_title))
        ).first()
        if item is None or item.id is None or learner.id is None:
            continue

        external_id = log.get("id")
        if not isinstance(external_id, int):
            continue

        existing_interaction = (
            await session.exec(
                select(InteractionLog).where(InteractionLog.external_id == external_id)
            )
        ).first()
        if existing_interaction is not None:
            continue

        submitted_at = log.get("submitted_at")
        if not isinstance(submitted_at, str):
            continue

        session.add(
            InteractionLog(
                external_id=external_id,
                learner_id=learner.id,
                item_id=item.id,
                kind="attempt",
                score=log.get("score"),
                checks_passed=log.get("passed"),
                checks_total=log.get("total"),
                created_at=_parse_api_datetime(submitted_at),
            )
        )
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    last_synced_at = (
        await session.exec(select(func.max(InteractionLog.created_at)))
    ).one()

    logs = await fetch_logs(last_synced_at)
    new_records = await load_logs(logs, items_catalog, session)

    total_records = (
        await session.exec(select(func.count()).select_from(InteractionLog))
    ).one()

    return {"new_records": new_records, "total_records": total_records}
