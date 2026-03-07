"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, distinct, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_fragment(lab: str) -> str:
    """Convert 'lab-04' -> 'Lab 04' for title matching."""
    return lab.replace("lab-", "Lab ")


async def _get_lab_item(session: AsyncSession, lab: str) -> ItemRecord | None:
    """Find the lab item whose title contains the formatted lab token."""
    lab_fragment = _lab_title_fragment(lab)
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_fragment),
        )
    )
    return result.first()


async def _get_task_ids(session: AsyncSession, lab_item_id: int) -> list[int]:
    """Get IDs of task items that belong to the lab."""
    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.parent_id == lab_item_id,
            ItemRecord.type == "task",
        )
    )
    return list(result.all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    default_buckets = [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]

    lab_item = await _get_lab_item(session, lab)
    if not lab_item or lab_item.id is None:
        return default_buckets

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return default_buckets

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    result = await session.exec(
        select(
            bucket_case.label("bucket"),
            func.count(InteractionLog.id).label("count"),
        )
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_case)
    )

    counts = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0,
    }

    for bucket, count in result.all():
        counts[bucket] = count

    return [
        {"bucket": "0-25", "count": counts["0-25"]},
        {"bucket": "26-50", "count": counts["26-50"]},
        {"bucket": "51-75", "count": counts["51-75"]},
        {"bucket": "76-100", "count": counts["76-100"]},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_item = await _get_lab_item(session, lab)
    if not lab_item or lab_item.id is None:
        return []

    result = await session.exec(
        select(
            ItemRecord.title.label("task"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task",
            InteractionLog.score.is_not(None),
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    return [
        {
            "task": task,
            "avg_score": round(float(avg_score), 1) if avg_score is not None else 0.0,
            "attempts": attempts,
        }
        for task, avg_score, attempts in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_item = await _get_lab_item(session, lab)
    if not lab_item or lab_item.id is None:
        return []

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []

    date_expr = func.date(InteractionLog.created_at)

    result = await session.exec(
        select(
            date_expr.label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )

    return [
        {"date": str(date_value), "submissions": submissions}
        for date_value, submissions in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_item = await _get_lab_item(session, lab)
    if not lab_item or lab_item.id is None:
        return []

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),
        )
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    return [
        {
            "group": group,
            "avg_score": round(float(avg_score), 1) if avg_score is not None else 0.0,
            "students": students,
        }
        for group, avg_score, students in result.all()
    ]