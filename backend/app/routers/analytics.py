"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_fragment(lab: str) -> str:
    normalized = lab.strip().replace("_", "-")
    if normalized.lower().startswith("lab-"):
        suffix = normalized.split("-", 1)[1]
        return f"Lab {suffix}"
    return normalized.replace("-", " ").title()


async def _task_ids_for_lab(lab: str, session: AsyncSession) -> list[int]:
    lab_fragment = _lab_title_fragment(lab)
    lab_item = (
        await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title.contains(lab_fragment),
            )
        )
    ).first()
    if lab_item is None or lab_item.id is None:
        return []

    return list(
        (
            await session.exec(
                select(ItemRecord.id).where(
                    ItemRecord.type == "task",
                    ItemRecord.parent_id == lab_item.id,
                )
            )
        ).all()
    )


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    task_ids = await _task_ids_for_lab(lab, session)
    buckets = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    if not task_ids:
        return [{"bucket": key, "count": value} for key, value in buckets.items()]

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    rows = (
        await session.exec(
            select(bucket_expr.label("bucket"), func.count(InteractionLog.id))
            .where(
                InteractionLog.item_id.in_(task_ids),
                InteractionLog.score.is_not(None),
            )
            .group_by(bucket_expr)
        )
    ).all()

    for bucket, count in rows:
        buckets[bucket] = int(count)

    return [{"bucket": key, "count": value} for key, value in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    task_ids = await _task_ids_for_lab(lab, session)
    if not task_ids:
        return []

    rows = (
        await session.exec(
            select(
                ItemRecord.title,
                func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
                func.count(InteractionLog.id).label("attempts"),
            )
            .join(InteractionLog, InteractionLog.item_id == ItemRecord.id, isouter=True)
            .where(ItemRecord.id.in_(task_ids))
            .group_by(ItemRecord.id, ItemRecord.title)
            .order_by(ItemRecord.title.asc())
        )
    ).all()

    return [
        {
            "task": title,
            "avg_score": float(avg_score) if avg_score is not None else 0.0,
            "attempts": int(attempts),
        }
        for title, avg_score, attempts in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    task_ids = await _task_ids_for_lab(lab, session)
    if not task_ids:
        return []

    day_expr = func.date(InteractionLog.created_at)
    rows = (
        await session.exec(
            select(day_expr.label("day"), func.count(InteractionLog.id).label("submissions"))
            .where(InteractionLog.item_id.in_(task_ids))
            .group_by(day_expr)
            .order_by(day_expr.asc())
        )
    ).all()

    return [
        {"date": str(day), "submissions": int(submissions)}
        for day, submissions in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    task_ids = await _task_ids_for_lab(lab, session)
    if not task_ids:
        return []

    rows = (
        await session.exec(
            select(
                Learner.student_group.label("group"),
                func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
                func.count(func.distinct(InteractionLog.learner_id)).label("students"),
            )
            .join(Learner, Learner.id == InteractionLog.learner_id)
            .where(InteractionLog.item_id.in_(task_ids))
            .group_by(Learner.student_group)
            .order_by(Learner.student_group.asc())
        )
    ).all()

    return [
        {
            "group": group,
            "avg_score": float(avg_score) if avg_score is not None else 0.0,
            "students": int(students),
        }
        for group, avg_score, students in rows
    ]
