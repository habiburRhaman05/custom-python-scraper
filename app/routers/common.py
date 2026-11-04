"""Shared router helpers: pagination, admin auth, simple entity listings."""

from __future__ import annotations

from math import ceil
from typing import Any

from fastapi import Depends, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_admin(key: str | None = Depends(api_key_header)) -> str:
    """Guard for the admin routes."""
    if not settings.admin_api_key:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "ADMIN_API_KEY is not configured on the server",
        )
    if not key or key != settings.admin_api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")
    return key


def paginate(
    db: Session, stmt, page: int, page_size: int
) -> tuple[list[Any], int, int]:
    """Run a count + a windowed select. Returns (rows, total, pages)."""
    total = db.scalar(
        select(func.count()).select_from(stmt.order_by(None).subquery())
    ) or 0
    rows = list(
        db.execute(stmt.limit(page_size).offset((page - 1) * page_size)).scalars()
    )
    return rows, total, max(ceil(total / page_size), 1) if total else 1


def page_params(
    page: int = Query(1, ge=1, description="1-indexed page number"),
    page_size: int = Query(50, ge=1, le=200, description="items per page (max 200)"),
) -> tuple[int, int]:
    return page, page_size


def active_filter(stmt, model, include_delisted: bool):
    """Hide soft-deleted rows unless the caller opts in."""
    if include_delisted:
        return stmt
    return stmt.where(model.is_active.is_(True))


def get_or_404(db: Session, model, slug: str):
    row = db.execute(select(model).where(model.slug == slug)).scalars().first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"{slug} not found")
    return row
