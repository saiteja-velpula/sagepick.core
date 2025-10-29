from math import ceil
from typing import TypeVar, Generic, List
from pydantic import BaseModel

T = TypeVar("T")


class PaginationInfo(BaseModel):
    """Pagination metadata."""

    page: int
    per_page: int
    total_items: int
    total_pages: int
    has_next: bool
    has_prev: bool


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper."""

    data: List[T]
    pagination: PaginationInfo


def create_pagination_info(
    page: int, per_page: int, total_items: int
) -> PaginationInfo:
    """Create pagination metadata."""
    total_pages = ceil(total_items / per_page) if total_items > 0 else 1
    return PaginationInfo(
        page=page,
        per_page=per_page,
        total_items=total_items,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1,
    )


def calculate_offset(page: int, per_page: int) -> int:
    """Calculate SQL offset for pagination."""
    return (page - 1) * per_page
