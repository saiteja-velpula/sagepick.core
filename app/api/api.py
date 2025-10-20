from fastapi import APIRouter
from .endpoints.jobs import router as jobs_router
from .endpoints.movies import router as movies_router

router = APIRouter(prefix="/api")
router.include_router(jobs_router, prefix="/jobs", tags=["Job Management"])
router.include_router(movies_router, tags=["Movies & Categories"])
