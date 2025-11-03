from fastapi import APIRouter
from .endpoints.jobs import router as jobs_router
from .endpoints.movies import router as movies_router
from .endpoints.discover import router as discover_router

router = APIRouter(prefix="/v1")
router.include_router(jobs_router, prefix="/jobs", tags=["Job Management"])
router.include_router(movies_router, prefix="/movies", tags=["Movies & Details"])
router.include_router(discover_router, tags=["Discovery & Categories"])