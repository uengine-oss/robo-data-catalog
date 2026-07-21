"""ROBO Data Catalog 서비스

스키마 조회/편집/보강 + 리니지 + DW 관리 서비스.
분석 그래프를 소비하는 독립 마이크로서비스.

시작 방법:
    uvicorn main:app --host 0.0.0.0 --port 5503 --reload
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.enrichment import router as enrichment_router
from api.graph import router as graph_router
from api.lineage import router as lineage_router
from api.schema import router as schema_router
from api.schema_edit import router as schema_edit_router
from api.search import router as search_router
from api.table_samples import router as table_samples_router
from shared.config.settings import CATALOG_SETTINGS
from shared.observability.logger import get_catalog_logger, setup_catalog_logging


# 로깅 초기화
setup_catalog_logging()
logger = get_catalog_logger(__name__)


# =============================================================================
# FastAPI 앱 설정
# =============================================================================

app = FastAPI(
    title="ROBO Data Catalog",
    description="스키마 조회/편집/보강, 리니지, DW 관리 서비스",
    version=CATALOG_SETTINGS.version,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS 미들웨어
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(CATALOG_SETTINGS.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
for router in (
    graph_router,
    lineage_router,
    schema_router,
    schema_edit_router,
    search_router,
    table_samples_router,
    enrichment_router,
):
    app.include_router(router)


# =============================================================================
# 예외 핸들러
# =============================================================================

@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    logger.error("RuntimeError 발생", exc_info=(type(exc), exc, exc.__traceback__))
    return JSONResponse(
        status_code=500,
        content={"detail": "Catalog operation failed", "error_type": "RuntimeError"},
    )


# =============================================================================
# 헬스체크
# =============================================================================

@app.get("/")
async def health_check():
    return {"status": "ok", "service": "robo-data-catalog", "version": CATALOG_SETTINGS.version}


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "robo-data-catalog", "version": CATALOG_SETTINGS.version}


# =============================================================================
# 서버 시작
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(__import__("os").getenv("PORT", "5503"))
    logger.info("ROBO Data Catalog starting on %s:%d", CATALOG_SETTINGS.host, port)
    uvicorn.run("main:app", host=CATALOG_SETTINGS.host, port=port, reload=False)
