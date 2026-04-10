"""
FastAPI main application.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging

from app.config import settings
from app.database import init_db
from app.services.notification_scheduler import NotificationSchedulerRunner

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

notification_scheduler_runner = NotificationSchedulerRunner()

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _register_vnstock_api_key() -> None:
    """Đăng ký API key vnstock từ biến môi trường khi khởi động."""
    api_key = settings.vnstock_api_key
    if not api_key:
        logger.warning(
            "VNSTOCK_API_KEY không được đặt — chạy với giới hạn Guest (20 req/min)."
        )
        return
    try:
        from vnstock.core.utils.auth import register_user

        success = register_user(api_key=api_key)
        if success:
            logger.info("Đã đăng ký VNSTOCK_API_KEY thành công (60 req/min).")
        else:
            logger.warning("Đăng ký VNSTOCK_API_KEY thất bại, tiếp tục với Guest tier.")
    except Exception as e:
        logger.warning(f"Không thể đăng ký VNSTOCK_API_KEY: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Environment: {settings.environment}")

    _register_vnstock_api_key()

    # Initialize database (create tables if they don't exist)
    # Note: In production, use Alembic migrations instead
    if settings.debug:
        logger.info("Initializing database tables...")

    notification_scheduler_runner.start()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    await notification_scheduler_runner.stop()
    logger.info("Shutting down application...")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs": "/docs" if settings.debug else None,
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get(f"{settings.api_v1_prefix}/info")
async def api_info():
    """API information endpoint."""
    return {
        "api_version": "v1",
        "app_name": settings.app_name,
        "app_version": settings.app_version,
        "environment": settings.environment,
    }


# Import and include routers
from app.api import auth, chat, market, notifications, optimize, portfolios

app.include_router(auth.router, prefix=settings.api_v1_prefix)
app.include_router(market.router, prefix=settings.api_v1_prefix)
app.include_router(portfolios.router, prefix=settings.api_v1_prefix)
app.include_router(optimize.router, prefix=settings.api_v1_prefix)
app.include_router(chat.router, prefix=settings.api_v1_prefix)
app.include_router(notifications.router, prefix=settings.api_v1_prefix)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )
