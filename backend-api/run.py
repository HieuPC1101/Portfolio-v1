"""
Quick start script for backend API.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def get_project_python(
    current_executable: str | None = None, project_dir: str | None = None
) -> str | None:
    """Return the project's venv Python when the current interpreter is external."""
    base_dir = Path(project_dir or os.path.dirname(os.path.abspath(__file__)))
    venv_python = base_dir / ".venv" / "Scripts" / "python.exe"
    if not venv_python.exists():
        return None

    current = Path(current_executable or sys.executable)
    if current.resolve() == venv_python.resolve():
        return None

    return str(venv_python)


def ensure_project_python() -> None:
    """Re-launch the script with the project's virtualenv when available."""
    preferred_python = get_project_python()
    if not preferred_python:
        return

    print(f"Switching to project virtual environment: {preferred_python}")
    os.execv(preferred_python, [preferred_python, os.path.abspath(__file__), *sys.argv[1:]])

if __name__ == "__main__":
    ensure_project_python()

    import uvicorn
    from app.config import settings

    print(f"Starting {settings.app_name} v{settings.app_version}")
    print(f"Environment: {settings.environment}")
    print(f"Server: http://localhost:8000")
    print(f"Docs: http://localhost:8000/docs")
    print(f"Debug mode: {settings.debug}")
    print("-" * 50)

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
