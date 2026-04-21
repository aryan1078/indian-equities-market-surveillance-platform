from pathlib import Path

from .settings import get_settings


def ensure_runtime_dirs() -> None:
    settings = get_settings()
    required = [
        settings.data_root,
        settings.data_root / "raw",
        settings.data_root / "replay",
        settings.data_root / "logs",
        settings.data_root / "exports",
        Path("./datasets"),
        Path("./tmp"),
    ]
    for path in required:
        path.mkdir(parents=True, exist_ok=True)

