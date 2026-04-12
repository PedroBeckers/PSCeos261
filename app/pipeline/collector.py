from __future__ import annotations

from pathlib import Path

from app.core.config import RAW_DIR, ensure_directories


def list_raw_files() -> list[Path]:
    ensure_directories()
    return sorted(
        [
            path
            for path in RAW_DIR.iterdir()
            if path.is_file() and path.suffix.lower() == ".zip"
        ]
    )


def collect() -> list[Path]:
    files = list_raw_files()
    print(f"[collector] {len(files)} arquivo(s) ZIP encontrado(s) em {RAW_DIR}")

    for file_path in files:
        print(f"[collector] - {file_path.name}")

    return files