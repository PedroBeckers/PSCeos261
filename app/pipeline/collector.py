from __future__ import annotations

import zipfile
from pathlib import Path

from app.core.config import RAW_DIR, STAGED_DIR, ensure_directories


def collect() -> list[Path]:
    ensure_directories()

    zip_files = sorted(
        [
            path
            for path in RAW_DIR.iterdir()
            if path.is_file() and path.suffix.lower() == ".zip"
        ]
    )

    print(f"[collector] {len(zip_files)} arquivo(s) ZIP encontrado(s) em {RAW_DIR}")

    snapshot_dirs: list[Path] = []

    for zip_path in zip_files:
        snapshot_name = zip_path.name
        extract_dir = STAGED_DIR / snapshot_name

        if extract_dir.exists():
            print(f"[collector] reutilizando extração existente de {snapshot_name}")
        else:
            print(f"[collector] extraindo {snapshot_name}")
            extract_dir.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(extract_dir)

        snapshot_dirs.append(extract_dir)

    return snapshot_dirs