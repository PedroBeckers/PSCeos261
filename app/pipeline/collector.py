from __future__ import annotations

import zipfile
from pathlib import Path

from app.core.config import RAW_DIR, STAGED_DIR, ensure_directories


def collect() -> list[tuple[str, Path]]:
    ensure_directories()

    zip_files = sorted(
        [
            path
            for path in RAW_DIR.iterdir()
            if path.is_file() and path.suffix.lower() == ".zip"
        ]
    )

    print(f"[collector] {len(zip_files)} arquivo(s) ZIP encontrado(s) em {RAW_DIR}")

    snapshots: list[tuple[str, Path]] = []

    for zip_path in zip_files:
        snapshot_name = zip_path.stem
        snapshot_root = STAGED_DIR / snapshot_name

        if snapshot_root.exists():
            print(f"[collector] reutilizando extração existente de {snapshot_name}")
        else:
            print(f"[collector] extraindo {zip_path.name} em {STAGED_DIR}")

            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(STAGED_DIR)

        print(f"[collector] snapshot disponível em {snapshot_root}")
        snapshots.append((snapshot_name, snapshot_root))

    return snapshots