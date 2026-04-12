from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from app.core.config import PROCESSED_RAW_DIR, RAW_DIR, STAGED_DIR, ensure_directories


def is_relevant_file(file_name: str) -> bool:
    upper_name = file_name.upper()

    return any(
        token in upper_name
        for token in (
            "EMPRE",
            "ESTABELE",
            "SOCIO",
            "CNAE",
            "NATJU",
            "MUNIC",
        )
    )


def extract_main_zip(zip_path: Path, snapshot_root: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(snapshot_root)


def extract_nested_zips(snapshot_root: Path) -> None:
    inner_zips = sorted([path for path in snapshot_root.rglob("*.zip") if path.is_file()])

    for inner_zip in inner_zips:
        target_dir = inner_zip.with_suffix("")
        target_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(inner_zip, "r") as zip_file:
            for member in zip_file.infolist():
                if member.is_dir():
                    continue

                member_name = Path(member.filename).name

                if not is_relevant_file(member_name):
                    continue

                target_path = target_dir / member_name

                with zip_file.open(member, "r") as src, target_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

        inner_zip.unlink()


def flatten_snapshot_root(snapshot_root: Path) -> Path:
    children = [path for path in snapshot_root.iterdir()]

    if len(children) == 1 and children[0].is_dir() and children[0].name == snapshot_root.name:
        nested_root = children[0]

        for child in nested_root.iterdir():
            child.replace(snapshot_root / child.name)

        nested_root.rmdir()

    return snapshot_root


def collect() -> tuple[str, Path, Path] | None:
    ensure_directories()

    zip_files = sorted(
        [
            path
            for path in RAW_DIR.iterdir()
            if path.is_file() and path.suffix.lower() == ".zip"
        ]
    )

    print(f"[collector] {len(zip_files)} arquivo(s) ZIP encontrado(s) em {RAW_DIR}")

    if not zip_files:
        return None

    zip_path = zip_files[0]
    snapshot_name = zip_path.stem
    snapshot_root = STAGED_DIR / snapshot_name

    if snapshot_root.exists():
        print(f"[collector] reutilizando extração existente de {snapshot_name}")
    else:
        snapshot_root.mkdir(parents=True, exist_ok=True)
        print(f"[collector] extraindo {zip_path.name} em {STAGED_DIR}")

        extract_main_zip(zip_path, snapshot_root)
        snapshot_root = flatten_snapshot_root(snapshot_root)
        extract_nested_zips(snapshot_root)

    print(f"[collector] snapshot disponível em {snapshot_root}")
    return snapshot_name, snapshot_root, zip_path


def finalize_batch(snapshot_root: Path, zip_path: Path) -> None:
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)

    destination = PROCESSED_RAW_DIR / zip_path.name
    zip_path.replace(destination)

    print(f"[collector] lote concluído: {zip_path.name} movido para {destination}")