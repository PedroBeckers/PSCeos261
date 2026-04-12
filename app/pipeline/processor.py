from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from app.core.config import STAGED_DIR, ensure_directories


def clear_staged_directory() -> None:
    ensure_directories()

    if STAGED_DIR.exists():
        for item in STAGED_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


def extract_zip(zip_path: Path) -> list[Path]:
    extracted_files: list[Path] = []

    destination_dir = STAGED_DIR / zip_path.stem
    destination_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(destination_dir)

    for path in destination_dir.rglob("*"):
        if path.is_file():
            extracted_files.append(path)

    return sorted(extracted_files)


def classify_file(file_path: Path) -> str:
    parts = [part.lower() for part in file_path.parts]

    for part in reversed(parts):
        if part.startswith("empresa"):
            return "empresas"
        if part.startswith("estabelecimento"):
            return "estabelecimentos"
        if part.startswith("socio"):
            return "socios"
        if part.startswith("cnae"):
            return "cnaes"
        if part.startswith("natureza"):
            return "naturezas_juridicas"
        if part.startswith("municipio"):
            return "municipios"

    return "desconhecido"


def process(zip_files: list[Path]) -> list[tuple[Path, str]]:
    clear_staged_directory()

    processed_files: list[tuple[Path, str]] = []

    for zip_path in zip_files:
        print(f"[processor] extraindo {zip_path.name}")
        extracted_files = extract_zip(zip_path)

        for file_path in extracted_files:
            entity = classify_file(file_path)
            print(f"[processor] {file_path} -> {entity}")
            processed_files.append((file_path, entity))

    return processed_files