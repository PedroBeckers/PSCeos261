from __future__ import annotations

from pathlib import Path

from duckdb import DuckDBPyConnection

from app.pipeline.models import PipelineFile


def classify_entity(file_path: Path) -> str:
    parent_name = file_path.parent.name.lower()

    if parent_name.startswith("empresa"):
        return "empresas"
    if parent_name.startswith("estabelecimento"):
        return "estabelecimentos"
    if parent_name.startswith("socio"):
        return "socios"
    if parent_name.startswith("cnae"):
        return "cnaes"
    if parent_name.startswith("natureza"):
        return "naturezas_juridicas"
    if parent_name.startswith("municipio"):
        return "municipios"

    return "desconhecido"


def load_processed_keys(conn: DuckDBPyConnection) -> set[str]:
    rows = conn.execute("""
        SELECT file_key
        FROM processed_files
    """).fetchall()
    return {row[0] for row in rows}


def normalize_file_to_utf8(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)

    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            with source_path.open("r", encoding=encoding, newline="") as src, \
                 target_path.open("w", encoding="utf-8", newline="") as dst:
                for line in src:
                    dst.write(line)
            return
        except UnicodeDecodeError:
            continue

    raise RuntimeError(f"Erro ao converter encoding: {source_path}")


def process(
    conn: DuckDBPyConnection,
    snapshots: list[tuple[str, Path]],
) -> list[PipelineFile]:
    processed_keys = load_processed_keys(conn)
    pipeline_files: list[PipelineFile] = []

    for snapshot_name, snapshot_root in snapshots:
        for file_path in sorted([p for p in snapshot_root.rglob("*") if p.is_file()]):
            entity = classify_entity(file_path)

            if entity == "desconhecido":
                print(f"[processor] ignorado (entidade desconhecida): {file_path}")
                continue

            relative_inside_snapshot = file_path.relative_to(snapshot_root).as_posix()
            file_key = f"{snapshot_name}/{relative_inside_snapshot}"

            if file_key in processed_keys:
                print(f"[processor] ignorado (já processado): {file_key}")
                continue

            normalized_path = file_path.with_name(f"{file_path.name}.utf8")
            normalize_file_to_utf8(file_path, normalized_path)

            pipeline_files.append(
                PipelineFile(
                    file_key=file_key,
                    snapshot=snapshot_name,
                    entity=entity,
                    staged_file_path=normalized_path,
                )
            )

            print(f"[processor] {file_key} -> {entity}")

    print(f"[processor] {len(pipeline_files)} arquivo(s) pendente(s)")
    return pipeline_files