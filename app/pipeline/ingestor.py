from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import tempfile

from duckdb import DuckDBPyConnection


TABLE_COLUMNS: dict[str, list[str]] = {
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ],
    "cnaes": [
        "codigo",
        "descricao",
    ],
    "naturezas_juridicas": [
        "codigo",
        "descricao",
    ],
    "municipios": [
        "codigo",
        "descricao",
    ],
}


def register_ingestion(
    conn: DuckDBPyConnection,
    file_name: str,
    entity: str | None,
    stage: str,
    status: str,
    message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ingestion_control (file_name, entity, stage, status, message)
        VALUES (?, ?, ?, ?, ?)
        """,
        [file_name, entity, stage, status, message],
    )


def truncate_table(conn: DuckDBPyConnection, table_name: str) -> None:
    conn.execute(f"DELETE FROM {table_name}")


def normalize_file_to_utf8(file_path: Path) -> Path:
    temp_dir = Path(tempfile.gettempdir())
    normalized_path = temp_dir / f"{file_path.name}.utf8"

    encodings_to_try = ["utf-8", "latin-1", "cp1252"]

    last_error: Exception | None = None

    for encoding in encodings_to_try:
        try:
            with file_path.open("r", encoding=encoding, newline="") as source, \
                 normalized_path.open("w", encoding="utf-8", newline="") as target:
                for line in source:
                    target.write(line)
            return normalized_path
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        f"Nao foi possivel decodificar o arquivo {file_path} com utf-8, latin-1 ou cp1252. Ultimo erro: {last_error}"
    )


def ingest_csv_file(conn: DuckDBPyConnection, file_path: Path, entity: str) -> None:
    columns = TABLE_COLUMNS[entity]
    column_list = ", ".join(columns)

    normalized_file_path = normalize_file_to_utf8(file_path)

    conn.execute(
        f"""
        INSERT INTO {entity} ({column_list})
        SELECT *
        FROM read_csv(
            ?,
            delim=';',
            header=false,
            all_varchar=true,
            ignore_errors=false,
            encoding='utf-8'
        )
        """,
        [str(normalized_file_path)],
    )


def ingest(conn: DuckDBPyConnection, processed_files: list[tuple[Path, str]]) -> None:
    grouped: dict[str, list[Path]] = defaultdict(list)

    for file_path, entity in processed_files:
        grouped[entity].append(file_path)

    for entity, files in grouped.items():
        if entity == "desconhecido":
            for file_path in files:
                register_ingestion(
                    conn,
                    file_path.name,
                    None,
                    "ingestion",
                    "ignored",
                    "Tipo de arquivo não reconhecido",
                )
                print(f"[ingestor] ignorado: {file_path}")
            continue

        truncate_table(conn, entity)

        for file_path in sorted(files):
            try:
                ingest_csv_file(conn, file_path, entity)
                register_ingestion(
                    conn,
                    file_path.name,
                    entity,
                    "ingestion",
                    "success",
                    f"Arquivo carregado com sucesso em {entity}",
                )
                print(f"[ingestor] carregado: {file_path} -> {entity}")
            except Exception as exc:
                register_ingestion(
                    conn,
                    file_path.name,
                    entity,
                    "ingestion",
                    "error",
                    str(exc),
                )
                print(f"[ingestor] erro: {file_path} -> {entity} | {exc}")