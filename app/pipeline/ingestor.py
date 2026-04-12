from __future__ import annotations

from duckdb import DuckDBPyConnection

from app.pipeline.models import PipelineFile


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


def mark_as_processed(conn: DuckDBPyConnection, file_key: str) -> None:
    conn.execute(
        """
        INSERT INTO processed_files (file_key)
        VALUES (?)
        """,
        [file_key],
    )


def ingest_file(conn: DuckDBPyConnection, file: PipelineFile) -> None:
    columns = TABLE_COLUMNS[file.entity]
    target_columns = ", ".join([*columns, "source_file"])

    conn.execute(
        f"""
        INSERT INTO {file.entity} ({target_columns})
        SELECT *, ? AS source_file
        FROM read_csv(
            ?,
            delim=';',
            header=false,
            all_varchar=true,
            ignore_errors=false,
            encoding='utf-8'
        )
        """,
        [file.file_key, str(file.staged_file_path)],
    )


def ingest(conn: DuckDBPyConnection, files: list[PipelineFile]) -> None:
    for file in files:
        try:
            ingest_file(conn, file)
            mark_as_processed(conn, file.file_key)
            print(f"[ingestor] carregado: {file.file_key} -> {file.entity}")
        except Exception as exc:
            print(f"[ingestor] erro: {file.file_key} -> {file.entity} | {exc}")