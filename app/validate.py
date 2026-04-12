from __future__ import annotations

from duckdb import DuckDBPyConnection

from app.database.connection import get_connection


TABLES = [
    "empresas",
    "estabelecimentos",
    "socios",
    "cnaes",
    "naturezas_juridicas",
    "municipios",
]


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_result(columns: list[str], rows: list[tuple]) -> None:
    print("Colunas:", ", ".join(columns))

    if not rows:
        print("Nenhum registro encontrado.")
        return

    for row in rows:
        print(row)


def execute_and_print(
    conn: DuckDBPyConnection,
    title: str,
    query: str,
    params: list[str] | None = None,
) -> None:
    print_section(title)

    result = conn.execute(query, params or [])
    columns = [description[0] for description in result.description]
    rows = result.fetchall()

    print_result(columns, rows)


def validate_table_counts(conn: DuckDBPyConnection) -> None:
    print_section("CONTAGEM DE REGISTROS POR TABELA")

    print("Colunas: tabela, quantidade")
    for table in TABLES:
        count = conn.execute(f"SELECT COUNT(*) AS quantidade FROM {table}").fetchone()[0]
        print((table, count))


def validate_table_samples(conn: DuckDBPyConnection) -> None:
    print_section("AMOSTRAS DAS TABELAS")

    for table in TABLES:
        result = conn.execute(f"SELECT * FROM {table} LIMIT 5")
        columns = [description[0] for description in result.description]
        rows = result.fetchall()

        print(f"\n--- {table} ---")
        print_result(columns, rows)


def validate_join_empresa_natureza(conn: DuckDBPyConnection) -> None:
    execute_and_print(
        conn,
        "JOIN: EMPRESA + NATUREZA JURIDICA",
        """
        SELECT
            e.cnpj_basico,
            e.razao_social,
            e.natureza_juridica,
            nj.descricao AS natureza_descricao
        FROM empresas e
        LEFT JOIN naturezas_juridicas nj
            ON e.natureza_juridica = nj.codigo
        LIMIT 10
        """,
    )


def validate_join_estabelecimento_municipio_cnae(conn: DuckDBPyConnection) -> None:
    execute_and_print(
        conn,
        "JOIN: ESTABELECIMENTO + MUNICIPIO + CNAE",
        """
        SELECT
            est.cnpj_basico,
            est.cnpj_ordem,
            est.cnpj_dv,
            est.nome_fantasia,
            est.uf,
            est.municipio,
            m.descricao AS municipio_descricao,
            est.cnae_fiscal_principal,
            c.descricao AS cnae_principal_descricao
        FROM estabelecimentos est
        LEFT JOIN municipios m
            ON est.municipio = m.codigo
        LEFT JOIN cnaes c
            ON est.cnae_fiscal_principal = c.codigo
        LIMIT 10
        """,
    )


def validate_socios_por_empresa(conn: DuckDBPyConnection) -> None:
    print_section("SOCIOS POR EMPRESA")

    result = conn.execute(
        """
        SELECT cnpj_basico
        FROM socios
        WHERE cnpj_basico IS NOT NULL
          AND cnpj_basico <> ''
        LIMIT 1
        """
    ).fetchone()

    if not result:
        print("Nenhum sócio encontrado para validação.")
        return

    cnpj_basico = result[0]
    print(f"CNPJ básico usado no teste: {cnpj_basico}")

    query_result = conn.execute(
        """
        SELECT
            s.cnpj_basico,
            s.nome_socio,
            s.cnpj_cpf_socio,
            s.qualificacao_socio,
            s.data_entrada_sociedade
        FROM socios s
        WHERE s.cnpj_basico = ?
        LIMIT 10
        """,
        [cnpj_basico],
    )

    columns = [description[0] for description in query_result.description]
    rows = query_result.fetchall()
    print_result(columns, rows)


def validate_busca_razao_social(conn: DuckDBPyConnection) -> None:
    print_section("BUSCA POR RAZAO SOCIAL")

    result = conn.execute(
        """
        SELECT SUBSTR(razao_social, 1, 4) AS trecho
        FROM empresas
        WHERE razao_social IS NOT NULL
          AND razao_social <> ''
        LIMIT 1
        """
    ).fetchone()

    if not result:
        print("Nenhuma razão social encontrada para validação.")
        return

    termo = result[0]
    print(f"Trecho usado na busca: {termo}")

    query_result = conn.execute(
        """
        SELECT
            cnpj_basico,
            razao_social,
            natureza_juridica,
            porte_empresa
        FROM empresas
        WHERE UPPER(razao_social) LIKE UPPER(?)
        LIMIT 10
        """,
        [f"%{termo}%"],
    )

    columns = [description[0] for description in query_result.description]
    rows = query_result.fetchall()
    print_result(columns, rows)


def validate_busca_cnpj_completo(conn: DuckDBPyConnection) -> None:
    execute_and_print(
        conn,
        "MONTAGEM DE CNPJ COMPLETO",
        """
        SELECT
            cnpj_basico,
            cnpj_ordem,
            cnpj_dv,
            cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj_completo,
            nome_fantasia,
            uf
        FROM estabelecimentos
        LIMIT 10
        """,
    )


def validate_ingestion_logs(conn: DuckDBPyConnection) -> None:
    execute_and_print(
        conn,
        "LOGS DE INGESTAO",
        """
        SELECT
            file_name,
            entity,
            stage,
            status,
            processed_at,
            message
        FROM ingestion_control
        ORDER BY processed_at DESC
        LIMIT 20
        """,
    )


def main() -> None:
    conn = get_connection()

    try:
        validate_table_counts(conn)
        validate_table_samples(conn)
        validate_join_empresa_natureza(conn)
        validate_join_estabelecimento_municipio_cnae(conn)
        validate_socios_por_empresa(conn)
        validate_busca_razao_social(conn)
        validate_busca_cnpj_completo(conn)
        validate_ingestion_logs(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()