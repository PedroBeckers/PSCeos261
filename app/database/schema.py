from __future__ import annotations

from duckdb import DuckDBPyConnection


def initialize_database(conn: DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_control (
            file_name VARCHAR NOT NULL,
            entity VARCHAR,
            stage VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico VARCHAR,
            razao_social VARCHAR,
            natureza_juridica VARCHAR,
            qualificacao_responsavel VARCHAR,
            capital_social VARCHAR,
            porte_empresa VARCHAR,
            ente_federativo_responsavel VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS estabelecimentos (
            cnpj_basico VARCHAR,
            cnpj_ordem VARCHAR,
            cnpj_dv VARCHAR,
            identificador_matriz_filial VARCHAR,
            nome_fantasia VARCHAR,
            situacao_cadastral VARCHAR,
            data_situacao_cadastral VARCHAR,
            motivo_situacao_cadastral VARCHAR,
            nome_cidade_exterior VARCHAR,
            pais VARCHAR,
            data_inicio_atividade VARCHAR,
            cnae_fiscal_principal VARCHAR,
            cnae_fiscal_secundaria VARCHAR,
            tipo_logradouro VARCHAR,
            logradouro VARCHAR,
            numero VARCHAR,
            complemento VARCHAR,
            bairro VARCHAR,
            cep VARCHAR,
            uf VARCHAR,
            municipio VARCHAR,
            ddd_1 VARCHAR,
            telefone_1 VARCHAR,
            ddd_2 VARCHAR,
            telefone_2 VARCHAR,
            ddd_fax VARCHAR,
            fax VARCHAR,
            correio_eletronico VARCHAR,
            situacao_especial VARCHAR,
            data_situacao_especial VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS socios (
            cnpj_basico VARCHAR,
            identificador_socio VARCHAR,
            nome_socio VARCHAR,
            cnpj_cpf_socio VARCHAR,
            qualificacao_socio VARCHAR,
            data_entrada_sociedade VARCHAR,
            pais VARCHAR,
            representante_legal VARCHAR,
            nome_representante VARCHAR,
            qualificacao_representante_legal VARCHAR,
            faixa_etaria VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cnaes (
            codigo VARCHAR,
            descricao VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS naturezas_juridicas (
            codigo VARCHAR,
            descricao VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS municipios (
            codigo VARCHAR,
            descricao VARCHAR
        )
    """)