from __future__ import annotations

from duckdb import DuckDBPyConnection


def initialize_database(conn: DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_key VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico VARCHAR PRIMARY KEY,
            razao_social VARCHAR,
            natureza_juridica VARCHAR,
            qualificacao_responsavel VARCHAR,
            capital_social VARCHAR,
            porte_empresa VARCHAR,
            ente_federativo_responsavel VARCHAR,
            source_file VARCHAR
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
            data_situacao_especial VARCHAR,
            source_file VARCHAR,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
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
            faixa_etaria VARCHAR,
            source_file VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cnaes (
            codigo VARCHAR PRIMARY KEY,
            descricao VARCHAR,
            source_file VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS naturezas_juridicas (
            codigo VARCHAR PRIMARY KEY,
            descricao VARCHAR,
            source_file VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS municipios (
            codigo VARCHAR PRIMARY KEY,
            descricao VARCHAR,
            source_file VARCHAR
        )
    """)