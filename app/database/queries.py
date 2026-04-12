SEARCH_EMPRESAS_BY_RAZAO = """
    SELECT
        e.cnpj_basico,
        e.razao_social,
        nj.descricao AS natureza_juridica,
        e.porte_empresa,
        e.capital_social
    FROM empresas e
    LEFT JOIN naturezas_juridicas nj
        ON e.natureza_juridica = nj.codigo
    WHERE UPPER(e.razao_social) LIKE UPPER(?)
    LIMIT 50
"""

COUNT_EMPRESAS_BY_RAZAO = """
    SELECT COUNT(*) AS total
    FROM empresas
    WHERE UPPER(razao_social) LIKE UPPER(?)
"""

SEARCH_ESTABELECIMENTOS_BY_CNPJ_COMPLETO = """
    SELECT
        est.cnpj_basico,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj_completo,
        e.razao_social,
        nj.descricao AS natureza_juridica,
        e.porte_empresa,
        e.capital_social
    FROM estabelecimentos est
    LEFT JOIN empresas e
        ON est.cnpj_basico = e.cnpj_basico
    LEFT JOIN naturezas_juridicas nj
        ON e.natureza_juridica = nj.codigo
    WHERE est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv = ?
    LIMIT 1
"""

LIST_ESTABELECIMENTOS_BY_CNPJ_BASICO = """
    SELECT
        est.cnpj_basico,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj_completo,
        est.identificador_matriz_filial,
        est.nome_fantasia,
        est.situacao_cadastral,
        est.uf,
        m.descricao AS municipio,
        est.cnae_fiscal_principal,
        c.descricao AS cnae_principal
    FROM estabelecimentos est
    LEFT JOIN municipios m
        ON est.municipio = m.codigo
    LEFT JOIN cnaes c
        ON est.cnae_fiscal_principal = c.codigo
    WHERE est.cnpj_basico = ?
    ORDER BY est.cnpj_ordem, est.cnpj_dv
"""

LIST_ESTABELECIMENTOS_BY_FILTERS = """
    SELECT
        est.cnpj_basico,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj_completo,
        e.razao_social,
        est.nome_fantasia,
        est.situacao_cadastral,
        est.uf,
        m.descricao AS municipio,
        est.cnae_fiscal_principal,
        c.descricao AS cnae_principal
    FROM estabelecimentos est
    LEFT JOIN empresas e
        ON est.cnpj_basico = e.cnpj_basico
    LEFT JOIN municipios m
        ON est.municipio = m.codigo
    LEFT JOIN cnaes c
        ON est.cnae_fiscal_principal = c.codigo
    WHERE (? = '' OR est.uf = ?)
      AND (? = '' OR m.descricao = ?)
    LIMIT 100
"""

COUNT_ESTABELECIMENTOS_BY_FILTERS = """
    SELECT COUNT(*)
    FROM estabelecimentos est
    LEFT JOIN municipios m
        ON est.municipio = m.codigo
    WHERE (? = '' OR est.uf = ?)
      AND (? = '' OR m.descricao = ?)
"""

LIST_SOCIOS_BY_CNPJ_BASICO = """
    SELECT
        nome_socio,
        cnpj_cpf_socio,
        qualificacao_socio,
        data_entrada_sociedade,
        faixa_etaria
    FROM socios
    WHERE cnpj_basico = ?
    LIMIT 100
"""

LIST_UFS = """
    SELECT DISTINCT uf
    FROM estabelecimentos
    WHERE uf IS NOT NULL
      AND uf <> ''
    ORDER BY uf
"""

LIST_MUNICIPIOS = """
    SELECT DISTINCT descricao
    FROM municipios
    WHERE descricao IS NOT NULL
      AND descricao <> ''
    ORDER BY descricao
"""