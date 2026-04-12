from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import re
import streamlit as st

from app.database.connection import get_connection
from app.database.queries import (
    COUNT_EMPRESAS_BY_RAZAO,
    COUNT_ESTABELECIMENTOS_BY_FILTERS,
    LIST_ESTABELECIMENTOS_BY_CNPJ_BASICO,
    LIST_ESTABELECIMENTOS_BY_FILTERS,
    LIST_MUNICIPIOS,
    LIST_SOCIOS_BY_CNPJ_BASICO,
    LIST_UFS,
    SEARCH_EMPRESAS_BY_RAZAO,
    SEARCH_ESTABELECIMENTOS_BY_CNPJ_COMPLETO,
)


def clean_cnpj(value: str) -> str:
    return re.sub(r"\D", "", value)


def format_cnpj(cnpj: str) -> str:
    digits = clean_cnpj(cnpj)
    if len(digits) != 14:
        return cnpj
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"


def format_matriz_filial(value: str | None) -> str:
    if value == "1":
        return "Matriz"
    if value == "2":
        return "Filial"
    return value or ""


def format_situacao(value: str | None) -> str:
    mapping = {
        "01": "Nula",
        "02": "Ativa",
        "03": "Suspensa",
        "04": "Inapta",
        "08": "Baixada",
    }
    if value is None:
        return ""
    return mapping.get(value.zfill(2), value)


def get_ufs(conn):
    return [row[0] for row in conn.execute(LIST_UFS).fetchall()]


def get_municipios(conn):
    return [row[0] for row in conn.execute(LIST_MUNICIPIOS).fetchall()]


def run() -> None:
    st.set_page_config(page_title="Consulta CNPJ", layout="wide")
    st.title("Consulta de Dados do CNPJ")

    conn = get_connection()

    try:
        tab1, tab2, tab3 = st.tabs(
            ["Buscar por razão social", "Buscar por CNPJ", "Filtrar estabelecimentos"]
        )

        # ======================
        # BUSCA POR RAZAO SOCIAL
        # ======================
        with tab1:
            st.subheader("Busca por razão social")
            termo = st.text_input("Digite parte da razão social")

            if termo.strip():
                total = conn.execute(
                    COUNT_EMPRESAS_BY_RAZAO,
                    [f"%{termo.strip()}%"],
                ).fetchone()[0]

                resultados = conn.execute(
                    SEARCH_EMPRESAS_BY_RAZAO,
                    [f"%{termo.strip()}%"],
                ).fetchdf()

                exibidos = len(resultados)

                st.write(f"Mostrando {exibidos} de {total} resultados")
                st.dataframe(resultados, use_container_width=True)

        # ======================
        # BUSCA POR CNPJ
        # ======================
        with tab2:
            st.subheader("Busca por CNPJ")
            cnpj_input = st.text_input("Digite o CNPJ completo")
            cnpj_limpo = clean_cnpj(cnpj_input)

            if cnpj_input.strip():
                if len(cnpj_limpo) != 14:
                    st.warning("Digite um CNPJ válido com 14 dígitos.")
                else:
                    empresa = conn.execute(
                        SEARCH_ESTABELECIMENTOS_BY_CNPJ_COMPLETO,
                        [cnpj_limpo],
                    ).fetchdf()

                    if empresa.empty:
                        st.warning("Nenhum resultado encontrado.")
                    else:
                        row = empresa.iloc[0]

                        st.subheader("Dados principais")
                        st.write(f"**CNPJ:** {format_cnpj(row['cnpj_completo'])}")
                        st.write(f"**Razão social:** {row['razao_social']}")
                        st.write(f"**Natureza jurídica:** {row['natureza_juridica']}")
                        st.write(f"**Porte:** {row['porte_empresa']}")
                        st.write(f"**Capital social:** {row['capital_social']}")

                        cnpj_basico = row["cnpj_basico"]

                        estabelecimentos = conn.execute(
                            LIST_ESTABELECIMENTOS_BY_CNPJ_BASICO,
                            [cnpj_basico],
                        ).fetchdf()

                        estabelecimentos["cnpj_completo"] = estabelecimentos["cnpj_completo"].apply(format_cnpj)
                        estabelecimentos["identificador_matriz_filial"] = estabelecimentos[
                            "identificador_matriz_filial"
                        ].apply(format_matriz_filial)
                        estabelecimentos["situacao_cadastral"] = estabelecimentos[
                            "situacao_cadastral"
                        ].apply(format_situacao)

                        st.subheader("Estabelecimentos")
                        st.dataframe(estabelecimentos, use_container_width=True)

                        socios = conn.execute(
                            LIST_SOCIOS_BY_CNPJ_BASICO,
                            [cnpj_basico],
                        ).fetchdf()

                        st.subheader("Sócios")
                        st.dataframe(socios, use_container_width=True)

        # ======================
        # FILTROS
        # ======================
        with tab3:
            st.subheader("Filtrar estabelecimentos")

            ufs = [""] + get_ufs(conn)
            municipios = [""] + get_municipios(conn)

            col1, col2 = st.columns(2)

            with col1:
                uf = st.selectbox("UF", options=ufs, format_func=lambda x: "Todas" if x == "" else x)

            with col2:
                municipio = st.selectbox(
                    "Município",
                    options=municipios,
                    format_func=lambda x: "Todos" if x == "" else x,
                )

            total = conn.execute(
                COUNT_ESTABELECIMENTOS_BY_FILTERS,
                [uf, uf, municipio, municipio],
            ).fetchone()[0]

            resultados = conn.execute(
                LIST_ESTABELECIMENTOS_BY_FILTERS,
                [uf, uf, municipio, municipio],
            ).fetchdf()

            exibidos = len(resultados)

            resultados["cnpj_completo"] = resultados["cnpj_completo"].apply(format_cnpj)
            resultados["situacao_cadastral"] = resultados["situacao_cadastral"].apply(format_situacao)

            st.write(f"Mostrando {exibidos} de {total} resultados")
            st.dataframe(resultados, use_container_width=True)

    finally:
        conn.close()


if __name__ == "__main__":
    run()