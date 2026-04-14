import marimo

__generated_with = "0.23.1"
app = marimo.App(width="full", app_title="Consulta entregas SDR")


@app.cell
async def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import sys
    import jinja2

    # Em ambiente de Navegador (WASM), precisamos puxar o Parquet do próprio repositório
    if sys.platform == "emscripten":
        import pyodide.http
        base_url = "https://r-giacomin.github.io/entregas_sdr/"
        
        # Download para o sistema de arquivos virtual
        res1 = await pyodide.http.pyfetch(base_url + "agregado_detalhado_por_convenio_ano.parquet")
        with open("agregado_detalhado_por_convenio_ano.parquet", "wb") as _f:
            _f.write(await res1.bytes())
            
        res2 = await pyodide.http.pyfetch(base_url + "classificacao_municipios_SDR.parquet")
        with open("classificacao_municipios_SDR.parquet", "wb") as _f:
            _f.write(await res2.bytes())

    # 1. Conexão e View
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW sdr_agregado AS SELECT * FROM 'agregado_detalhado_por_convenio_ano.parquet'")
    con.execute("CREATE OR REPLACE VIEW municipios AS SELECT * FROM 'classificacao_municipios_SDR.parquet'")

    # 1.1 Busca valores únicos para os filtros gerais
    tipologias = sorted(con.execute("SELECT DISTINCT Tipologia_PNDR_3 FROM municipios WHERE Tipologia_PNDR_3 IS NOT NULL").df()["Tipologia_PNDR_3"].tolist())

    # Busca nomes das rotas (colunas começando com R_)
    colunas = con.execute("DESCRIBE municipios").df()["column_name"].tolist()
    rotas = [c for c in colunas if c.startswith('R_')]
    opcoes_rotas = {r: sorted(con.execute(f"SELECT DISTINCT {r} FROM municipios WHERE {r} IS NOT NULL").df()[r].tolist()) for r in rotas}

    # 2. Busca os limites para o Range Slider
    anos_df = con.execute("SELECT DISTINCT ANO_pgto FROM sdr_agregado ORDER BY ANO_pgto").df()
    anos_int = [int(a) for a in anos_df["ANO_pgto"].tolist() if pd.notna(a)]
    ano_min = min(anos_int) if anos_int else 2000
    ano_max = max(anos_int) if anos_int else 2024
    return ano_max, ano_min, con, mo, opcoes_rotas, pd, rotas, tipologias


@app.cell
def _(ano_max, ano_min, con, mo, opcoes_rotas, rotas, tipologias):
    slicer_anos = mo.ui.range_slider(
        start=ano_min,
        stop=ano_max,
        step=1,
        value=(ano_min, ano_max),
        label="Período"
    )
    seletor_metrica = mo.ui.dropdown(
        options={
            "Valor Executado": "VALOR_AGREGADO",
            "Quantidade": "QTD_AGREGADA",
            "KMs Estimados": "KM_estimado",
            "População Beneficiária": "populacao",
            "Quantidade de Municípios": "qtde_municipios",
            "Número de Convênios": "nr_convenios"
        },
        value="Valor Executado",
        label="Métrica"
    )

    regioes = sorted(con.execute("SELECT DISTINCT nome_regiao FROM municipios WHERE nome_regiao IS NOT NULL").df()["nome_regiao"].tolist())
    filtro_regiao = mo.ui.multiselect(options=regioes, label="Região")

    flags = ["amazonia_legal", "SUDENE", "semiarido", "faixa_fronteira", "matopiba", "cidades_intermediadoras", "amazonia_azul"]
    titulos_flags = {
        "amazonia_legal": "Amazônia Legal",
        "SUDENE": "SUDENE",
        "semiarido": "Semiárido",
        "faixa_fronteira": "Faixa de Fronteira",
        "matopiba": "MATOPIBA",
        "cidades_intermediadoras": "Cidades Intermediadoras",
        "amazonia_azul": "Amazônia Azul"
    }
    filtro_flags = mo.ui.dictionary({
        f: mo.ui.dropdown(options=["Todos", "Sim", "Não"], value="Todos", label=titulos_flags[f]) 
        for f in flags
    })

    filtro_tipologia = mo.ui.multiselect(options=tipologias, label="Tipologia PNDR 3")

    filtros_rotas = mo.ui.dictionary({
        r: mo.ui.multiselect(options=opcoes_rotas[r], label=r.replace("R_", "Rota ").replace("_", " ").title()) 
        for r in rotas
    })

    return (
        filtro_flags,
        filtro_regiao,
        filtro_tipologia,
        filtros_rotas,
        seletor_metrica,
        slicer_anos,
    )


@app.cell
def _(con, filtro_regiao, mo):
    # Condição hierárquica para UFs baseada na região selecionada
    if filtro_regiao.value:
        _reg_list = ", ".join([f"'{r}'" for r in filtro_regiao.value])
        _q_uf = f"SELECT DISTINCT sigla_uf FROM municipios WHERE nome_regiao IN ({_reg_list}) AND sigla_uf IS NOT NULL"
    else:
        _q_uf = "SELECT DISTINCT sigla_uf FROM municipios WHERE sigla_uf IS NOT NULL"

    _ufs_list = sorted(con.execute(_q_uf).df()["sigla_uf"].tolist())
    filtro_uf = mo.ui.multiselect(options=_ufs_list, label="Sigla UF")

    return (filtro_uf,)


@app.cell
def _(con, filtro_regiao, filtro_uf, mo):
    # Condição hierárquica para Municípios baseada em Região e UF
    _conds = []
    if filtro_regiao.value:
        _reg_list = ", ".join([f"'{r}'" for r in filtro_regiao.value])
        _conds.append(f"nome_regiao IN ({_reg_list})")
    if filtro_uf.value:
        _uf_list = ", ".join([f"'{r}'" for r in filtro_uf.value])
        _conds.append(f"sigla_uf IN ({_uf_list})")

    _where = " AND ".join(_conds) if _conds else "1=1"
    _q_mun = f"SELECT DISTINCT nome FROM municipios WHERE {_where} AND nome IS NOT NULL"

    _municipios_list = sorted(con.execute(_q_mun).df()["nome"].tolist())
    # O input foi alterado para multiselect para respeitar perfeitamente o domínio dinâmico e suportar todas as seleções
    filtro_municipio = mo.ui.multiselect(options=_municipios_list, label="Município de Proponente")

    return (filtro_municipio,)


@app.cell
def _(
    filtro_municipio,
    filtro_regiao,
    filtro_uf,
    mo,
    seletor_metrica,
    slicer_anos,
):
    advanced_filters = mo.hstack(
        [filtro_regiao, filtro_uf, filtro_municipio], justify="start"
    )

    layout = mo.vstack([
        mo.hstack([slicer_anos, seletor_metrica], justify="start"),
        advanced_filters
    ])

    # A última expressão do bloco é exibida na tela do dashboard.
    layout

    return


@app.cell
def _(filtro_flags, filtro_tipologia, filtros_rotas, mo):
    sidebar_content = mo.vstack([
        mo.md("### 🌍 Abrangência"),
        *filtro_flags.values(),
        mo.md("---"),
        mo.md("### 📊 Tipologia"),
        filtro_tipologia,
        mo.md("---"),
        mo.md("### 🛣️ Rotas de Integração"),
        *filtros_rotas.values()
    ])

    sidebar_element = mo.sidebar(sidebar_content)
    sidebar_element

    return


@app.cell
def _(
    con,
    filtro_flags,
    filtro_municipio,
    filtro_regiao,
    filtro_tipologia,
    filtro_uf,
    filtros_rotas,
    mo,
    pd,
    seletor_metrica,
    slicer_anos,
):
    ano_inicio, ano_fim = slicer_anos.value

    def format_in(vals):
        if not vals: return ""
        items = ", ".join([f"'{v}'" for v in vals])
        return f"({items})"

    condicoes = [f"s.ANO_pgto BETWEEN {ano_inicio} AND {ano_fim}"]

    if filtro_municipio.value:
        condicoes.append(f"m.nome IN {format_in(filtro_municipio.value)}")

    if filtro_uf.value:
        condicoes.append(f"m.sigla_uf IN {format_in(filtro_uf.value)}")

    if filtro_regiao.value:
        condicoes.append(f"m.nome_regiao IN {format_in(filtro_regiao.value)}")

    for f, val in filtro_flags.value.items():
        if val == "Sim":
            condicoes.append(f"m.{f} = 1")
        elif val == "Não":
            condicoes.append(f"m.{f} = 0")

    if filtro_tipologia.value:
        condicoes.append(f"m.Tipologia_PNDR_3 IN {format_in(filtro_tipologia.value)}")

    for r, val in filtros_rotas.value.items():
        if val:
            condicoes.append(f"m.{r} IN {format_in(val)}")

    where_clause = " AND ".join(condicoes)

    if seletor_metrica.value == "populacao":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.COD_MUNIC_IBGE, m."População 2022" AS populacao, m.Tipologia_PNDR_3
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()
        df_filtrado_sdr = df_filtrado_sdr.drop_duplicates(subset=['Divisao', 'CATEGORIA_SUGERIDA', 'ANO_pgto', 'COD_MUNIC_IBGE'])

    elif seletor_metrica.value == "qtde_municipios":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.COD_MUNIC_IBGE, m.Tipologia_PNDR_3
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    elif seletor_metrica.value == "nr_convenios":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.NR_CONVENIO, m.Tipologia_PNDR_3
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    else:
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.{seletor_metrica.value}, m.Tipologia_PNDR_3
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    if df_filtrado_sdr.empty:
        dash_content = mo.md("⚠️ Nenhum dado encontrado para os filtros selecionados.")
    else:
        if seletor_metrica.value == "populacao":
            # Mapa da população para garantir a soma estritamente distinta baseada nos códigos de município (importante para os Totais)
            pop_map = df_filtrado_sdr.set_index('COD_MUNIC_IBGE')['populacao'].to_dict()
            aggfunc = lambda s: sum(pop_map[x] for x in s.unique() if x in pop_map and not pd.isna(pop_map[x]))
            val_col = "COD_MUNIC_IBGE"
        elif seletor_metrica.value in ["qtde_municipios", "nr_convenios"]:
            aggfunc = pd.Series.nunique
            val_col = "COD_MUNIC_IBGE" if seletor_metrica.value == "qtde_municipios" else "NR_CONVENIO"
        else:
            aggfunc = 'sum'
            val_col = seletor_metrica.value

        tabela_dinamica = pd.pivot_table(
            data=df_filtrado_sdr,
            index=['Divisao', 'CATEGORIA_SUGERIDA'],
            columns=['ANO_pgto'],
            values=val_col,
            aggfunc=aggfunc,
            fill_value=0,
            margins=True,
            margins_name='Total Geral'
        )

        tabela_divisao = pd.pivot_table(
            data=df_filtrado_sdr,
            index=['Divisao'],
            columns=['ANO_pgto'],
            values=val_col,
            aggfunc=aggfunc,
            fill_value=0,
            margins=True,
            margins_name='Total Geral'
        )

        tabela_tipologia = pd.pivot_table(
            data=df_filtrado_sdr,
            index=['Tipologia_PNDR_3'],
            columns=['ANO_pgto'],
            values=val_col,
            aggfunc=aggfunc,
            fill_value=0,
            margins=True,
            margins_name='Total Geral'
        )

        colunas_completas = list(range(ano_inicio, ano_fim + 1)) + ['Total Geral']
        tabela_dinamica = tabela_dinamica.reindex(columns=colunas_completas, fill_value=0)
        tabela_divisao = tabela_divisao.reindex(columns=colunas_completas, fill_value=0)
        tabela_tipologia = tabela_tipologia.reindex(columns=colunas_completas, fill_value=0)

        # Ordenação customizada para tipologias
        ordem_tipologia_desejada = [
            "Alta Renda",
            "Média Renda e Alto Dinamismo",
            "Média Renda e Médio Dinamismo",
            "Média Renda e Baixo Dinamismo",
            "Baixa Renda e Alto Dinamismo",
            "Baixa Renda e Médio Dinamismo",
            "Baixa Renda e Baixo Dinamismo"
        ]
        outros = [t for t in tabela_tipologia.index if t not in ordem_tipologia_desejada and t != "Total Geral"]
        ordem_final_index = ordem_tipologia_desejada + outros + ["Total Geral"]
        # reindex(index=...) reorganiza as linhas, as ausentes serão adicionadas com NaN por isso fillna(0)
        tabela_tipologia = tabela_tipologia.reindex(index=ordem_final_index).fillna(0)

        # Trata melhor casos nulos (NaN) e regras de formatação visual
        def fmt_moeda(v): return "-" if pd.isna(v) or v == 0 else f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        def fmt_int(v): return "-" if pd.isna(v) or v == 0 else f"{int(v):,}".replace(",", ".")
        def fmt_float(v): return "-" if pd.isna(v) or v == 0 else f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        if seletor_metrica.value == "VALOR_AGREGADO":
            formatador = fmt_moeda
        elif seletor_metrica.value in ["Quantidade Agregada", "populacao", "qtde_municipios", "nr_convenios"]:
            formatador = fmt_int
        else:
            formatador = fmt_float

        estilos_css = [
            {'selector': 'th', 'props': [('text-align', 'center'), ('font-weight', 'bold'), ('padding', '10px 12px'), ('border-bottom', '2px solid rgba(128, 128, 128, 0.5)')]},
            {'selector': 'th.row_heading', 'props': [('text-align', 'left')]},
            {'selector': 'tr:hover', 'props': [('background-color', 'rgba(128, 128, 128, 0.1)')]},
            {'selector': 'tr:last-child', 'props': [('font-weight', 'bold'), ('border-top', '2px solid rgba(128, 128, 128, 0.8)')]}
        ]
        propriedades_css = {
            'text-align': 'right', 'padding': '6px 12px',
            'border-bottom': '1px solid rgba(128, 128, 128, 0.2)', 'white-space': 'nowrap'
        }

        estilo_tabela = (
            tabela_dinamica.style
            .format(formatador)
            .set_properties(**propriedades_css)
            .set_table_styles(estilos_css)
        )

        estilo_tabela_divisao = (
            tabela_divisao.style
            .format(formatador)
            .set_properties(**propriedades_css)
            .set_table_styles(estilos_css)
        )

        estilo_tabela_tipologia = (
            tabela_tipologia.style
            .format(formatador)
            .set_properties(**propriedades_css)
            .set_table_styles(estilos_css)
        )

        nomes_metricas = {
            "VALOR_AGREGADO": "Valor Agregado",
            "QTD_AGREGADA": "Quantidade",
            "KM_estimado": "KMs Estimados",
            "populacao": "População Beneficiária",
            "qtde_municipios": "Quantidade de Municípios",
            "nr_convenios": "Número de Convênios"
        }
        titulo_metrica = nomes_metricas.get(seletor_metrica.value, "Métrica Selecionada")

        dash_content = mo.vstack([
            mo.md(f"### Evolução por {titulo_metrica} (Resumo por Divisão)"),
            mo.Html(f"<div style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_divisao.to_html()}</div>"),
            mo.md(f"### Detalhamento por Categoria"),
            mo.Html(f"<div style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela.to_html()}</div>"),
            mo.md(f"### Resumo por Tipologia PNDR 3"),
            mo.Html(f"<div style='width: 100%; max-width: 100%; overflow-x: auto;'>{estilo_tabela_tipologia.to_html()}</div>")
        ])

    # A última expressão do bloco é exibida na tela do dashboard.
    dash_content

    return


if __name__ == "__main__":
    app.run()
