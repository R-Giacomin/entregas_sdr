# marimo: requirements=["pandas", "duckdb", "openpyxl", "plotly", "folium"]

import marimo

__generated_with = "0.23.1"
app = marimo.App(width="full", app_title="Consulta entregas SDR")


@app.cell
async def _():
    import marimo as mo
    import duckdb
    import sys

    # Em ambiente de Navegador (WASM), precisamos puxar os pacotes explicitamente ANTES do Pandas
    if sys.platform == "emscripten":
        import micropip
        await micropip.install(["Jinja2", "pandas", "openpyxl", "plotly", "folium"])
        import pyodide.http
        base_url = "https://r-giacomin.github.io/entregas_sdr/"

        # Download para o sistema de arquivos virtual
        res1 = await pyodide.http.pyfetch(base_url + "agregado_detalhado_por_convenio_ano.parquet")
        with open("agregado_detalhado_por_convenio_ano.parquet", "wb") as _f:
            _f.write(await res1.bytes())

        res2 = await pyodide.http.pyfetch(base_url + "classificacao_municipios_SDR.parquet")
        with open("classificacao_municipios_SDR.parquet", "wb") as _f:
            _f.write(await res2.bytes())

        res3 = await pyodide.http.pyfetch(base_url + "a_executar.parquet")
        with open("a_executar.parquet", "wb") as _f:
            _f.write(await res3.bytes())

        import os
        os.makedirs("assets", exist_ok=True)
        res4 = await pyodide.http.pyfetch(base_url + "assets/municipios_br_simpl.geojson")
        with open("assets/municipios_br_simpl.geojson", "wb") as _f:
            _f.write(await res4.bytes())

    import pandas as pd
    import jinja2
    import plotly.express as px
    import folium
    import json

    # 1. Conexão e View
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE VIEW sdr_agregado AS SELECT * FROM 'agregado_detalhado_por_convenio_ano.parquet'")
    con.execute("CREATE OR REPLACE VIEW a_executar AS SELECT * FROM 'a_executar.parquet'")
    con.execute("""
        CREATE OR REPLACE VIEW municipios AS
        SELECT *,
            CASE WHEN nome_regiao = 'Centro-Oeste' THEN 1 ELSE 0 END AS SUDECO
        FROM 'classificacao_municipios_SDR.parquet'
    """)

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

    # 3. Busca valores únicos para os novos filtros de situação
    situacoes_convenio = sorted(con.execute("SELECT DISTINCT SIT_CONVENIO FROM sdr_agregado WHERE SIT_CONVENIO IS NOT NULL").df()["SIT_CONVENIO"].tolist())
    instrumentos_ativos = sorted(con.execute("SELECT DISTINCT INSTRUMENTO_ATIVO FROM sdr_agregado WHERE INSTRUMENTO_ATIVO IS NOT NULL").df()["INSTRUMENTO_ATIVO"].tolist())
    origens_recurso = sorted(con.execute("SELECT DISTINCT ORIGEM_RECURSO FROM sdr_agregado WHERE ORIGEM_RECURSO IS NOT NULL").df()["ORIGEM_RECURSO"].tolist())

    # Mapeamento de labels para os códigos RP
    labels_origem = {
        'RP2': 'RP2 — Discricionário',
        'RP6': 'RP6 — Emenda Individual',
        'RP7': 'RP7 — Emenda de Bancada',
        'RP8': 'RP8 — Emenda de Comissão',
        'RP9': 'RP9 — Emenda de Relator',
        'RP2 / RP9': 'RP2/RP9 — Misto',
        'Discricionário / Não identificado': 'Discricionário / Não identificado',
    }
    origens_recurso_labels = {labels_origem.get(o, o): o for o in origens_recurso}


    # 4. Busca valores únicos para os filtros de Programa e Ação
    programas_raw = sorted(con.execute("SELECT DISTINCT PROGRAMA FROM sdr_agregado WHERE PROGRAMA IS NOT NULL AND PROGRAMA != ''").df()["PROGRAMA"].tolist())
    acoes_df = con.execute("SELECT DISTINCT PROGRAMA, ACAO FROM sdr_agregado WHERE ACAO IS NOT NULL AND ACAO != ''").df()

    # Mapeamento de nomes das ações orçamentárias
    nomes_acoes = {
        "7W59": "Implantação do Projeto Sul-Fronteira",
        "00SX": "Apoio a Projetos de Desenvolvimento Sustentável Local Integrado",
        "10T2": "Apoio a Projetos e Obras de Reabilitação, de Acessibilidade e Modernização Tecnológica em Áreas Urbanas",
        "20NK": "Estruturação e Dinamização de Arranjos Produtivos Locais em Espaços Sub-regionais",
        "20WQ": "Gestão de Políticas de Desenvolvimento Regional e Ordenamento Territorial",
        "214S": "Estruturação e Dinamização de Atividades Produtivas - Rotas de Integração Nacional",
        "1211": "Implementação de Infraestrutura Básica — Calha Norte",
        "1851": "Aquisição de Equipamentos e/ou Implantação de Obras de Infraestrutura Hídrica",
        "7K66": "Apoio a Projetos de Desenvolvimento Sustentável Local Integrado",
        "12QC": "Implantação de Obras e Equipamentos para Oferta de Água — Plano Brasil sem Miséria",
        "0021": "Recuperação e Preservação de Bacias Hidrográficas",
        "0035": "Recuperação e Preservação de Bacias Hidrográficas",
        "217V": "Apoio a Projetos de Ampliação do Acesso à Água por Meio de Tecnologias Sustentáveis",
        "6553": "Apoio a Implantação de Infraestrutura Complementar, Social e Produtiva na Faixa de Fronteira",
        "3147": "Recuperação e Preservação de Bacias Hidrográficas",
        "00VA": "Apoio a Projetos de Desenvolvimento Sustentável Local Integrado",
    }

    def formatar_acao(codigo):
        nome = nomes_acoes.get(codigo)
        return f"{codigo} — {nome}" if nome else codigo

    acoes_por_programa = acoes_df.groupby("PROGRAMA")["ACAO"].apply(
        lambda s: sorted([formatar_acao(a) for a in s])
    ).to_dict()
    todas_acoes = sorted([formatar_acao(a) for a in acoes_df["ACAO"].unique().tolist()])

    # Mapeia label formatado de volta ao código original para uso nos filtros WHERE
    label_para_codigo_acao = {formatar_acao(a): a for a in acoes_df["ACAO"].unique().tolist()}

    programas = programas_raw
    return (
        acoes_por_programa,
        ano_max,
        ano_min,
        con,
        folium,
        instrumentos_ativos,
        json,
        label_para_codigo_acao,
        mo,
        opcoes_rotas,
        origens_recurso_labels,
        pd,
        programas,
        px,
        rotas,
        situacoes_convenio,
        tipologias,
        todas_acoes,
    )


@app.cell
def _(
    ano_max,
    ano_min,
    con,
    mo,
    opcoes_rotas,
    origens_recurso_labels,
    programas,
    rotas,
    situacoes_convenio,
    tipologias,
):
    slicer_anos = mo.ui.range_slider(
        start=ano_min,
        stop=ano_max,
        step=1,
        value=(ano_min, ano_max)
    )
    seletor_metrica = mo.ui.dropdown(
        options={
            "Valor Executado": "VALOR_AGREGADO",
            "Valor a executar": "VALOR_A_EXECUTAR",
            "Execução per capita": "execucao_per_capita",
            "Quantidade": "QTD_AGREGADA",
            "KMs Estimados": "KM_estimado",
            "População Beneficiária": "populacao",
            "Quantidade de Municípios": "qtde_municipios",
            "Número de Convênios": "nr_convenios"
        },
        value="Valor Executado"
    )

    regioes = sorted(con.execute("SELECT DISTINCT nome_regiao FROM municipios WHERE nome_regiao IS NOT NULL").df()["nome_regiao"].tolist())
    filtro_regiao = mo.ui.multiselect(options=regioes)

    flags = ["amazonia_legal", "SUDECO", "SUDENE", "semiarido", "faixa_fronteira", "matopiba", "cidades_intermediadoras", "amazonia_azul"]
    # titulos movidos para o layout
    filtro_flags = mo.ui.dictionary({
        f: mo.ui.dropdown(options=["Todos", "Sim", "Não"], value="Todos") 
        for f in flags
    })

    filtro_tipologia = mo.ui.multiselect(options=tipologias)

    filtros_rotas = mo.ui.dictionary({
        r: mo.ui.multiselect(options=opcoes_rotas[r]) 
        for r in rotas
    })

    # Novos filtros — todas as opções selecionadas por padrão (lista vazia = sem filtro ativo)
    filtro_sit_convenio = mo.ui.multiselect(
        options=situacoes_convenio,
        value=situacoes_convenio  # seleciona tudo por padrão
    )

    filtro_programa = mo.ui.multiselect(
        options=programas,
    )

    filtro_origem_recurso = mo.ui.multiselect(
        options=origens_recurso_labels,
    )
    return (
        filtro_flags,
        filtro_origem_recurso,
        filtro_programa,
        filtro_regiao,
        filtro_sit_convenio,
        filtro_tipologia,
        filtros_rotas,
        seletor_metrica,
        slicer_anos,
    )


@app.cell
def _(acoes_por_programa, filtro_programa, mo, todas_acoes):
    # Ações disponíveis dependem dos programas selecionados (hierarquia)
    if filtro_programa.value:
        _acoes_disponiveis = sorted(set(
            acao
            for prog in filtro_programa.value
            for acao in acoes_por_programa.get(prog, [])
        ))
    else:
        _acoes_disponiveis = todas_acoes

    filtro_acao = mo.ui.multiselect(options=_acoes_disponiveis)
    return (filtro_acao,)


@app.cell
def _(instrumentos_ativos, mo, seletor_metrica):
    _opcoes = ["SIM"] if seletor_metrica.value == "VALOR_A_EXECUTAR" else instrumentos_ativos
    _valor = ["SIM"] if seletor_metrica.value == "VALOR_A_EXECUTAR" else instrumentos_ativos
    filtro_instrumento = mo.ui.multiselect(
        options=_opcoes,
        value=_valor
    )
    return (filtro_instrumento,)


@app.cell
def _(con, filtro_regiao, mo):
    # Condição hierárquica para UFs baseada na região selecionada
    if filtro_regiao.value:
        _reg_list = ", ".join([f"'{r}'" for r in filtro_regiao.value])
        _q_uf = f"SELECT DISTINCT sigla_uf FROM municipios WHERE nome_regiao IN ({_reg_list}) AND sigla_uf IS NOT NULL"
    else:
        _q_uf = "SELECT DISTINCT sigla_uf FROM municipios WHERE sigla_uf IS NOT NULL"

    _ufs_list = sorted(con.execute(_q_uf).df()["sigla_uf"].tolist())
    filtro_uf = mo.ui.multiselect(options=_ufs_list)
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
    filtro_municipio = mo.ui.multiselect(options=_municipios_list)
    return (filtro_municipio,)


@app.cell
def _(
    filtro_acao,
    filtro_municipio,
    filtro_programa,
    filtro_regiao,
    filtro_uf,
    mo,
    seletor_metrica,
    slicer_anos,
):
    advanced_filters = mo.hstack(
        [
            mo.vstack([mo.md("**Região**"), filtro_regiao], align="start"),
            mo.vstack([mo.md("**Estado**"), filtro_uf], align="start"),
            mo.vstack([mo.md("**Município**"), filtro_municipio], align="start"),
            mo.vstack([mo.md("**Programa**"), filtro_programa], align="start"),
            mo.vstack([mo.md("**Ação**"), filtro_acao], align="start"),
        ], justify="start", align="start"
    )

    filtros = mo.vstack([
        mo.hstack([
            mo.vstack([mo.md("**Período (Anos)**"), slicer_anos], align="start"),
            mo.vstack([mo.md("**Métrica**"), seletor_metrica], align="start")
        ], justify="start", align="start"),
        advanced_filters
    ], align="start")

    layout = mo.Html(f"""
    <!-- Importando as fontes Padrão GovBR -->
    <link href="https://fonts.cdnfonts.com/css/rawline" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css" />

    <style>
        /* Sobrescrevendo variáveis nativas do Marimo para aplicar o Padrão GovBR */
        :root {{
            --base-1: #ffffff !important;
            --blue-11: #1351b4 !important;
            --sky-11: #1351b4 !important;
            --primary: #1351b4 !important;
            --marimo-font-family: 'Rawline', 'Raleway', 'Inter', sans-serif !important;
        }}

        /* Remove o selo "made with marimo" fixo no canto — atrapalha tabelas largas */
        [data-testid="watermark"] {{
            display: none !important;
        }}

        body, html, marimo-app, marimo-island, main {{
            font-family: 'Rawline', 'Raleway', sans-serif !important;
            background-color: #f2f5fd !important; /* Cor de fundo suave govbr */
        }}

        /* Trava o scroll global só na tela; na impressão precisa fluir para várias páginas */
        @media screen {{
            body, html, marimo-app, marimo-island, main {{
                overflow: hidden !important;
                max-height: 100vh !important;
            }}
        }}

        /* Novo header estático nativo */
        .govbr-header {{
            background-color: white !important;
            border-bottom: 2px solid #1351b4 !important;
            width: 100% !important;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1) !important;
            z-index: 99 !important;
        }}

        /* Faixa Cívica Brasileira no Topo */
        .govbr-faixa-top {{
            background-color: #0c326f;
            color: #ffffff;
            padding: 4px 30px;
            font-size: 0.85rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .govbr-faixa-top a {{
            color: white;
            text-decoration: none;
            font-weight: 700;
        }}

        .govbr-main-header {{
            padding: 15px 30px 15px 30px;
            text-align: left !important;
        }}

        .govbr-orgao {{
            color: #555555;
            font-size: 1.1rem;
            margin-bottom: 5px;
            display: block;
        }}

        .govbr-title {{
            color: #1351b4;
            font-size: 2.2rem;
            font-weight: bold;
            margin-top: 0px;
            margin-bottom: 1.5rem;
        }}

        /* O Sidebar Marimo recupera sua borda nativa sem interferir */
        .app-sidebar, marimo-sidebar, aside, [data-testid="sidebar"], .sidebar {{
            background-color: #ffffff !important;
            border-right: 1px solid #e0e0e0 !important;
        }}

        /* Estetização Padrão de Seções do GovBR na barra lateral */
        .govbr-sidebar-title {{
            color: #0c326f;
            font-weight: bold;
            font-size: 1.1rem;
            border-bottom: 2px solid #1351b4;
            padding-bottom: 5px;
            margin-bottom: 15px;
            font-family: 'Rawline', 'Raleway', sans-serif;
        }}

        /* Ajuste do Sidebar: Alinhando os rótulos à esquerda e os botões à direita */
        .app-sidebar label, marimo-sidebar label, .sidebar label, aside label, [data-testid="sidebar"] label {{
            display: flex !important;
            justify-content: space-between !important;
            align-items: center !important;
            width: 100% !important;
            margin-bottom: 8px !important;
        }}

        /* Controla a largura do campo do botão para não empurrar ou esmagar o texto */
        .app-sidebar select, marimo-sidebar select, .sidebar select, aside select, [data-testid="sidebar"] select,
        .app-sidebar input, marimo-sidebar input, .sidebar input, aside input, [data-testid="sidebar"] input {{
            max-width: 55% !important;
            margin-left: 10px !important;
            text-align: left !important;
        }}

        @media (prefers-color-scheme: dark) {{
            body, html, marimo-app, marimo-island, main {{
                background-color: #020617 !important;
                color: #f8fafc !important;
            }}
            .govbr-header {{
                background-color: #0f172a !important;
                border-bottom: 2px solid #1e293b !important;
            }}
            .govbr-orgao {{
                color: #e2e8f0 !important;
            }}
            .govbr-title {{
                color: #60a5fa !important;
            }}
            /* Cores dos labels de filtros no header */
            .govbr-main-header strong, .govbr-main-header p {{
                color: #f8fafc !important;
            }}
            .app-sidebar, marimo-sidebar, aside, [data-testid="sidebar"], .sidebar {{
                background-color: #0f172a !important;
                border-right: 1px solid #1e293b !important;
                color: #f8fafc !important;
            }}
            /* Força que todos os textos paralelos na barra lateral ganhem cor clara no modo noturno */
            .app-sidebar p, .app-sidebar strong, marimo-sidebar p, marimo-sidebar strong, aside p, aside strong, [data-testid="sidebar"] p, [data-testid="sidebar"] strong, .sidebar p, .sidebar strong {{
                color: #f8fafc !important;
            }}
            .govbr-sidebar-title {{
                color: #ffffff;
                border-bottom: 2px solid #334155;
            }}

            /* Estilização para Alertas e Notas em Modo Escuro */
            .govbr-alert {{
                background-color: #1e293b !important;
                color: #cbd5e1 !important;
                border-left: 4px solid #3b82f6 !important;
            }}
            .govbr-alert strong {{
                color: #f8fafc !important;
            }}
            .govbr-note {{
                background-color: #1e293b !important;
                color: #cbd5e1 !important;
                border-left: 4px solid #3b82f6 !important;
            }}
            .govbr-note strong {{
                color: #f8fafc !important;
            }}

            /* Ajuste de Tabelas em Modo Escuro */
            .govbr-table-container th.row_heading {{
                background-color: #1e293b !important;
                color: #f8fafc !important;
                border-right: 1px solid #334155 !important;
            }}
            .govbr-table-container td {{
                border-bottom: 1px solid #334155 !important;
                color: #cbd5e1 !important; /* Texto das células em modo escuro */
            }}
            .govbr-table-container {{
                background-color: #0f172a !important; /* Fundo escuro para a área da tabela */
                padding: 15px;
                border-radius: 8px;
            }}
            .govbr-table-container table {{
                background-color: #0f172a !important;
                color: #cbd5e1 !important;
            }}
            .govbr-table-container tr:last-child {{
                background-color: #1e293b !important;
                color: #f8fafc !important;
                border-top: 2px solid #3b82f6 !important;
            }}
            .govbr-table-container tr:hover {{
                background-color: rgba(59, 130, 246, 0.15) !important;
            }}
        }}

        /* Classes base para Alertas e Notas (Modo Claro) */
        .govbr-alert {{
            color: #0c326f;
            background-color: #eef2f9;
            border-left: 4px solid #1351b4;
            padding: 10px 15px;
            margin-bottom: 20px;
            font-family: 'Rawline', sans-serif;
            font-size: 0.95rem;
            border-radius: 4px;
            display: inline-block;
        }}
        .govbr-note {{
            font-size: 0.85rem;
            color: #555;
            background-color: #f8f9fa;
            padding: 12px 15px;
            border-left: 4px solid #1351b4;
            margin-bottom: 2rem;
            border-radius: 4px;
        }}
        .govbr-note strong {{
            color: #0c326f;
        }}
    </style>

    <div class="govbr-header">
        <div class="govbr-faixa-top">
            <div>
                <a href="https://www.gov.br" target="_blank">gov.br</a>
            </div>
        </div>
        <div class="govbr-main-header">
            <span class="govbr-orgao">Ministério da Integração e do Desenvolvimento Regional (MIDR/SDR)</span>
            <h1 class="govbr-title">Painel de Entregas da SDR</h1>
            {filtros}
        </div>
    </div>
    """)

    # A última expressão do bloco é exibida na tela do dashboard.
    layout
    return


@app.cell
def _(
    filtro_flags,
    filtro_instrumento,
    filtro_origem_recurso,
    filtro_sit_convenio,
    filtro_tipologia,
    filtros_rotas,
    mo,
):
    titulos_flags = {
        "amazonia_legal": "SUDAM", "SUDECO": "SUDECO",
        "SUDENE": "SUDENE", "semiarido": "Semiárido",
        "faixa_fronteira": "Faixa de Fronteira", "matopiba": "MATOPIBA",
        "cidades_intermediadoras": "Cidades Intermediadoras", "amazonia_azul": "Amazônia Azul"
    }

    _flags_layout = [
        mo.hstack([mo.md(f"**{titulos_flags[f]}**"), filtro_flags[f]], justify="space-between", align="center")
        for f in filtro_flags
    ]

    _rotas_layout = [
        mo.hstack([mo.md(f"**{r.replace('R_', 'Rota ').replace('_', ' ').title()}**"), filtros_rotas[r]], justify="space-between", align="center")
        for r in filtros_rotas
    ]

    sidebar_content = mo.vstack([
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-file-contract'></i> SITUAÇÃO DO CONVÊNIO</div>"),
        mo.hstack([mo.md("**Situação**"), filtro_sit_convenio], justify="space-between", align="center"),
        mo.Html("<div style='height: 20px;'></div>"),
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-handshake'></i> INSTRUMENTO</div>"),
        mo.hstack([mo.md("**Instrumento Ativo**"), filtro_instrumento], justify="space-between", align="center"),
        mo.Html("<div style='height: 20px;'></div>"),
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-coins'></i> ORIGEM DOS RECURSOS</div>"),
        mo.hstack([mo.md("**Origem**"), filtro_origem_recurso], justify="space-between", align="center"),
        mo.Html("<div style='height: 20px;'></div>"),
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-globe'></i> ABRANGÊNCIA</div>"),
        *_flags_layout,
        mo.Html("<div style='height: 20px;'></div>"),
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-chart-bar'></i> TIPOLOGIA</div>"),
        mo.hstack([mo.md("**Tipologia PNDR 3**"), filtro_tipologia], justify="space-between", align="center"),
        mo.Html("<div style='height: 20px;'></div>"),
        mo.Html("<div class='govbr-sidebar-title'><i class='fas fa-road'></i> ROTAS DE INTEGRAÇÃO</div>"),
        *_rotas_layout
    ])

    sidebar_element = mo.sidebar(sidebar_content)
    sidebar_element
    return


@app.cell
def _(
    con,
    filtro_acao,
    filtro_flags,
    filtro_instrumento,
    filtro_municipio,
    filtro_origem_recurso,
    filtro_programa,
    filtro_regiao,
    filtro_sit_convenio,
    filtro_tipologia,
    filtro_uf,
    filtros_rotas,
    folium,
    json,
    label_para_codigo_acao,
    mo,
    pd,
    px,
    seletor_metrica,
    slicer_anos,
):
    import io
    from datetime import datetime
    ano_inicio, ano_fim = slicer_anos.value

    def format_in(vals):
        if not vals: return ""
        items = ", ".join([f"'{v}'" for v in vals])
        return f"({items})"

    ano_col = "ANO_Convenio" if seletor_metrica.value == "VALOR_A_EXECUTAR" else "ANO_pgto"
    condicoes = [f"s.{ano_col} BETWEEN {ano_inicio} AND {ano_fim}"]

    # Filtro de Situação do Convênio — só filtra se não estiver com tudo selecionado
    if filtro_sit_convenio.value:
        condicoes.append(f"s.SIT_CONVENIO IN {format_in(filtro_sit_convenio.value)}")

    # Filtro de Instrumento Ativo
    if filtro_instrumento.value and seletor_metrica.value != "VALOR_A_EXECUTAR":
        condicoes.append(f"s.INSTRUMENTO_ATIVO IN {format_in(filtro_instrumento.value)}")

    # Filtro de Origem de Recurso
    if filtro_origem_recurso.value:
        # filtro_origem_recurso.value retorna os valores (códigos originais) do dicionário
        condicoes.append(f"s.ORIGEM_RECURSO IN {format_in(filtro_origem_recurso.value)}")

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

    # Filtros de Programa e Ação — aplicados para todas as métricas
    if filtro_programa.value:
        condicoes.append(f"s.PROGRAMA IN {format_in(filtro_programa.value)}")
    if filtro_acao.value:
        # Converte labels formatados ("1211 — Calha Norte") de volta para códigos ("1211")
        codigos_acao = [label_para_codigo_acao.get(label, label) for label in filtro_acao.value]
        condicoes.append(f"s.ACAO IN {format_in(codigos_acao)}")

    where_clause = " AND ".join(condicoes)

    if seletor_metrica.value == "VALOR_A_EXECUTAR":
        query_sdr = f"""
            SELECT s.ANO_Convenio AS "ANO Convenio", s.VALOR_A_EXECUTAR, m.Tipologia_PNDR_3, m.nome_regiao, m.sigla_uf AS UF, m.nome AS Municipio,
                   s.data_carga, s.NR_CONVENIO, s.SIT_CONVENIO, s.COD_MUNIC_IBGE, s.MUNIC_PROPONENTE, s.UF_PROPONENTE, s.MAX_VL_GLOBAL_CONV, s.SOMA_VALOR_AGREGADO, s.PERC_EXECUCAO,
                   s.PROGRAMA, s.ACAO
            FROM a_executar s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    elif seletor_metrica.value == "populacao":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.COD_MUNIC_IBGE, m."População 2022" AS populacao, m.Tipologia_PNDR_3, m.sigla_uf AS UF, m.nome AS Municipio
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()
        df_filtrado_sdr = df_filtrado_sdr.drop_duplicates(subset=['Divisao', 'CATEGORIA_SUGERIDA', 'ANO_pgto', 'COD_MUNIC_IBGE'])

    elif seletor_metrica.value == "qtde_municipios":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.COD_MUNIC_IBGE, m.Tipologia_PNDR_3, m.sigla_uf AS UF, m.nome AS Municipio
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    elif seletor_metrica.value == "nr_convenios":
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.NR_CONVENIO, s.COD_MUNIC_IBGE, m.Tipologia_PNDR_3, m.sigla_uf AS UF, m.nome AS Municipio
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    elif seletor_metrica.value == "execucao_per_capita":
        query_sdr = f"""
            SELECT s.ANO_pgto, s.VALOR_AGREGADO, s.COD_MUNIC_IBGE, m."População 2022" AS populacao, m.Tipologia_PNDR_3, m.sigla_uf AS UF, m.nome_regiao, m.nome AS Municipio
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    else:
        query_sdr = f"""
            SELECT s.Divisao, s.CATEGORIA_SUGERIDA, s.ANO_pgto, s.{seletor_metrica.value}, s.COD_MUNIC_IBGE, m.Tipologia_PNDR_3, m.sigla_uf AS UF, m.nome AS Municipio
            FROM sdr_agregado s
            LEFT JOIN municipios m ON s.COD_MUNIC_IBGE = m.COD_MUNIC_IBGE
            WHERE {where_clause}
        """
        df_filtrado_sdr = con.execute(query_sdr).df()

    if df_filtrado_sdr.empty:
        dash_content = mo.md("⚠️ Nenhum dado encontrado para os filtros selecionados.")
    elif seletor_metrica.value == "execucao_per_capita":
        import io
        from datetime import datetime

        try:
            val_carga_raw = con.execute("SELECT data_carga FROM sdr_agregado LIMIT 1").fetchone()[0]
            if val_carga_raw:
                val_str = str(val_carga_raw).strip()
                data_limpa = val_str
                if len(val_str) >= 10 and "-" in val_str:
                    try:
                        data_limpa = datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
                    except ValueError:
                        try:
                            data_limpa = datetime.strptime(val_str, "%Y-%m-%d").strftime("%d/%m/%Y")
                        except ValueError:
                            pass

                texto_data_carga = f"Data de carga dos dados: {data_limpa}"
                texto_fonte = "Fonte dos dados: Transferegov.br (Convênios, Contratos de Repasse e Termos de Fomento)"

                alerta_carga = mo.Html(
                    f"<div class='govbr-alert'>"
                    f"<div><i class='far fa-calendar-alt'></i> <strong>{texto_data_carga}</strong></div>"
                    f"<div style='margin-top: 5px;'><i class='fas fa-database'></i> <strong>{texto_fonte}</strong></div>"
                    f"</div>"
                )
            else:
                alerta_carga = mo.md("")
        except Exception as e:
            alerta_carga = mo.md(f"*(Aviso: Não foi possível carregar a data de carga - {str(e)})*")

        # Calculo do indicador
        df_mun = df_filtrado_sdr.groupby(['COD_MUNIC_IBGE', 'Municipio', 'UF', 'populacao', 'nome_regiao'])['VALOR_AGREGADO'].sum().reset_index()
        df_mun['execucao_per_capita'] = df_mun.apply(lambda row: row['VALOR_AGREGADO'] / row['populacao'] if row['populacao'] > 0 else 0, axis=1)

        with open("assets/municipios_br_simpl.geojson", 'r') as f:
            geojson_data = json.load(f)

        for feature in geojson_data['features']:
            if 'codarea' in feature['properties']:
                codarea_original = feature['properties']['codarea']
                feature['properties']['codarea'] = codarea_original[:-1] if codarea_original else ""

        map_data = df_mun[['COD_MUNIC_IBGE', 'execucao_per_capita', 'Municipio', 'UF']].copy()
        map_data['COD_MUNIC_IBGE'] = map_data['COD_MUNIC_IBGE'].astype(str).apply(lambda x: x[:6].zfill(6))

        if map_data.empty:
            fig_map = mo.md("Sem dados para exibir no mapa.")
        else:
            _m = folium.Map(location=[-14.2350, -51.9253], zoom_start=4.2, tiles="cartodbpositron", width='100%', height='600px', control_scale=True)
            _m.fit_bounds([[-33.75, -73.98], [5.27, -34.79]])

            # Cálculo de Quebra Natural Jenks (implementação pura, sem mapclassify)
            def _jenks_breaks(data, k=5):
                """Fisher-Jenks Natural Breaks em Python puro."""
                data = sorted(data)
                n = len(data)
                if n <= k:
                    return data
                # Matrizes de programação dinâmica
                lc = [[0] * (k + 1) for _ in range(n + 1)]
                vc = [[float('inf')] * (k + 1) for _ in range(n + 1)]
                for i in range(1, k + 1):
                    lc[1][i] = 1
                    vc[1][i] = 0.0
                for l in range(2, n + 1):
                    s1 = s2 = 0.0
                    for m in range(l, 0, -1):
                        val = data[m - 1]
                        s1 += val
                        s2 += val * val
                        w = l - m + 1
                        variance = s2 - (s1 * s1) / w
                        if m > 1:
                            for j in range(2, k + 1):
                                new_val = variance + vc[m - 1][j - 1]
                                if new_val < vc[l][j]:
                                    lc[l][j] = m
                                    vc[l][j] = new_val
                    lc[l][1] = 1
                    vc[l][1] = variance
                # Recuperar as quebras
                breaks = [data[-1]]
                kk = n
                for j in range(k, 1, -1):
                    kk = lc[kk][j] - 1
                    breaks.insert(0, data[kk])
                breaks.insert(0, data[0])
                return sorted(set(breaks))

            vals = map_data['execucao_per_capita'].dropna()
            if not vals.empty and vals.nunique() > 5:
                try:
                    bins_jenks = _jenks_breaks(vals.tolist(), k=5)
                except Exception:
                    bins_jenks = 5
            else:
                bins_jenks = 5

            choropleth = folium.Choropleth(
                geo_data=geojson_data,
                data=map_data,
                columns=["COD_MUNIC_IBGE", "execucao_per_capita"],
                key_on="feature.properties.codarea",
                fill_color="PuBuGn",
                fill_opacity=0.9,
                line_opacity=0.01,
                legend_name="Execução per capita (R$)",
                bins=bins_jenks,
                highlight=True,
                reset=True,
                smooth_factor=0.5,
                nan_fill_color="white",
                nan_fill_opacity=1.0
            ).add_to(_m)

            nome_dict = dict(zip(map_data['COD_MUNIC_IBGE'], map_data['Municipio']))
            val_dict = dict(zip(map_data['COD_MUNIC_IBGE'], map_data['execucao_per_capita']))

            for feature in choropleth.geojson.data['features']:
                codarea = feature['properties'].get('codarea', '')
                if codarea in nome_dict:
                    feature['properties']['nome_mun'] = nome_dict[codarea]
                    valor = val_dict[codarea]
                    feature['properties']['val_str'] = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                else:
                    feature['properties']['nome_mun'] = 'Sem dado'
                    feature['properties']['val_str'] = 'N/A'

            folium.GeoJsonTooltip(
                fields=['nome_mun', 'codarea', 'val_str'],
                aliases=['Município: ', 'Código IBGE: ', 'Per capita: '],
                style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px; border: 1px solid grey; border-radius: 5px;")
            ).add_to(choropleth.geojson)

            fix_size_js = "<script>setTimeout(function() { var mapDiv = document.querySelector('.folium-map'); if (mapDiv) { mapDiv.style.width = '100%'; mapDiv.style.height = '600px'; var mapObj = window[mapDiv.id]; if (mapObj) { mapObj.invalidateSize(); mapObj.setView([-14.2350, -51.9253], 4.2); } } }, 200);</script>"
            _m.get_root().html.add_child(folium.Element(fix_size_js))

            fig_map = mo.Html(f'<div style="width: 100%; height: 600px; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06);">{_m._repr_html_()}</div>')

        # Top 10 Municipios
        top10 = df_mun.nlargest(10, 'execucao_per_capita').sort_values('execucao_per_capita', ascending=True)
        top10['mun_uf'] = top10['Municipio'] + ' - ' + top10['UF']
        fig_rank = px.bar(top10, y='mun_uf', x='execucao_per_capita', orientation='h', color='execucao_per_capita', color_continuous_scale='PuBuGn', labels={'execucao_per_capita': 'Execução per capita (R$)', 'mun_uf': ''}, title='Top 10 Municípios por Execução per capita')
        fig_rank.update_traces(textfont_size=10, textangle=0, cliponaxis=False)
        fig_rank.update_layout(template='plotly_white', height=500, margin=dict(t=50, b=20), showlegend=False, coloraxis_showscale=False, yaxis=dict(categoryorder='total ascending'))

        # Region Chart
        df_mun_distinct = df_filtrado_sdr[['COD_MUNIC_IBGE', 'nome_regiao', 'UF', 'populacao']].drop_duplicates(subset=['COD_MUNIC_IBGE'])
        pop_regiao = df_mun_distinct.groupby('nome_regiao')['populacao'].sum().reset_index()
        val_regiao = df_filtrado_sdr.groupby('nome_regiao')['VALOR_AGREGADO'].sum().reset_index()
        df_regiao = pd.merge(val_regiao, pop_regiao, on='nome_regiao')
        df_regiao['execucao_per_capita'] = df_regiao.apply(lambda row: row['VALOR_AGREGADO'] / row['populacao'] if row['populacao'] > 0 else 0, axis=1)
        df_regiao = df_regiao.sort_values('execucao_per_capita', ascending=False)
        fig_regiao = px.bar(df_regiao, x='nome_regiao', y='execucao_per_capita', color='execucao_per_capita', color_continuous_scale='PuBuGn', labels={'execucao_per_capita': 'Execução per capita (R$)', 'nome_regiao': 'Região'}, title='Execução per capita por Região')
        fig_regiao.update_layout(template='plotly_white', height=400, margin=dict(t=50, b=20), showlegend=False, coloraxis_showscale=False)

        # State Chart
        pop_uf = df_mun_distinct.groupby('UF')['populacao'].sum().reset_index()
        val_uf = df_filtrado_sdr.groupby('UF')['VALOR_AGREGADO'].sum().reset_index()
        df_uf = pd.merge(val_uf, pop_uf, on='UF')
        df_uf['execucao_per_capita'] = df_uf.apply(lambda row: row['VALOR_AGREGADO'] / row['populacao'] if row['populacao'] > 0 else 0, axis=1)
        df_uf = df_uf.sort_values('execucao_per_capita', ascending=False)
        fig_uf = px.bar(df_uf, x='UF', y='execucao_per_capita', color='execucao_per_capita', color_continuous_scale='PuBuGn', labels={'execucao_per_capita': 'Execução per capita (R$)', 'UF': 'Estado'}, title='Execução per capita por Estado')
        fig_uf.update_layout(template='plotly_white', height=400, margin=dict(t=50, b=20), showlegend=False, coloraxis_showscale=False)

        dash_content = mo.vstack([
            alerta_carga,
            mo.Html('<div class="govbr-sidebar-title" style="margin-top: 20px;"><i class="fas fa-map-marked-alt"></i> Mapa Municipal</div>'),
            fig_map,
            mo.Html('<div style="height: 30px;"></div>'),
            mo.Html('<div class="govbr-sidebar-title"><i class="fas fa-chart-bar"></i> Análise Comparativa</div>'),
            mo.hstack(
                [
                    mo.Html(f"<div style='flex: 1 1 100%; min-width: 300px; max-width: 100vw;'>{mo.as_html(fig_rank).text}</div>"), 
                    mo.Html(f"<div style='flex: 1 1 100%; min-width: 300px; max-width: 100vw;'>{mo.as_html(mo.vstack([fig_regiao, mo.Html('<div style=\"height: 20px;\"></div>'), fig_uf])).text}</div>")
                ],
                wrap=True,
                justify="center"
            )
        ])

    else:

        def gerar_excel(df: pd.DataFrame) -> bytes:
            buffer = io.BytesIO()
            df.to_excel(buffer)
            return buffer.getvalue()
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

        if seletor_metrica.value == "VALOR_A_EXECUTAR":
            tabela_regiao = pd.pivot_table(
                data=df_filtrado_sdr,
                index=['nome_regiao'],
                columns=['ANO Convenio'],
                values=val_col,
                aggfunc=aggfunc,
                fill_value=0,
                margins=True,
                margins_name='Total Geral'
            )
            tabela_uf = pd.pivot_table(
                data=df_filtrado_sdr,
                index=['UF'],
                columns=['ANO Convenio'],
                values=val_col,
                aggfunc=aggfunc,
                fill_value=0,
                margins=True,
                margins_name='Total Geral'
            )
        else:
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

        col_ano = "ANO Convenio" if seletor_metrica.value == "VALOR_A_EXECUTAR" else "ANO_pgto"
        tabela_tipologia = pd.pivot_table(
            data=df_filtrado_sdr,
            index=['Tipologia_PNDR_3'],
            columns=[col_ano],
            values=val_col,
            aggfunc=aggfunc,
            fill_value=0,
            margins=True,
            margins_name='Total Geral'
        )

        val_col_mun = val_col
        if val_col == "COD_MUNIC_IBGE":
            df_filtrado_sdr['_COD_MUNIC_IBGE_VAL'] = df_filtrado_sdr['COD_MUNIC_IBGE']
            val_col_mun = '_COD_MUNIC_IBGE_VAL'

        tabela_municipio = pd.pivot_table(
            data=df_filtrado_sdr,
            index=['COD_MUNIC_IBGE', 'Municipio', 'UF'],
            columns=[col_ano],
            values=val_col_mun,
            aggfunc=aggfunc,
            fill_value=0,
            margins=True,
            margins_name='Total Geral'
        )

        colunas_completas = list(range(ano_inicio, ano_fim + 1)) + ['Total Geral']
        if seletor_metrica.value == "VALOR_A_EXECUTAR":
            tabela_regiao = tabela_regiao.reindex(columns=colunas_completas, fill_value=0)
            tabela_uf = tabela_uf.reindex(columns=colunas_completas, fill_value=0)
        else:
            tabela_dinamica = tabela_dinamica.reindex(columns=colunas_completas, fill_value=0)
            tabela_divisao = tabela_divisao.reindex(columns=colunas_completas, fill_value=0)
        tabela_tipologia = tabela_tipologia.reindex(columns=colunas_completas, fill_value=0)
        tabela_municipio = tabela_municipio.reindex(columns=colunas_completas, fill_value=0)

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

        if seletor_metrica.value in ["VALOR_AGREGADO", "VALOR_A_EXECUTAR"]:
            formatador = fmt_moeda
        elif seletor_metrica.value in ["QTD_AGREGADA", "populacao", "qtde_municipios", "nr_convenios"]:
            formatador = fmt_int
        else:
            formatador = fmt_float

        estilos_css = [
            {'selector': 'th', 'props': [('text-align', 'center'), ('font-weight', 'bold'), ('padding', '6px 10px'), ('background-color', '#0c326f'), ('color', '#ffffff'), ('font-family', "'Rawline', sans-serif"), ('border-right', '1px solid #1351b4'), ('border-bottom', '2px solid #1351b4'), ('font-size', '0.9rem')]},
            {'selector': 'th.row_heading', 'props': [('text-align', 'left'), ('background-color', '#f4f4f4'), ('color', '#333'), ('border-right', '1px solid #ddd')]},
            {'selector': 'tr:hover', 'props': [('background-color', 'rgba(19, 81, 180, 0.08)')]},
            {'selector': 'tr:last-child', 'props': [('font-weight', 'bold'), ('border-top', '2px solid #1351b4'), ('background-color', '#eef2f9')]}
        ]
        propriedades_css = {
            'text-align': 'right', 'padding': '4px 10px',
            'border-bottom': '1px solid #e0e0e0', 'white-space': 'nowrap',
            'font-family': "'Rawline', 'Raleway', sans-serif",
            'font-size': '0.9rem'
        }

        if seletor_metrica.value == "VALOR_A_EXECUTAR":
            estilo_tabela_regiao = (
                tabela_regiao.style
                .format(formatador)
                .set_properties(**propriedades_css)
                .set_table_styles(estilos_css)
            )

            estilo_tabela_uf = (
                tabela_uf.style
                .format(formatador)
                .set_properties(**propriedades_css)
                .set_table_styles(estilos_css)
            )
        else:
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

        df_municipio_ui = tabela_municipio.reset_index()
        format_map = {col: formatador for col in df_municipio_ui.columns if col not in ['COD_MUNIC_IBGE', 'Municipio', 'UF']}

        def clean_ibge(x):
            if pd.isna(x) or x == '': return ''
            if x == 'Total Geral': return x
            try: return str(int(float(x)))
            except: return str(x)

        df_municipio_ui['COD_MUNIC_IBGE'] = df_municipio_ui['COD_MUNIC_IBGE'].apply(clean_ibge)
        tabela_municipio_ui = mo.ui.table(df_municipio_ui, pagination=True, selection=None, format_mapping=format_map)

        nomes_metricas = {
            "VALOR_AGREGADO": "Valor Agregado",
            "VALOR_A_EXECUTAR": "Valor a executar",
            "QTD_AGREGADA": "Quantidade",
            "KM_estimado": "KMs Estimados",
            "populacao": "População Beneficiária",
            "qtde_municipios": "Quantidade de Municípios",
            "nr_convenios": "Número de Convênios"
        }
        titulo_metrica = nomes_metricas.get(seletor_metrica.value, "Métrica Selecionada")

        try:
            tabela_origem = "a_executar" if seletor_metrica.value == "VALOR_A_EXECUTAR" else "sdr_agregado"
            val_carga_raw = con.execute(f"SELECT data_carga FROM {tabela_origem} LIMIT 1").fetchone()[0]
            if val_carga_raw:
                val_str = str(val_carga_raw).strip()
                data_limpa = val_str  # ✅ fallback: garante que data_limpa sempre existe

                if len(val_str) >= 10 and "-" in val_str:
                    try:
                        data_limpa = datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
                    except ValueError:
                        try:
                            data_limpa = datetime.strptime(val_str, "%Y-%m-%d").strftime("%d/%m/%Y")
                        except ValueError:
                            data_limpa = val_str  # mantém o valor bruto se nenhum formato funcionar

                texto_data_carga = f"Data de carga dos dados: {data_limpa}"
                texto_fonte = "Fonte dos dados: Transferegov.br (Convênios, Contratos de Repasse e Termos de Fomento)"

                alerta_carga = mo.Html(
                    f"<div class='govbr-alert'>"
                    f"<div><i class='far fa-calendar-alt'></i> <strong>{texto_data_carga}</strong></div>"
                    f"<div style='margin-top: 5px;'><i class='fas fa-database'></i> <strong>{texto_fonte}</strong></div>"
                    f"</div>"
                )
            else:
                alerta_carga = mo.md("")
        except Exception as e:
            alerta_carga = mo.md(f"*(Aviso: Não foi possível carregar a data de carga - {str(e)})*")

        nota_html_base = """
        <div class="govbr-note">
            <strong>Nota{plural}:</strong>
            {content}
        </div>
        """

        if seletor_metrica.value == "QTD_AGREGADA":
            content = """
            <ul style="margin-top: 6px; margin-bottom: 0; padding-left: 20px;">
                <li><strong>Para Equipamentos:</strong> Somou-se o número físico de itens pagos</li>
                <li><strong>Para Obras, Capacitações e Projetos:</strong> Contou-se o número de Propostas/Contratos distintos por ano (uma vez que uma obra não se mede unitariamente pela soma de NFs, mas pelo seu contrato).</li>
                <li><strong>Para Outros:</strong> Não foi mensurado devido à heterogeneidade e granularidade dos produtos e serviços</li>
            </ul>
            """
            nota_dinamica = mo.Html(nota_html_base.format(plural="s", content=content))
        elif seletor_metrica.value == "populacao":
            content = """<div style="margin-top: 6px;">O total de cada categoria não é a soma dos anos. São somadas para cada categoria a população de municípios distintos que constaram no período selecionado.</div>"""
            nota_dinamica = mo.Html(nota_html_base.format(plural="", content=content))
        elif seletor_metrica.value == "qtde_municipios":
            content = """<div style="margin-top: 6px;">O total de cada categoria não é a soma dos anos. São somadas para cada categoria os municípios distintos que constaram no período selecionado.</div>"""
            nota_dinamica = mo.Html(nota_html_base.format(plural="", content=content))
        elif seletor_metrica.value == "nr_convenios":
            content = """<div style="margin-top: 6px;">O total de cada categoria não é a soma dos anos. São somadas para cada categoria os convênios distintos que constaram no período selecionado.</div>"""
            nota_dinamica = mo.Html(nota_html_base.format(plural="", content=content))
        else:
            nota_dinamica = mo.Html("")

        relatorio_metodologico_html = mo.Html(f"""
            <style>
                .relatorio-metodologico h1 {{ font-size: 1.1rem; margin-top: 15px; margin-bottom: 5px; color: #0c326f; }}
                .relatorio-metodologico h2 {{ font-size: 1rem; margin-top: 12px; margin-bottom: 4px; color: #1351b4; }}
                .relatorio-metodologico h3 {{ font-size: 0.95rem; margin-top: 10px; margin-bottom: 3px; }}
                .relatorio-metodologico p, .relatorio-metodologico li {{ font-size: 0.9rem; line-height: 1.4; margin-bottom: 4px; color: #444; }}
                .relatorio-metodologico ul, .relatorio-metodologico ol {{ margin-bottom: 8px; padding-left: 20px; margin-top: 0px; }}
                .relatorio-metodologico hr {{ margin: 15px 0; border: 0; border-top: 1px solid #ddd; }}

                @media (prefers-color-scheme: dark) {{
                    .relatorio-metodologico {{
                        background-color: #0f172a !important;
                        padding: 20px;
                        border-radius: 8px;
                        margin-top: 20px;
                    }}
                    .relatorio-metodologico h1 {{ color: #60a5fa !important; }}
                    .relatorio-metodologico h2 {{ color: #93c5fd !important; }}
                    .relatorio-metodologico h3 {{ color: #bfdbfe !important; }}
                    .relatorio-metodologico p, .relatorio-metodologico li {{ color: #cbd5e1 !important; }}
                    .relatorio-metodologico hr {{ border-top: 1px solid #334155; }}
                }}
            </style>
            <div class="relatorio-metodologico">{mo.md(r'''
    ---
    # Relatório Metodológico: Consolidação e Estimativa de Entregas da SDR (Siconv)

    ## 1. Introdução
    Este relatório descreve o fluxo metodológico do script de processamento de dados desenvolvido para extrair, categorizar e agregar dados brutos do Siconv. O pipeline gera tabelas consolidadas com foco nas entregas da Secretaria de Desenvolvimento Regional (SDR), com destaque para a nova etapa de higienização de métricas físicas e estimativa avançada de área pavimentada (em m²), posteriormente convertida para Km por meio da multiplicação por 6 mil.

    ## 2. Ingestão e Processamento Inicial dos Dados
    A ingestão utiliza uma consulta SQL otimizada com junções (JOINs) entre tabelas estruturais (`Proposta`, `Convênio`, `Pagamento`, `Itens_DL`, etc.) do Siconv.

    **Filtros de Escopo Aplicados na Origem:**
    * Restrição ao Órgão Superior `53000` (Ministério da Integração e do Desenvolvimento Regional).
    * Exclusão de projetos básicos rejeitados e convênios não assinados, rescindidos ou anulados.
    * Foco em UGs específicas da SDR (`530023`, `530020`, `530036`, `74019`).
    * Seleção estrita de itens de liquidação com valores válidos documentados.

    ## 3. Tratamento de Variáveis e Limpeza
    Os dados passaram por sanitização para viabilizar as regras de negócio:
    * Desmembramento da coluna `ACAO_ORCAMENTARIA` para isolar o código do Programa.
    * Remoção de caracteres de controle inválidos via Expressões Regulares (Regex).
    * Normalização de textos textuais (conversão para minúsculas e remoção de acentos) no objeto da proposta, nome do item e descrição do item do documento de liquidação.
    * Correção de tipagem e substituição de separadores decimais nas colunas financeiras.

    ## 4. Motor de Categorização (Regex e Regras de Negócio)
    A classificação dos itens baseia-se em um algoritmo de Regex hierárquico, contendo termos de inclusão e exclusão (para evitar falsos positivos):

    1. **Despesas Financeiras:** Isolamento de aditivos e rendimentos.
    2. **Projetos e Capacitação:** Separação de engenharia consultiva de obras físicas.
    3. **Obras e Infraestrutura:** Classificação detalhada (Pavimentação, Pontes, Barragens, Edificações, etc.).
    4. **Máquinas Pesadas e Caminhões:** Subclassificação avançada de frotas (Linha amarela, basculantes, compactadores, pipas).
    5. **Tratores e Implementos:** Diferenciação entre o maquinário trator e seus implementos secundários.
    6. **Políticas Regionais (Rotas):** Mapeamento transversal no objeto da proposta para cadeias produtivas (Cacau, Mel, Cordeiro, Açaí, TIC, etc.).

    *Nota de Correção:* Equipamentos com preenchimento inconsistente de quantidade no documento de liquidação (menor que 1 ou maior que 100) tiveram seu valor unitário forçado para `1` para evitar distorções de contagem.

    ## 5. Extração de Métricas Físicas Brutas (Obras em M²)
    Para recuperar as dimensões físicas das obras (m²), construiu-se uma CTE (Common Table Expression) específica:
    * O uso de funções de janela (`DENSE_RANK()`) garantiu a extração exclusiva da meta associada à **versão mais recente e validada** do Projeto Básico.
    * Aplicou-se a condição de existência (`EXISTS`) para garantir que apenas convênios com pagamentos reais transitassem para a base de métricas.
    * O valor da metragem extraída (`QUANTIDADE_M2`) foi isolado por convênio e filtrado apenas para obras medidas estritamente em "M2".

    ---

    ## 6. Identificação de Outliers e Higienização do Custo por M²
    Uma vez que os dados do Siconv dependem de preenchimento humano, a metragem informada (`MAX_QUANTIDADE_M2`) apresentou inconsistências graves. Para mitigar isso, implementou-se uma regra de detecção de *outliers* baseada no custo paramétrico da obra.

    1. **Cálculo do Custo Bruto:** O custo desembolsado por metro quadrado foi calculado pela razão entre o Valor Desembolsado do Convênio e a Metragem Máxima informada.
    2. **Corte por Limites de Domínio:** O método estatístico padrão de Intervalo Interquartil (IQR) foi descartado por gerar limite inferior negativo. Em seu lugar, adotou-se um corte baseado na realidade de mercado da construção civil brasileira (CBUQ, Paver, Tratamentos a Frio).
    3. **Limites Aplicados:** Valores de custo unitário abaixo de **R$ 10,00/m²** ou acima de **R$ 1.500,00/m²** foram classificados como anomalias.
    4. **Tratamento:** Registros fora dessa banda de plausibilidade tiveram sua quantidade original de M² anulada (`NaN`), preparando o terreno para a imputação algorítmica.

    ---

    ## 7. Modelagem e Estimativa Avançada de Área Pavimentada
    Para lidar com a granularidade dos pagamentos (diversas notas fiscais em anos diferentes para a mesma obra) e com os dados anulados no passo anterior, desenvolveu-se um modelo de estimativa de área executada.

    ### 7.1. Cálculo do Custo por M² Ponderado
    Para os convênios com metragem válida, o valor real do M² foi calculado levando em conta o grau de conclusão financeira da obra e o peso daquele pagamento específico no todo:

    $$
    Custo_{m^2} = \frac{Valor\ Agregado}{\left( \frac{Valor\ Agregado}{Soma\ Valor\ Agregado} \right) \times \left( \frac{Valor\ Desembolsado\ Conv}{Valor\ Repasse\ Conv} \right) \times Max\ Quantidade\ M^2}
    $$

    A partir dessa fórmula, extraiu-se a **mediana do custo por metro quadrado para cada ano de pagamento** (`MEDIANA_CUSTO_M2`), criando um referencial de mercado ajustado à inflação de cada período.

    ### 7.2. Imputação e Estimativa Final (`M2_estimado`) por ano
    A consolidação da área pavimentada por pagamento obedeceu a uma lógica condicional bipartida:

    * **Cenário A (Dados Válidos):** Se o convênio possui metragem confiável, a área executada na nota fiscal foi calculada rateando a metragem total pela fração financeira do pagamento:
      $M^2\_Estimado = \left( \frac{Valor\ Agregado}{Soma\ Valor\ Agregado} \right) \times Quantidade\ M^2$
    * **Cenário B (Dados Ausentes ou Outliers):** Se a metragem original foi reprovada nos limites de **R\$ 10 - R\$ 1.500**, a área foi matematicamente imputada dividindo o valor daquele pagamento específico pela mediana do custo do ano correspondente:

    $M^{2} {Estimado} = \frac{Valor\ Agregado}{Mediana\ Custo\ M^{2}}$

    ---

    ## 8. Limitações
    As classificações podem conter erros devido à falta de informação ou informação incerta ou ambígua no objeto da proposta, nome ou descrição dos itens no documento de liquidação, como Nota Fiscal. 
    ''').text}</div>""")

        if seletor_metrica.value == "VALOR_A_EXECUTAR":
            dash_content = mo.vstack([
                alerta_carga,
                mo.hstack([
                    mo.md(f"### Resumo por Região"),
                    mo.download(data=lambda: gerar_excel(tabela_regiao), filename="resumo_regiao.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_regiao.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Resumo por UF"),
                    mo.download(data=lambda: gerar_excel(tabela_uf), filename="resumo_uf.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_uf.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Resumo por Tipologia PNDR 3"),
                    mo.download(data=lambda: gerar_excel(tabela_tipologia), filename="resumo_tipologia.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_tipologia.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Resumo por Município"),
                    mo.download(data=lambda: gerar_excel(tabela_municipio), filename="resumo_municipio.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                tabela_municipio_ui,
                mo.Html("<div style='height: 2rem;'></div>"),

                mo.hstack([
                    mo.md(f"### Download Base Completa: A Executar"),
                    mo.download(data=lambda: gerar_excel(df_filtrado_sdr), filename="a_executar_completo.xlsx", label="💾 Baixar XLSX Completo")
                ], justify="space-between", align="center"),

                nota_dinamica,
                relatorio_metodologico_html
            ])
        else:
            dash_content = mo.vstack([
                alerta_carga,
                mo.hstack([
                    mo.md(f"### Evolução por {titulo_metrica} (Resumo por Divisão)"),
                    mo.download(data=lambda: gerar_excel(tabela_divisao), filename="resumo_divisao.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_divisao.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Detalhamento por Categoria"),
                    mo.download(data=lambda: gerar_excel(tabela_dinamica), filename="detalhe_categoria.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Resumo por Tipologia PNDR 3"),
                    mo.download(data=lambda: gerar_excel(tabela_tipologia), filename="resumo_tipologia.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                mo.Html(f"<div class='govbr-table-container' style='width: 100%; max-width: 100%; overflow-x: auto; margin-bottom: 2rem;'>{estilo_tabela_tipologia.to_html()}</div>"),

                mo.hstack([
                    mo.md(f"### Resumo por Município"),
                    mo.download(data=lambda: gerar_excel(tabela_municipio), filename="resumo_municipio.xlsx", label="💾 Baixar XLSX")
                ], justify="space-between", align="center"),
                tabela_municipio_ui,
                mo.Html("<div style='height: 2rem;'></div>"),

                nota_dinamica,

                relatorio_metodologico_html
            ])

    css_impressao = mo.Html("""
        <style>
        @media print {
            /* Forçar paisagem e tamanho A4 */
            @page {
                size: A4 landscape;
                margin: 10mm 8mm 10mm 8mm;
            }

            /* Oculta header, sidebar e botão */
            .govbr-header,
            .app-sidebar,
            marimo-sidebar,
            aside,
            [data-testid="sidebar"],
            .sidebar,
            .no-print {
                display: none !important;
            }

            /* Remove fundo e margens */
            body, html {
                background-color: white !important;
                margin: 0 !important;
                padding: 0 !important;
                -webkit-print-color-adjust: exact !important;
                print-color-adjust: exact !important;
            }

            /* Conteúdo ocupa largura total */
            main, marimo-app, marimo-island {
                width: 100% !important;
                margin: 0 !important;
                padding: 0 !important;
            }

            /* O marimo limita a saída de cada célula a 610px com scroll interno (.output-area);
               isso corta o conteúdo na impressão se não for removido. */
            .output-area {
                max-height: none !important;
                overflow: visible !important;
            }

            /* O marimo usa react-resizable-panels por baixo (divs com estilo inline
               "flex: 1 1 0px; overflow: hidden" e "height: 100%; overflow: hidden",
               sem classe/id estáveis pra mirar) que trava #App na altura da tela
               (confirmado via DOM: scrollHeight 6236 x clientHeight 945, cortando
               tudo que passa da tela). Como nenhuma dessas declarações usa !important,
               um overflow:visible universal (baixa especificidade, mas importance
               sempre vence sobre não-importance) desarma o corte em qualquer nível
               da árvore sem precisar saber o nome exato de cada elemento. */
            * {
                overflow: visible !important;
            }
            #App, [data-panel], [data-panel-group], html, body {
                height: auto !important;
                max-height: none !important;
            }

            /* .govbr-table-container tem overflow-x: auto para rolagem na tela;
               na impressão isso escondia as colunas fora da largura visível. */
            .govbr-table-container {
                overflow-x: visible !important;
                overflow: visible !important;
                max-width: none !important;
                width: auto !important;
            }

            /* mo.vstack/mo.hstack geram divs com "display: flex" inline; containers flex
               não se fragmentam entre páginas na impressão (limitação do Chrome), o que
               travava o dashboard inteiro em 1 página. Força fluxo em bloco só na impressão. */
            div[style*="display: flex"] {
                display: block !important;
            }

            /* Tabelas: permitir quebra entre páginas, não dentro de uma linha.
               table-layout: fixed força todas as colunas a caberem na largura da
               página (sem cortar anos); fonte bem pequena + nowrap evita amontoar. */
            table {
                page-break-inside: auto !important;
                width: 100% !important;
                table-layout: fixed !important;
                font-size: 4.5pt !important;
            }

            table td, table th {
                white-space: normal !important;
                word-break: break-word !important;
                padding: 1px 2px !important;
            }

            /* Coluna de rótulos (categorias) precisa de mais espaço que as colunas
               numéricas; sem isso o table-layout:fixed divide tudo igualmente. */
            th.row_heading {
                width: 12% !important;
                white-space: normal !important;
            }

            tr {
                page-break-inside: avoid !important;
                page-break-after: auto !important;
            }

            thead {
                display: table-header-group !important;
            }

            /* Botões de download ficam ocultos */
            button {
                display: none !important;
            }

            /* Cabeçalho de impressão */
            .print-header {
                display: block !important;
                margin-bottom: 10px;
                font-family: 'Rawline', sans-serif;
                font-size: 9pt;
                border-bottom: 2px solid #1351b4;
                padding-bottom: 6px;
            }
        }

        /* Oculto na tela, visível só na impressão */
        .print-header {
            display: none;
        }
        </style>
    """)

    aviso_impressao = mo.Html(
        "<div class='print-header'>"
        "📌 Para uma impressão mais legível, recomendamos filtrar o período (Anos) "
        "para no máximo 6 anos antes de imprimir."
        "</div>"
    )

    dash_content_final = mo.vstack([css_impressao, aviso_impressao, dash_content])

    # A última expressão do bloco é exibida na tela do dashboard.
    dash_content_final
    return


if __name__ == "__main__":
    app.run()
