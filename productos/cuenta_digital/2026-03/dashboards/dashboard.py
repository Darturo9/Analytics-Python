"""
dashboard.py
------------
Dashboard Cuenta Digital - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

import calendar
import pandas as pd
from dash import Dash, html, dcc, Input, Output
import plotly.graph_objects as go
from core.db import run_query_file
from core.colors import COLORES

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos de cuentas digitales...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")

print("Cargando clientes contactados por campaña...")
df_clientes_campania = run_query_file("productos/cuenta_digital/2026-03/campaing/CampañasIniciales.sql")
print(f"  {len(df_clientes_campania)} clientes de campaña cargados")


# ── Helpers de normalización ─────────────────────────────────────────────────
def normalizar_codigo_cliente(valor) -> str | None:
    """Normaliza un código cliente para comparación en formato de 8 dígitos."""
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_genero(valor: str) -> str:
    """Normaliza valores de género en categorías consistentes."""
    if pd.isna(valor):
        return "Sin dato"
    genero = str(valor).strip().upper()
    if genero in {"F", "FEMENINO", "MUJER"}:
        return "Mujer"
    if genero in {"M", "MASCULINO", "H", "HOMBRE"}:
        return "Hombre"
    return "Sin dato"


def clasificar_generacion(fecha_nac):
    """Clasifica generación a partir del año de nacimiento."""
    if pd.isna(fecha_nac):
        return "OTRA GENERACION"
    anio = int(fecha_nac.year)
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generación Z (1997-2012)"
    return "OTRA GENERACION"


# ── Preparación de columnas ──────────────────────────────────────────────────
df["tipo_cliente"] = df["dif"].apply(lambda x: "Nuevo" if x == 0 else "Existente")
df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
df["dia"] = df["fecha_apertura"].dt.day
df["periodo_mes"] = df["fecha_apertura"].dt.to_period("M").astype(str)
df["fecha_nac"] = pd.to_datetime(df["fecha_nac"], errors="coerce")
df["edad_apertura"] = ((df["fecha_apertura"] - df["fecha_nac"]).dt.days // 365).where(df["fecha_nac"].notna())
df["genero_normalizado"] = df["genero"].apply(normalizar_genero)
df["generacion"] = df["fecha_nac"].apply(clasificar_generacion)
df["padded_codigo_cliente_norm"] = df["padded_codigo_cliente"].apply(normalizar_codigo_cliente)

clientes_campania = set(
    df_clientes_campania["padded_codigo_cliente"].apply(normalizar_codigo_cliente).dropna().tolist()
)

df["es_campania"] = df["padded_codigo_cliente_norm"].isin(clientes_campania)
df_campania = df[df["es_campania"]].copy()

print(f"  {len(df_campania)} aperturas relacionadas a clientes de campaña")

MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def format_mes_label(periodo_mes: str) -> str:
    """Convierte 'YYYY-MM' a etiqueta legible en español."""
    anio, mes = periodo_mes.split("-")
    return f"{MESES_ES[int(mes)]} {anio}"


def calcular_metricas(dataframe: pd.DataFrame, periodo_mes: str):
    """Retorna métricas y series por día para el mes seleccionado."""
    df_mes = dataframe[dataframe["periodo_mes"] == periodo_mes].copy()
    if df_mes.empty:
        return 0, 0, 0, [], [], [], []

    total_cuentas = len(df_mes)
    total_nuevos = (df_mes["tipo_cliente"] == "Nuevo").sum()
    total_existentes = (df_mes["tipo_cliente"] == "Existente").sum()

    anio, mes = map(int, periodo_mes.split("-"))
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))

    pivot = df_mes.groupby(["dia", "tipo_cliente"]).size().unstack(fill_value=0)

    existentes = [
        int(pivot.loc[d, "Existente"]) if d in pivot.index and "Existente" in pivot.columns else 0
        for d in dias
    ]
    nuevos = [
        int(pivot.loc[d, "Nuevo"]) if d in pivot.index and "Nuevo" in pivot.columns else 0
        for d in dias
    ]
    totales = [e + n for e, n in zip(existentes, nuevos)]
    return total_cuentas, total_nuevos, total_existentes, dias, existentes, nuevos, totales


def construir_figura_aperturas(dias, existentes, nuevos, totales, titulo_vacio: str) -> go.Figure:
    """Construye gráfica de barras apiladas con detalle por tipo y total."""
    fig = go.Figure()

    if not dias:
        fig.update_layout(
            plot_bgcolor=COLORES["blanco"],
            paper_bgcolor=COLORES["blanco"],
            font=dict(color=COLORES["azul_experto"]),
            margin=dict(t=30, b=40, l=40, r=10),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text=titulo_vacio,
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=14, color=COLORES["gris_texto"]),
                )
            ],
        )
        return fig

    textos_existentes = [f"{v:,}" if v > 0 else "" for v in existentes]
    textos_nuevos = [f"{v:,}" if v > 0 else "" for v in nuevos]
    textos_totales = [f"{v:,}" for v in totales]

    max_total = max(totales) if totales else 0
    separacion_total = max(1, int(max_total * 0.03))
    y_texto_total = [t + separacion_total for t in totales]

    fig.add_trace(go.Bar(
        name="Existente",
        x=dias,
        y=existentes,
        marker_color=COLORES["amarillo_opt"],
        text=textos_existentes,
        textposition="inside",
        textfont=dict(size=10, color=COLORES["azul_experto"]),
        hovertemplate="Día %{x}<br>Existentes: %{y:,}<extra></extra>",
    ))

    fig.add_trace(go.Bar(
        name="Nuevo",
        x=dias,
        y=nuevos,
        marker_color=COLORES["aqua_digital"],
        text=textos_nuevos,
        textposition="inside",
        textfont=dict(size=10, color=COLORES["blanco"]),
        hovertemplate="Día %{x}<br>Nuevos: %{y:,}<extra></extra>",
    ))

    fig.add_trace(go.Scatter(
        x=dias,
        y=y_texto_total,
        mode="text",
        text=textos_totales,
        textposition="top center",
        textfont=dict(size=11, color=COLORES["azul_experto"]),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=30, b=40, l=40, r=10),
        xaxis=dict(title="", tickmode="linear", tick0=1, dtick=1, range=[0.5, dias[-1] + 0.5]),
        yaxis=dict(title="Cuentas", range=[0, max_total + (separacion_total * 3)]),
        legend_title_text="",
    )
    return fig


def construir_figura_vacia(mensaje: str) -> go.Figure:
    """Crea figura de fallback cuando no hay información para mostrar."""
    fig = go.Figure()
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=30, b=40, l=40, r=10),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[
            dict(
                text=mensaje,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color=COLORES["gris_texto"]),
            )
        ],
    )
    return fig


def construir_figura_genero(df_mes: pd.DataFrame) -> go.Figure:
    """Construye gráfico de distribución por género con conteo y porcentaje."""
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de género para el mes seleccionado")

    orden = ["Mujer", "Hombre", "Sin dato"]
    conteos = df_mes["genero_normalizado"].value_counts().reindex(orden, fill_value=0)
    conteos = conteos[conteos > 0]
    if conteos.empty:
        return construir_figura_vacia("No hay datos de género para el mes seleccionado")

    colores = {
        "Mujer": COLORES["aqua_digital"],
        "Hombre": COLORES["amarillo_opt"],
        "Sin dato": "#A0A0A0",
    }

    fig = go.Figure(data=[
        go.Pie(
            labels=conteos.index.tolist(),
            values=conteos.values.tolist(),
            hole=0.5,
            marker=dict(colors=[colores[label] for label in conteos.index.tolist()]),
            texttemplate="%{label}<br>%{value:,} (%{percent})",
            textposition="outside",
            hovertemplate="%{label}: %{value:,} cuentas (%{percent})<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=10, b=10, l=10, r=10),
        legend_title_text="",
        showlegend=True,
    )
    return fig


def construir_figura_generaciones_pastel(df_mes: pd.DataFrame) -> go.Figure:
    """Construye gráfico pastel por generación con conteo y porcentaje."""
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de generaciones para el mes seleccionado")

    orden = [
        "Generation X (1965-1980)",
        "Gen Y - Millennials (1981-1996)",
        "Generación Z (1997-2012)",
        "OTRA GENERACION",
    ]
    conteos = df_mes["generacion"].value_counts().reindex(orden, fill_value=0)
    conteos = conteos[conteos > 0]
    if conteos.empty:
        return construir_figura_vacia("No hay datos de generaciones para el mes seleccionado")

    colores = {
        "Generation X (1965-1980)": COLORES["azul_financiero"],
        "Gen Y - Millennials (1981-1996)": COLORES["aqua_digital"],
        "Generación Z (1997-2012)": COLORES["amarillo_opt"],
        "OTRA GENERACION": COLORES["azul_experto"],
    }

    fig = go.Figure(data=[
        go.Pie(
            labels=conteos.index.tolist(),
            values=conteos.values.tolist(),
            hole=0.45,
            marker=dict(colors=[colores[label] for label in conteos.index.tolist()]),
            texttemplate="%{value:,} (%{percent})",
            textposition="outside",
            hovertemplate="%{label}<br>%{value:,} cuentas (%{percent})<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=10, b=10, l=10, r=10),
        legend_title_text="",
        showlegend=True,
    )
    return fig


def construir_figura_departamentos(df_mes: pd.DataFrame) -> go.Figure:
    """Construye gráfico Top 5 departamentos con más aperturas."""
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de residencia para el mes seleccionado")

    campo_residencia = "direccion_2" if "direccion_2" in df_mes.columns else "direccion_lvl_2"
    departamentos = df_mes[campo_residencia].fillna("Sin dato").astype(str).str.strip()
    departamentos = departamentos.replace("", "Sin dato")
    top5 = departamentos.value_counts().head(5)
    if top5.empty:
        return construir_figura_vacia("No hay datos de residencia para el mes seleccionado")

    total = int(departamentos.shape[0])
    porcentajes = (top5 / total) * 100
    textos = [f"{int(v):,} ({p:.1f}%)" for v, p in zip(top5.values.tolist(), porcentajes.values.tolist())]

    fig = go.Figure(data=[
        go.Bar(
            y=top5.index.tolist()[::-1],
            x=top5.values.tolist()[::-1],
            orientation="h",
            marker_color=COLORES["azul_experto"],
            text=textos[::-1],
            textposition="outside",
            hovertemplate="%{y}<br>%{x:,} cuentas<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=120, r=30),
        xaxis=dict(title="Cuentas"),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


periodos_disponibles = sorted(df["periodo_mes"].unique().tolist())
periodo_default = periodos_disponibles[-1]
opciones_periodo = [{"label": format_mes_label(p), "value": p} for p in reversed(periodos_disponibles)]

# ── Estilos ───────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius": "8px",
    "padding": "24px 32px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop": f"4px solid {COLORES['aqua_digital']}",
    "flex": "1",
}

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[
        html.H2(id="titulo-dashboard", style={"color": COLORES["azul_experto"], "marginBottom": "24px"}),

        html.Div(style={"marginBottom": "20px", "maxWidth": "340px"}, children=[
            html.P("Mes de análisis", style={"margin": "0 0 8px 0", "color": COLORES["gris_texto"], "fontSize": "14px"}),
            dcc.Dropdown(
                id="selector-mes",
                options=opciones_periodo,
                value=periodo_default,
                clearable=False,
                style={"color": COLORES["azul_experto"]},
            ),
        ]),

        # KPIs generales
        html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style, children=[
                html.P("Total cuentas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                html.P("Clientes nuevos", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-nuevos", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                html.P("Clientes existentes", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-existentes", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
            ]),
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Aperturas por día (General)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-aperturas")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}", "padding": "20px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Género (General)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-genero")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "20px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Generaciones en pastel (General)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-generaciones-pastel")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Top 5 departamentos (General)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-departamentos")
        ]),

        # Sección campaña
        html.H3("Clientes contactados por campaña", style={"color": COLORES["azul_experto"], "marginTop": "8px", "marginBottom": "16px"}),
        html.P(
            f"Universo campaña por match de padded_codigo_cliente (clientes únicos cargados: {len(clientes_campania):,}).",
            style={"marginTop": 0, "marginBottom": "16px", "color": COLORES["gris_texto"]},
        ),

        html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style, children=[
                html.P("Total cuentas campaña", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-camp-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                html.P("Nuevos campaña", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-camp-nuevos", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                html.P("Existentes campaña", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-camp-existentes", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
            ]),
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Aperturas por día (Solo campaña)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-aperturas-campania")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}", "padding": "20px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Género (Solo campaña)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-genero-campania")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "20px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Generaciones en pastel (Solo campaña)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-generaciones-campania")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Top 5 departamentos (Solo campaña)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-departamentos-campania")
        ]),
    ]
)


@app.callback(
    Output("titulo-dashboard", "children"),
    Output("kpi-total", "children"),
    Output("kpi-nuevos", "children"),
    Output("kpi-existentes", "children"),
    Output("grafico-aperturas", "figure"),
    Output("grafico-genero", "figure"),
    Output("grafico-generaciones-pastel", "figure"),
    Output("grafico-departamentos", "figure"),
    Output("kpi-camp-total", "children"),
    Output("kpi-camp-nuevos", "children"),
    Output("kpi-camp-existentes", "children"),
    Output("grafico-aperturas-campania", "figure"),
    Output("grafico-genero-campania", "figure"),
    Output("grafico-generaciones-campania", "figure"),
    Output("grafico-departamentos-campania", "figure"),
    Input("selector-mes", "value"),
)
def actualizar_dashboard(periodo_mes):
    # General
    total_cuentas, total_nuevos, total_existentes, dias, existentes, nuevos, totales = calcular_metricas(df, periodo_mes)
    df_mes = df[df["periodo_mes"] == periodo_mes].copy()
    figura_aperturas = construir_figura_aperturas(dias, existentes, nuevos, totales, "No hay datos para el mes seleccionado")
    figura_genero = construir_figura_genero(df_mes)
    figura_generaciones_pastel = construir_figura_generaciones_pastel(df_mes)
    figura_departamentos = construir_figura_departamentos(df_mes)

    # Campaña
    camp_total, camp_nuevos, camp_existentes, camp_dias, camp_exis, camp_nuev, camp_tot = calcular_metricas(df_campania, periodo_mes)
    df_mes_campania = df_campania[df_campania["periodo_mes"] == periodo_mes].copy()
    figura_aperturas_camp = construir_figura_aperturas(
        camp_dias,
        camp_exis,
        camp_nuev,
        camp_tot,
        "No hay cuentas de campaña para el mes seleccionado",
    )
    figura_genero_camp = construir_figura_genero(df_mes_campania)
    figura_generaciones_camp = construir_figura_generaciones_pastel(df_mes_campania)
    figura_departamentos_camp = construir_figura_departamentos(df_mes_campania)

    titulo = f"Cuenta Digital — {format_mes_label(periodo_mes)}"

    return (
        titulo,
        f"{total_cuentas:,}",
        f"{total_nuevos:,}",
        f"{total_existentes:,}",
        figura_aperturas,
        figura_genero,
        figura_generaciones_pastel,
        figura_departamentos,
        f"{camp_total:,}",
        f"{camp_nuevos:,}",
        f"{camp_existentes:,}",
        figura_aperturas_camp,
        figura_genero_camp,
        figura_generaciones_camp,
        figura_departamentos_camp,
    )


if __name__ == "__main__":
    import socket

    port = 8050
    try:
        ip_red = socket.gethostbyname(socket.gethostname())
    except Exception:
        ip_red = "IP_NO_DISPONIBLE"

    print(f"Dashboard corriendo en local: http://127.0.0.1:{port}")
    print(f"Dashboard expuesto en red: http://{ip_red}:{port}")
    app.run(debug=False, use_reloader=False, host="0.0.0.0", port=port)
