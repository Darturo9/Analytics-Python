"""
dashboard.py
------------
Dashboard Cuenta Digital - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from dash import Dash, html, dcc, Input, Output
import plotly.graph_objects as go
from core.db import run_query_file
from core.colors import COLORES

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")

# ── Clasificar nuevos vs existentes ──────────────────────────────────────────
df["tipo_cliente"] = df["dif"].apply(lambda x: "Nuevo" if x == 0 else "Existente")
df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
df["dia"] = df["fecha_apertura"].dt.day
df["periodo_mes"] = df["fecha_apertura"].dt.to_period("M").astype(str)

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

    pivot = df_mes.groupby(["dia", "tipo_cliente"]).size().unstack(fill_value=0)
    dias = sorted(pivot.index.tolist())

    existentes = [pivot.loc[d, "Existente"] if "Existente" in pivot.columns else 0 for d in dias]
    nuevos = [pivot.loc[d, "Nuevo"] if "Nuevo" in pivot.columns else 0 for d in dias]
    totales = [e + n for e, n in zip(existentes, nuevos)]
    return total_cuentas, total_nuevos, total_existentes, dias, existentes, nuevos, totales


def construir_figura(dias, existentes, nuevos, totales) -> go.Figure:
    """Construye la gráfica de barras apiladas con detalle por tipo y total."""
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
                    text="No hay datos para el mes seleccionado",
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
        xaxis=dict(title="", tickmode="linear", dtick=1),
        yaxis=dict(title="Cuentas", range=[0, max_total + (separacion_total * 3)]),
        legend_title_text="",
    )
    return fig


periodos_disponibles = sorted(df["periodo_mes"].unique().tolist())
periodo_default = periodos_disponibles[-1]
opciones_periodo = [{"label": format_mes_label(p), "value": p} for p in reversed(periodos_disponibles)]

# ── Estilos ───────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius":    "8px",
    "padding":         "24px 32px",
    "boxShadow":       "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop":       f"4px solid {COLORES['aqua_digital']}",
    "flex":            "1",
}

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[

        html.H2(id="titulo-dashboard",
                style={"color": COLORES["azul_experto"], "marginBottom": "24px"}),

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

        # KPIs
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

        # Gráfico
        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "flex": "none"}, children=[
            html.H4("Aperturas por día", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-aperturas")
        ]),
    ]
)


@app.callback(
    Output("titulo-dashboard", "children"),
    Output("kpi-total", "children"),
    Output("kpi-nuevos", "children"),
    Output("kpi-existentes", "children"),
    Output("grafico-aperturas", "figure"),
    Input("selector-mes", "value"),
)
def actualizar_dashboard(periodo_mes):
    total_cuentas, total_nuevos, total_existentes, dias, existentes, nuevos, totales = calcular_metricas(df, periodo_mes)
    figura = construir_figura(dias, existentes, nuevos, totales)
    titulo = f"Cuenta Digital — {format_mes_label(periodo_mes)}"
    return titulo, f"{total_cuentas:,}", f"{total_nuevos:,}", f"{total_existentes:,}", figura


if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8050")
    app.run(debug=True, use_reloader=False)
