"""
dashboard_fondeo.py
-------------------
Dashboard Cuenta Digital Fondeo - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard_fondeo.py
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


# ── Helpers ──────────────────────────────────────────────────────────────────
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


def format_mes_label(periodo_mes: str) -> str:
    """Convierte 'YYYY-MM' a etiqueta legible en español."""
    anio, mes = periodo_mes.split("-")
    return f"{MESES_ES[int(mes)]} {anio}"


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


def construir_figura_movimientos_por_dia(df_mes: pd.DataFrame, periodo_mes: str) -> go.Figure:
    """Barras apiladas de cuentas con y sin movimiento por día."""
    if df_mes.empty:
        return construir_figura_vacia("No hay datos para el mes seleccionado")

    anio, mes = map(int, periodo_mes.split("-"))
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))

    pivot = df_mes.groupby(["dia", "estado_movimiento"]).size().unstack(fill_value=0)
    con_mov = [int(pivot.loc[d, "Con movimiento"]) if d in pivot.index and "Con movimiento" in pivot.columns else 0 for d in dias]
    sin_mov = [int(pivot.loc[d, "Sin movimiento"]) if d in pivot.index and "Sin movimiento" in pivot.columns else 0 for d in dias]
    totales = [a + b for a, b in zip(con_mov, sin_mov)]

    max_total = max(totales) if totales else 0
    separacion_total = max(1, int(max_total * 0.03))
    y_texto_total = [t + separacion_total for t in totales]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Sin movimiento",
        x=dias,
        y=sin_mov,
        marker_color=COLORES["amarillo_opt"],
        text=[f"{v:,}" if v > 0 else "" for v in sin_mov],
        textposition="inside",
        textfont=dict(size=10, color=COLORES["azul_experto"]),
        hovertemplate="Día %{x}<br>Sin movimiento: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Con movimiento",
        x=dias,
        y=con_mov,
        marker_color=COLORES["aqua_digital"],
        text=[f"{v:,}" if v > 0 else "" for v in con_mov],
        textposition="inside",
        textfont=dict(size=10, color=COLORES["blanco"]),
        hovertemplate="Día %{x}<br>Con movimiento: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=dias,
        y=y_texto_total,
        mode="text",
        text=[f"{v:,}" for v in totales],
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


def construir_figura_barras_categoria(
    serie: pd.Series,
    color: str,
    titulo_vacio: str,
    top_n: int | None = None,
) -> go.Figure:
    """Construye barra horizontal con conteo + porcentaje por categoría."""
    conteos = serie.fillna("Sin dato").astype(str).str.strip().replace("", "Sin dato").value_counts()
    if top_n:
        conteos = conteos.head(top_n)
    if conteos.empty:
        return construir_figura_vacia(titulo_vacio)

    total = int(serie.shape[0]) if len(serie) > 0 else int(conteos.sum())
    porcentajes = (conteos / max(total, 1)) * 100
    textos = [f"{int(v):,} ({p:.1f}%)" for v, p in zip(conteos.values.tolist(), porcentajes.values.tolist())]

    fig = go.Figure(data=[
        go.Bar(
            y=conteos.index.tolist()[::-1],
            x=conteos.values.tolist()[::-1],
            orientation="h",
            marker_color=color,
            text=textos[::-1],
            textposition="outside",
            hovertemplate="%{y}<br>%{x:,} cuentas<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=140, r=30),
        xaxis=dict(title="Cuentas"),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


# ── Preparación de columnas ──────────────────────────────────────────────────
df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
df["dia"] = df["fecha_apertura"].dt.day
df["periodo_mes"] = df["fecha_apertura"].dt.to_period("M").astype(str)
df["fecha_nac"] = pd.to_datetime(df["fecha_nac"], errors="coerce")
df["genero_normalizado"] = df["genero"].apply(normalizar_genero)
df["generacion"] = df["fecha_nac"].apply(clasificar_generacion)
df["cant_transacciones"] = pd.to_numeric(df["cant_transacciones"], errors="coerce").fillna(0)
df["estado_movimiento"] = df["cant_transacciones"].apply(lambda x: "Con movimiento" if x > 0 else "Sin movimiento")

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

        html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style, children=[
                html.P("Total cuentas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                html.P("Con movimiento", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-con-mov", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                html.P("Sin movimiento", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-sin-mov", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
            ]),
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Cuentas con movimiento vs sin movimiento (por día)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-movimientos-dia")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Género (barras)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-genero-barras")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Generaciones (barras)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-generaciones-barras")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Departamentos más comunes (barras)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-departamentos-barras")
        ]),
    ]
)


@app.callback(
    Output("titulo-dashboard", "children"),
    Output("kpi-total", "children"),
    Output("kpi-con-mov", "children"),
    Output("kpi-sin-mov", "children"),
    Output("grafico-movimientos-dia", "figure"),
    Output("grafico-genero-barras", "figure"),
    Output("grafico-generaciones-barras", "figure"),
    Output("grafico-departamentos-barras", "figure"),
    Input("selector-mes", "value"),
)
def actualizar_dashboard(periodo_mes):
    df_mes = df[df["periodo_mes"] == periodo_mes].copy()

    total_cuentas = len(df_mes)
    total_con_mov = int((df_mes["estado_movimiento"] == "Con movimiento").sum())
    total_sin_mov = int((df_mes["estado_movimiento"] == "Sin movimiento").sum())

    figura_mov_dia = construir_figura_movimientos_por_dia(df_mes, periodo_mes)
    figura_genero = construir_figura_barras_categoria(
        df_mes["genero_normalizado"],
        COLORES["aqua_digital"],
        "No hay datos de género para el mes seleccionado",
    )
    figura_generaciones = construir_figura_barras_categoria(
        df_mes["generacion"],
        COLORES["azul_financiero"],
        "No hay datos de generaciones para el mes seleccionado",
    )
    campo_residencia = "direccion_2" if "direccion_2" in df_mes.columns else "direccion_lvl_2"
    figura_departamentos = construir_figura_barras_categoria(
        df_mes[campo_residencia],
        COLORES["azul_experto"],
        "No hay datos de residencia para el mes seleccionado",
        top_n=10,
    )

    titulo = f"Cuenta Digital Fondeo — {format_mes_label(periodo_mes)}"

    return (
        titulo,
        f"{total_cuentas:,}",
        f"{total_con_mov:,}",
        f"{total_sin_mov:,}",
        figura_mov_dia,
        figura_genero,
        figura_generaciones,
        figura_departamentos,
    )


if __name__ == "__main__":
    print("Dashboard Fondeo corriendo en http://127.0.0.1:8051")
    app.run(debug=True, use_reloader=False, port=8051)
