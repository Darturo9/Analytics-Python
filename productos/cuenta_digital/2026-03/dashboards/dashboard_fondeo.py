"""
dashboard_fondeo.py
-------------------
Dashboard Cuenta Digital Fondeo (Saldos) - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard_fondeo.py
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from dash import Dash, html, dcc, Input, Output
import plotly.graph_objects as go
from core.db import run_query_file
from core.colors import COLORES

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos de cuentas digitales (fondeo por saldo)...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")


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


def construir_figura_vacia(mensaje: str) -> go.Figure:
    """Crea figura vacía con mensaje informativo."""
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


def construir_barras_categoria(serie: pd.Series, color: str, mensaje_vacio: str, top_n: int | None = None) -> go.Figure:
    """Construye barra horizontal con conteo y porcentaje."""
    conteos = serie.fillna("Sin dato").astype(str).str.strip().replace("", "Sin dato").value_counts()
    if top_n:
        conteos = conteos.head(top_n)
    if conteos.empty:
        return construir_figura_vacia(mensaje_vacio)

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


def construir_pastel_generaciones(serie_generacion: pd.Series) -> go.Figure:
    """Construye gráfico pastel de generaciones para cuentas con fondos."""
    orden = [
        "Generation X (1965-1980)",
        "Gen Y - Millennials (1981-1996)",
        "Generación Z (1997-2012)",
        "OTRA GENERACION",
    ]
    conteos = serie_generacion.fillna("OTRA GENERACION").value_counts().reindex(orden, fill_value=0)
    conteos = conteos[conteos > 0]
    if conteos.empty:
        return construir_figura_vacia("No hay datos de generaciones con fondos para el mes seleccionado")

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


def construir_top_cuentas_fondos(df_mes_fondos: pd.DataFrame) -> go.Figure:
    """Top 5 cuentas con mayor saldo en el mes seleccionado."""
    if df_mes_fondos.empty:
        return construir_figura_vacia("No hay cuentas con fondos para el mes seleccionado")

    top5 = df_mes_fondos.sort_values("saldo_cuenta", ascending=False).head(5).copy()
    etiquetas = [str(v) for v in top5["numero_cuenta"].fillna(top5["codigo_cliente"]).tolist()]
    valores = top5["saldo_cuenta"].tolist()
    textos = [f"L {v:,.2f}" for v in valores]

    fig = go.Figure(data=[
        go.Bar(
            y=etiquetas[::-1],
            x=valores[::-1],
            orientation="h",
            marker_color=COLORES["azul_experto"],
            text=textos[::-1],
            textposition="outside",
            hovertemplate="Cuenta %{y}<br>Saldo: L %{x:,.2f}<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=140, r=30),
        xaxis=dict(title="Saldo (L)"),
        yaxis=dict(title="Cuenta"),
        showlegend=False,
    )
    return fig


# ── Preparación de columnas ──────────────────────────────────────────────────
df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
df["periodo_mes"] = df["fecha_apertura"].dt.to_period("M").astype(str)
df["fecha_nac"] = pd.to_datetime(df["fecha_nac"], errors="coerce")
df["genero_normalizado"] = df["genero"].apply(normalizar_genero)
df["generacion"] = df["fecha_nac"].apply(clasificar_generacion)
df["saldo_cuenta"] = pd.to_numeric(df["saldo_cuenta"], errors="coerce").fillna(0)

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
opciones_periodo = [{"label": f"{MESES_ES[int(p.split('-')[1])]} {p.split('-')[0]}", "value": p} for p in reversed(periodos_disponibles)]

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
                html.P("Total cuentas creadas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                html.P("Cuentas con fondos", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-con-fondos", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                html.P("Total dinero fondeado", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(id="kpi-total-fondos", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
            ]),
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Top 5 cuentas con más fondos", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-top-cuentas")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Género (solo cuentas con fondos)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-genero")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Generaciones en pastel (solo cuentas con fondos)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-generaciones")
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}", "padding": "24px", "marginBottom": "24px", "flex": "none"}, children=[
            html.H4("Departamentos más comunes (solo cuentas con fondos)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-departamentos")
        ]),
    ]
)


@app.callback(
    Output("titulo-dashboard", "children"),
    Output("kpi-total", "children"),
    Output("kpi-con-fondos", "children"),
    Output("kpi-total-fondos", "children"),
    Output("grafico-top-cuentas", "figure"),
    Output("grafico-genero", "figure"),
    Output("grafico-generaciones", "figure"),
    Output("grafico-departamentos", "figure"),
    Input("selector-mes", "value"),
)
def actualizar_dashboard(periodo_mes):
    df_mes = df[df["periodo_mes"] == periodo_mes].copy()
    df_mes_fondos = df_mes[df_mes["saldo_cuenta"] > 0].copy()

    total_cuentas = len(df_mes)
    cuentas_con_fondos = len(df_mes_fondos)
    total_fondos = float(df_mes_fondos["saldo_cuenta"].sum())

    figura_top_cuentas = construir_top_cuentas_fondos(df_mes_fondos)
    figura_genero = construir_barras_categoria(
        df_mes_fondos["genero_normalizado"],
        COLORES["aqua_digital"],
        "No hay datos de género con fondos para el mes seleccionado",
    )
    figura_generaciones = construir_pastel_generaciones(df_mes_fondos["generacion"])
    campo_residencia = "direccion_2" if "direccion_2" in df_mes_fondos.columns else "direccion_lvl_2"
    figura_deptos = construir_barras_categoria(
        df_mes_fondos[campo_residencia],
        COLORES["azul_experto"],
        "No hay datos de residencia con fondos para el mes seleccionado",
        top_n=10,
    )

    anio, mes = periodo_mes.split("-")
    titulo = f"Cuenta Digital Fondeo (Saldos) — {MESES_ES[int(mes)]} {anio}"

    return (
        titulo,
        f"{total_cuentas:,}",
        f"{cuentas_con_fondos:,}",
        f"L {total_fondos:,.2f}",
        figura_top_cuentas,
        figura_genero,
        figura_generaciones,
        figura_deptos,
    )


if __name__ == "__main__":
    import socket

    port = 8052
    try:
        ip_red = socket.gethostbyname(socket.gethostname())
    except Exception:
        ip_red = "IP_NO_DISPONIBLE"

    print(f"Dashboard Fondeo (Saldos) corriendo en local: http://127.0.0.1:{port}")
    print(f"Dashboard Fondeo (Saldos) expuesto en red: http://{ip_red}:{port}")
    app.run(debug=False, use_reloader=False, host="0.0.0.0", port=port)
