"""
dashboard.py
------------
Dashboard Creación de Usuario SV
Ejecutar desde la raíz del proyecto:

    python productos/creacion_usuario_sv/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

import calendar
import pandas as pd
from dash import Dash, html, dcc, Input, Output, State
import plotly.graph_objects as go
from core.db import run_query_file
from core.colors import COLORES

# ── Carga de datos ───────────────────────────────────────────────────────────
print("Cargando datos de creación de usuario SV...")
df = run_query_file("productos/creacion_usuario_sv/2026-03/queries/conversion.sql")
print(f"  {len(df)} registros cargados")

# ── Helpers ──────────────────────────────────────────────────────────────────
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


def normalizar_genero(valor: str) -> str:
    if pd.isna(valor):
        return "Sin dato"
    genero = str(valor).strip().upper()
    if genero in {"F", "FEMENINO", "MUJER"}:
        return "Mujer"
    if genero in {"M", "MASCULINO", "H", "HOMBRE"}:
        return "Hombre"
    return "Sin dato"


def clasificar_generacion(fecha_nac) -> str:
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


def normalizar_departamento(valor) -> str:
    if pd.isna(valor):
        return "Sin dato"
    texto = str(valor).strip()
    return texto if texto else "Sin dato"


def construir_figura_vacia(mensaje: str) -> go.Figure:
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


def construir_figura_diaria(df_mes: pd.DataFrame, anio: int, mes: int) -> go.Figure:
    if df_mes.empty:
        return construir_figura_vacia("No hay creaciones para el período seleccionado")

    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))
    conteo = df_mes.groupby("dia").size()
    valores = [int(conteo.get(d, 0)) for d in dias]
    textos = [f"{v:,}" if v > 0 else "" for v in valores]

    max_y = max(valores) if valores else 0
    fig = go.Figure(data=[
        go.Bar(
            x=dias,
            y=valores,
            marker_color=COLORES["aqua_digital"],
            text=textos,
            textposition="outside",
            hovertemplate="Día %{x}<br>Usuarios creados: %{y:,}<extra></extra>",
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1, range=[0.5, ultimo_dia + 0.5]),
        yaxis=dict(title="Usuarios", range=[0, max_y + max(1, int(max_y * 0.15))]),
        showlegend=False,
    )
    return fig


def construir_figura_genero(df_mes: pd.DataFrame) -> go.Figure:
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de género para el período seleccionado")

    orden = ["Mujer", "Hombre", "Sin dato"]
    conteos = df_mes["genero_normalizado"].value_counts().reindex(orden, fill_value=0)
    conteos = conteos[conteos > 0]
    if conteos.empty:
        return construir_figura_vacia("No hay datos de género para el período seleccionado")

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
            marker=dict(colors=[colores[c] for c in conteos.index.tolist()]),
            texttemplate="%{label}<br>%{value:,} (%{percent})",
            textposition="outside",
            hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=10, b=10, l=10, r=10),
        legend_title_text="",
    )
    return fig


def construir_figura_generaciones(df_mes: pd.DataFrame) -> go.Figure:
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de generaciones para el período seleccionado")

    orden = [
        "Generation X (1965-1980)",
        "Gen Y - Millennials (1981-1996)",
        "Generación Z (1997-2012)",
        "OTRA GENERACION",
    ]
    conteos = df_mes["generacion"].value_counts().reindex(orden, fill_value=0)
    conteos = conteos[conteos > 0]
    if conteos.empty:
        return construir_figura_vacia("No hay datos de generaciones para el período seleccionado")

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
            marker=dict(colors=[colores[c] for c in conteos.index.tolist()]),
            texttemplate="%{value:,} (%{percent})",
            textposition="outside",
            hovertemplate="%{label}<br>%{value:,} (%{percent})<extra></extra>",
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=10, b=10, l=10, r=10),
        legend_title_text="",
    )
    return fig


def construir_figura_departamentos(df_mes: pd.DataFrame) -> go.Figure:
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de ubicación para el período seleccionado")

    top5 = df_mes["departamento"].value_counts().head(5)
    if top5.empty:
        return construir_figura_vacia("No hay datos de ubicación para el período seleccionado")

    total = int(df_mes.shape[0])
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
            hovertemplate="%{y}<br>%{x:,} usuarios (%{customdata:.1f}%)<extra></extra>",
            customdata=porcentajes.values.tolist()[::-1],
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=140, r=25),
        xaxis=dict(title="Usuarios"),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


# ── Preparación de datos ─────────────────────────────────────────────────────
df["fecha_creacion_usuario"] = pd.to_datetime(df["fecha_creacion_usuario"], errors="coerce")
df["fecha_nacimiento_usuario"] = pd.to_datetime(df["fecha_nacimiento_usuario"], errors="coerce")

df = df[df["fecha_creacion_usuario"].notna()].copy()
df["anio"] = df["fecha_creacion_usuario"].dt.year
df["mes"] = df["fecha_creacion_usuario"].dt.month
df["dia"] = df["fecha_creacion_usuario"].dt.day
df["genero_normalizado"] = df["genero_cliente"].apply(normalizar_genero)
df["generacion"] = df["fecha_nacimiento_usuario"].apply(clasificar_generacion)
df["departamento"] = df["direccion_lvl_2"].apply(normalizar_departamento)

anios_disponibles = sorted(df["anio"].dropna().unique().astype(int).tolist()) if not df.empty else []
anio_default = anios_disponibles[-1] if anios_disponibles else None


def opciones_meses_por_anio(anio: int) -> list[dict]:
    if anio is None:
        return []
    meses = sorted(df.loc[df["anio"] == anio, "mes"].dropna().unique().astype(int).tolist())
    return [{"label": MESES_ES[m], "value": m} for m in meses]


meses_default_options = opciones_meses_por_anio(anio_default)
mes_default = meses_default_options[-1]["value"] if meses_default_options else None

# ── UI ───────────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius": "8px",
    "padding": "20px 24px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop": f"4px solid {COLORES['aqua_digital']}",
}

app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[
        html.H2(id="titulo-dashboard", style={"color": COLORES["azul_experto"], "marginBottom": "16px"}),

        html.Div(
            style={"display": "flex", "gap": "16px", "marginBottom": "20px", "maxWidth": "560px", "flexWrap": "wrap"},
            children=[
                html.Div(
                    style={"minWidth": "220px", "flex": "1"},
                    children=[
                        html.P("Año", style={"margin": "0 0 8px 0", "color": COLORES["gris_texto"], "fontSize": "14px"}),
                        dcc.Dropdown(
                            id="selector-anio",
                            options=[{"label": str(a), "value": int(a)} for a in anios_disponibles],
                            value=anio_default,
                            clearable=False,
                            style={"color": COLORES["azul_experto"]},
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "220px", "flex": "1"},
                    children=[
                        html.P("Mes", style={"margin": "0 0 8px 0", "color": COLORES["gris_texto"], "fontSize": "14px"}),
                        dcc.Dropdown(
                            id="selector-mes",
                            options=meses_default_options,
                            value=mes_default,
                            clearable=False,
                            style={"color": COLORES["azul_experto"]},
                        ),
                    ],
                ),
            ],
        ),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(180px, 1fr))", "gap": "16px", "marginBottom": "24px"},
            children=[
                html.Div(style=card_style, children=[
                    html.P("Usuarios creados", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.P("Mujeres", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-mujeres", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                    html.P("Hombres", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-hombres", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
                    html.P("Sin dato género", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-sin-dato", style={"margin": "8px 0 0 0", "color": COLORES["azul_financiero"]}),
                ]),
            ],
        ),

        html.Div(style={**card_style, "marginBottom": "20px"}, children=[
            html.H4("Creación diaria de usuario", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-diario"),
        ]),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))", "gap": "16px", "marginBottom": "20px"},
            children=[
                html.Div(style=card_style, children=[
                    html.H4("Distribución por género", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="grafico-genero"),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
                    html.H4("Generaciones", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="grafico-generaciones"),
                ]),
            ],
        ),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
            html.H4("Top 5 departamentos (direccion_lvl_2)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="grafico-departamentos"),
        ]),
    ],
)


@app.callback(
    Output("selector-mes", "options"),
    Output("selector-mes", "value"),
    Input("selector-anio", "value"),
    State("selector-mes", "value"),
)
def actualizar_meses(anio, mes_actual):
    opciones = opciones_meses_por_anio(anio)
    if not opciones:
        return [], None

    valores = [op["value"] for op in opciones]
    if mes_actual in valores:
        return opciones, mes_actual
    return opciones, valores[-1]


@app.callback(
    Output("titulo-dashboard", "children"),
    Output("kpi-total", "children"),
    Output("kpi-mujeres", "children"),
    Output("kpi-hombres", "children"),
    Output("kpi-sin-dato", "children"),
    Output("grafico-diario", "figure"),
    Output("grafico-genero", "figure"),
    Output("grafico-generaciones", "figure"),
    Output("grafico-departamentos", "figure"),
    Input("selector-anio", "value"),
    Input("selector-mes", "value"),
)
def actualizar_dashboard(anio, mes):
    if anio is None or mes is None:
        fig_vacia = construir_figura_vacia("No hay datos disponibles")
        return (
            "Creación de Usuario SV",
            "0",
            "0",
            "0",
            "0",
            fig_vacia,
            fig_vacia,
            fig_vacia,
            fig_vacia,
        )

    df_mes = df[(df["anio"] == anio) & (df["mes"] == mes)].copy()
    total = int(df_mes.shape[0])
    mujeres = int((df_mes["genero_normalizado"] == "Mujer").sum())
    hombres = int((df_mes["genero_normalizado"] == "Hombre").sum())
    sin_dato = int((df_mes["genero_normalizado"] == "Sin dato").sum())

    titulo = f"Creación de Usuario SV — {MESES_ES[int(mes)]} {int(anio)}"

    return (
        titulo,
        f"{total:,}",
        f"{mujeres:,}",
        f"{hombres:,}",
        f"{sin_dato:,}",
        construir_figura_diaria(df_mes, int(anio), int(mes)),
        construir_figura_genero(df_mes),
        construir_figura_generaciones(df_mes),
        construir_figura_departamentos(df_mes),
    )


if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8053")
    app.run(debug=True, use_reloader=False, port=8053)
