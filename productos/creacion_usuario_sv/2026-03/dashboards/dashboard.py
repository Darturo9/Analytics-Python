"""
dashboard.py
------------
Dashboard Creación de Usuario SV
Ejecutar desde la raíz del proyecto:

    python3 productos/creacion_usuario_sv/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

import calendar
import os
import pandas as pd
import urllib
from dash import Dash, html, dcc, Input, Output, State
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from core.colors import COLORES

# ── Carga de datos ───────────────────────────────────────────────────────────
DB_NAME_DASHBOARD = "DWHSV"


def run_query_hsv(sql: str, params: dict = None) -> pd.DataFrame:
    """Ejecuta SQL forzando DB DWHSV para alinear con Tableau."""
    db_server = os.getenv("DB_SERVER")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{db_driver}}};"
        f"SERVER={db_server};"
        f"DATABASE={DB_NAME_DASHBOARD};"
        f"UID={db_user};"
        f"PWD={db_pass};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_query_file_hsv(path: str, params: dict = None) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return run_query_hsv(sql, params)


print(f"Cargando datos de creación de usuario SV (conversión) en {DB_NAME_DASHBOARD}...")
df_conversion = run_query_file_hsv("productos/creacion_usuario_sv/2026-03/queries/conversion.sql")
print(f"  {len(df_conversion)} registros en conversión")

print("Cargando datos de campañas RTM...")
df_rtm = run_query_file_hsv("productos/creacion_usuario_sv/2026-03/queries/comunicacionesRTM.sql")
print(f"  {len(df_rtm)} registros en RTM")

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


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return ""
    return solo_digitos[-8:].zfill(8)


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


def primer_no_default(series: pd.Series, default: str) -> str:
    for val in series:
        if pd.notna(val) and str(val).strip() and str(val).strip() != default:
            return str(val).strip()
    return default


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
    conteo = df_mes.groupby("dia")["id_usuario"].nunique()
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
            hovertemplate="Día %{x}<br>Usuarios creados (distintos): %{y:,}<extra></extra>",
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1, range=[0.5, ultimo_dia + 0.5]),
        yaxis=dict(title="Usuarios distintos", range=[0, max_y + max(1, int(max_y * 0.15))]),
        showlegend=False,
    )
    return fig


def construir_figura_genero(df_mes: pd.DataFrame) -> go.Figure:
    if df_mes.empty:
        return construir_figura_vacia("No hay datos de género para el período seleccionado")

    orden = ["Mujer", "Hombre", "Sin dato"]
    conteos = df_mes.groupby("genero_normalizado")["id_usuario"].nunique().reindex(orden, fill_value=0)
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
    conteos = df_mes.groupby("generacion")["id_usuario"].nunique().reindex(orden, fill_value=0)
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

    tabla = df_mes.groupby("departamento")["id_usuario"].nunique().sort_values(ascending=False)
    top5 = tabla.head(5)
    if top5.empty:
        return construir_figura_vacia("No hay datos de ubicación para el período seleccionado")

    total = int(df_mes["id_usuario"].nunique())
    porcentajes = (top5 / total) * 100 if total > 0 else (top5 * 0)
    textos = [f"{int(v):,} ({p:.1f}%)" for v, p in zip(top5.values.tolist(), porcentajes.values.tolist())]

    fig = go.Figure(data=[
        go.Bar(
            y=top5.index.tolist()[::-1],
            x=top5.values.tolist()[::-1],
            orientation="h",
            marker_color=COLORES["azul_experto"],
            text=textos[::-1],
            textposition="outside",
            hovertemplate="%{y}<br>%{x:,} usuarios distintos (%{customdata:.1f}%)<extra></extra>",
            customdata=porcentajes.values.tolist()[::-1],
        )
    ])

    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=140, r=25),
        xaxis=dict(title="Usuarios distintos"),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


# ── Preparación de datos ─────────────────────────────────────────────────────
df_conversion["fecha_creacion_usuario"] = pd.to_datetime(df_conversion["fecha_creacion_usuario"], errors="coerce")
df_conversion["fecha_nacimiento_usuario"] = pd.to_datetime(df_conversion["fecha_nacimiento_usuario"], errors="coerce")
df_conversion["codigo_cliente_usuario_creado"] = df_conversion["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)

df_conversion = df_conversion[df_conversion["fecha_creacion_usuario"].notna()].copy()
df_conversion["anio"] = df_conversion["fecha_creacion_usuario"].dt.year
df_conversion["mes"] = df_conversion["fecha_creacion_usuario"].dt.month
df_conversion["dia"] = df_conversion["fecha_creacion_usuario"].dt.day
df_conversion["genero_normalizado"] = df_conversion["genero_cliente"].apply(normalizar_genero)
df_conversion["generacion"] = df_conversion["fecha_nacimiento_usuario"].apply(clasificar_generacion)
df_conversion["departamento"] = df_conversion["direccion_lvl_2"].apply(normalizar_departamento)

# id_usuario para conteo distinto (equivalente COUNTD/RECDIST de Tableau)
# Tableau no cuenta nulos en COUNTD, por eso vacíos se convierten a NA.
df_conversion["id_usuario"] = df_conversion["nombre_usuario"].astype(str).str.strip()
df_conversion.loc[df_conversion["nombre_usuario"].isna(), "id_usuario"] = pd.NA
df_conversion.loc[df_conversion["id_usuario"] == "", "id_usuario"] = pd.NA

df_rtm["fecha_campania"] = pd.to_datetime(df_rtm["fecha_campania"], errors="coerce")
df_rtm["codigo_cliente_usuario_campania"] = df_rtm["codigo_cliente_usuario_campania"].apply(normalizar_codigo_cliente)
df_rtm["anio"] = df_rtm["fecha_campania"].dt.year
df_rtm["mes"] = df_rtm["fecha_campania"].dt.month

df_rtm_match = (
    df_rtm[
        ["codigo_cliente_usuario_campania", "anio", "mes", "fecha_campania"]
    ]
    .dropna(subset=["anio", "mes"]) 
    .drop_duplicates(subset=["codigo_cliente_usuario_campania", "anio", "mes"])
    .copy()
)

# match por cliente para definir origen (mismo criterio de IF ISNULL(fecha_campania))
_df = df_conversion.merge(
    df_rtm_match,
    how="left",
    left_on=["codigo_cliente_usuario_creado"],
    right_on=["codigo_cliente_usuario_campania"],
)

_df["origen_creacion"] = _df["fecha_campania"].apply(
    lambda x: "Medios propios" if pd.notna(x) else "Producto"
)

# consolidación por usuario distinto dentro de año/mes (evita sobreconteo por joins)
df = (
    _df.groupby(["anio", "mes", "id_usuario"], as_index=False)
    .agg(
        dia=("dia", "min"),
        fecha_creacion_usuario=("fecha_creacion_usuario", "min"),
        genero_normalizado=("genero_normalizado", lambda s: primer_no_default(s, "Sin dato")),
        generacion=("generacion", lambda s: primer_no_default(s, "OTRA GENERACION")),
        departamento=("departamento", lambda s: primer_no_default(s, "Sin dato")),
        origen_creacion=("origen_creacion", lambda s: "Medios propios" if (s == "Medios propios").any() else "Producto"),
    )
)

# Mantener solo usuarios válidos para replicar RECDIST(nombre_usuario) de Tableau.
df = df[df["id_usuario"].notna()].copy()

print(f"  {df['id_usuario'].nunique():,} usuarios distintos consolidados")

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
            style={"display": "flex", "gap": "16px", "marginBottom": "20px", "maxWidth": "860px", "flexWrap": "wrap"},
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
                html.Div(
                    style={"minWidth": "260px", "flex": "1"},
                    children=[
                        html.P("Origen de creación", style={"margin": "0 0 8px 0", "color": COLORES["gris_texto"], "fontSize": "14px"}),
                        dcc.Dropdown(
                            id="selector-origen",
                            options=[
                                {"label": "Todos", "value": "Todos"},
                                {"label": "Medios propios", "value": "Medios propios"},
                                {"label": "Producto", "value": "Producto"},
                            ],
                            value="Todos",
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
                    html.P("Usuarios creados (distintos)", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-total", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
                    html.P("Medios propios", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-medios", style={"margin": "8px 0 0 0", "color": COLORES["azul_financiero"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                    html.P("Producto", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(id="kpi-producto", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
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
    Output("kpi-medios", "children"),
    Output("kpi-producto", "children"),
    Output("kpi-mujeres", "children"),
    Output("kpi-hombres", "children"),
    Output("kpi-sin-dato", "children"),
    Output("grafico-diario", "figure"),
    Output("grafico-genero", "figure"),
    Output("grafico-generaciones", "figure"),
    Output("grafico-departamentos", "figure"),
    Input("selector-anio", "value"),
    Input("selector-mes", "value"),
    Input("selector-origen", "value"),
)
def actualizar_dashboard(anio, mes, origen):
    if anio is None or mes is None:
        fig_vacia = construir_figura_vacia("No hay datos disponibles")
        return (
            "Creación de Usuario SV",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            fig_vacia,
            fig_vacia,
            fig_vacia,
            fig_vacia,
        )

    df_periodo = df[(df["anio"] == anio) & (df["mes"] == mes)].copy()

    if origen in {"Medios propios", "Producto"}:
        df_visual = df_periodo[df_periodo["origen_creacion"] == origen].copy()
    else:
        df_visual = df_periodo

    total = int(df_visual["id_usuario"].nunique())
    medios = int(df_periodo.loc[df_periodo["origen_creacion"] == "Medios propios", "id_usuario"].nunique())
    producto = int(df_periodo.loc[df_periodo["origen_creacion"] == "Producto", "id_usuario"].nunique())
    mujeres = int(df_visual.loc[df_visual["genero_normalizado"] == "Mujer", "id_usuario"].nunique())
    hombres = int(df_visual.loc[df_visual["genero_normalizado"] == "Hombre", "id_usuario"].nunique())
    sin_dato = int(df_visual.loc[df_visual["genero_normalizado"] == "Sin dato", "id_usuario"].nunique())

    sufijo = f" | {origen}" if origen and origen != "Todos" else ""
    titulo = f"Creación de Usuario SV — {MESES_ES[int(mes)]} {int(anio)}{sufijo}"

    return (
        titulo,
        f"{total:,}",
        f"{medios:,}",
        f"{producto:,}",
        f"{mujeres:,}",
        f"{hombres:,}",
        f"{sin_dato:,}",
        construir_figura_diaria(df_visual, int(anio), int(mes)),
        construir_figura_genero(df_visual),
        construir_figura_generaciones(df_visual),
        construir_figura_departamentos(df_visual),
    )


if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8053")
    app.run(debug=True, use_reloader=False, port=8053)
