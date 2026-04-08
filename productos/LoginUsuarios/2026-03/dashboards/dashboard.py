"""
dashboard.py
------------
Dashboard LoginUsuarios - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python3 productos/LoginUsuarios/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

from pathlib import Path
import calendar

import pandas as pd
from dash import Dash, html, dcc, dash_table, Input, Output
import plotly.graph_objects as go

from core.db import run_query_file
from core.colors import COLORES


RUTA_QUERY_LOGINS = "productos/LoginUsuarios/2026-03/queries/Logins.sql"
RUTA_DATA_CONTACTADOS = Path("productos/LoginUsuarios/2026-03/archivoExcel")
VALOR_TODOS_CANALES = "__TODOS__"
COLUMNAS_TABLA_CLIENTES = [
    "padded_codigo_usuario",
    "usuario",
    "canal_contacto",
    "total_logins",
    "primer_login",
    "ultimo_login",
]


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_nombre_columna(nombre: str) -> str:
    return (
        str(nombre)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace(" ", "_")
    )


def detectar_columna_codigo(columnas: list[str]) -> str | None:
    columnas_norm = {c: normalizar_nombre_columna(c) for c in columnas}
    for original, normalizada in columnas_norm.items():
        if "codigo" in normalizada and ("cliente" in normalizada or "usuario" in normalizada):
            return original
    for original, normalizada in columnas_norm.items():
        if normalizada in {"cldoc", "codigo", "cliente"}:
            return original
    return None


def detectar_columna_canal(columnas: list[str]) -> str | None:
    columnas_norm = {c: normalizar_nombre_columna(c) for c in columnas}
    for original, normalizada in columnas_norm.items():
        if "canal" in normalizada:
            return original
    return None


def normalizar_canal_contacto(valor) -> str:
    if pd.isna(valor):
        return "Sin canal"

    texto = str(valor).strip().lower()
    if not texto:
        return "Sin canal"
    if "rtm" in texto:
        return "RTM"
    if "pauta" in texto:
        return "Pauta"
    return "Otro"


def consolidar_canal_contacto(series: pd.Series) -> str:
    valores = sorted(set(v for v in series.dropna().tolist() if v and v != "Sin canal"))
    if not valores:
        return "Sin canal"
    if len(valores) == 1:
        return valores[0]
    if set(valores) == {"RTM", "Pauta"}:
        return "RTM + Pauta"
    return "Mixto"


def usuario_representativo(series: pd.Series) -> str:
    s = series.fillna("").astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return ""
    return s.value_counts().index[0]


def filtrar_por_canal(df: pd.DataFrame, canal: str | None) -> pd.DataFrame:
    if df.empty or canal in (None, "", VALOR_TODOS_CANALES):
        return df.copy()
    return df[df["canal_contacto"] == canal].copy()


def construir_tabla_clientes(df_logins: pd.DataFrame) -> pd.DataFrame:
    if df_logins.empty:
        return pd.DataFrame(columns=COLUMNAS_TABLA_CLIENTES)
    return (
        df_logins.groupby("padded_codigo_usuario", as_index=False)
        .agg(
            total_logins=("fecha_inicio", "count"),
            primer_login=("fecha_inicio", "min"),
            ultimo_login=("fecha_inicio", "max"),
            usuario=("id_usuario", usuario_representativo),
            canal_contacto=("canal_contacto", consolidar_canal_contacto),
        )
        .sort_values("total_logins", ascending=False)
    )


def preparar_tabla_clientes(df_tabla: pd.DataFrame) -> pd.DataFrame:
    if df_tabla.empty:
        return pd.DataFrame(columns=COLUMNAS_TABLA_CLIENTES)
    df_show = df_tabla.copy()
    df_show["primer_login"] = df_show["primer_login"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df_show["ultimo_login"] = df_show["ultimo_login"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df_show


def cargar_contactados() -> tuple[pd.DataFrame, str | None]:
    archivos_preferidos = [
        RUTA_DATA_CONTACTADOS / "Contactados.xlsx",
        RUTA_DATA_CONTACTADOS / "contactados.xlsx",
        RUTA_DATA_CONTACTADOS / "Contactados.csv",
        RUTA_DATA_CONTACTADOS / "contactados.csv",
    ]

    archivo = None
    for candidato in archivos_preferidos:
        if candidato.exists():
            archivo = candidato
            break

    if archivo is None and RUTA_DATA_CONTACTADOS.exists():
        encontrados = sorted(
            p for p in RUTA_DATA_CONTACTADOS.iterdir()
            if p.is_file() and "contact" in p.name.lower() and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
        )
        if encontrados:
            archivo = encontrados[0]

    if archivo is None:
        return pd.DataFrame(columns=["padded_codigo_usuario", "canal_contacto"]), None

    if archivo.suffix.lower() in {".xlsx", ".xls"}:
        df_contactados = pd.read_excel(archivo)
    else:
        df_contactados = pd.read_csv(archivo)

    col_codigo = detectar_columna_codigo(df_contactados.columns.tolist())
    col_canal = detectar_columna_canal(df_contactados.columns.tolist())

    if col_codigo is None:
        print(f"[ADVERTENCIA] No se encontró columna de código en {archivo}.")
        return pd.DataFrame(columns=["padded_codigo_usuario", "canal_contacto"]), str(archivo)

    df_contactados["padded_codigo_usuario"] = df_contactados[col_codigo].apply(normalizar_codigo)
    if col_canal:
        df_contactados["canal_contacto"] = df_contactados[col_canal].apply(normalizar_canal_contacto)
    else:
        df_contactados["canal_contacto"] = "Sin canal"

    df_contactados = df_contactados[df_contactados["padded_codigo_usuario"].notna()].copy()

    # Un cliente puede aparecer varias veces en el Excel; consolidamos su canal.
    df_contactados = (
        df_contactados.groupby("padded_codigo_usuario", as_index=False)
        .agg(canal_contacto=("canal_contacto", consolidar_canal_contacto))
    )

    return df_contactados, str(archivo)


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


def _agregar_barras_apiladas(
    fig: go.Figure,
    pivot: pd.DataFrame,
    dias: list,
    max_total: int,
    colores: list,
    hover_sufijo: str,
) -> None:
    """Agrega barras apiladas con etiquetas inteligentes:
    - Segmentos grandes: numero adentro
    - Segmentos pequeños: numero centrado (scatter), con mejor legibilidad
    """
    threshold = max(10, int(max_total * 0.15))
    cumsum = pivot.cumsum(axis=1)

    for idx, columna in enumerate(pivot.columns.tolist()):
        color = colores[idx % len(colores)]
        valores = pivot[columna].tolist()
        color_texto_interno = (
            COLORES["azul_experto"]
            if color in {COLORES["amarillo_opt"], COLORES["amarillo_emp"]}
            else COLORES["blanco"]
        )

        # Texto dentro solo para segmentos grandes
        texto_dentro = [f"{v:,}" if v >= threshold else "" for v in valores]

        fig.add_trace(go.Bar(
            name=columna,
            x=dias,
            y=valores,
            marker_color=color,
            text=texto_dentro,
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(size=13, color=color_texto_interno),
            textangle=0,
            hovertemplate=f"Día %{{x}}<br>{columna}: %{{y:,}} {hover_sufijo}<extra></extra>",
        ))

        # Scatter para segmentos pequeños: posicionado en el centro exacto del segmento
        tops = cumsum[columna].tolist()
        x_pequeños, y_pequeños, text_pequeños = [], [], []
        for i, v in enumerate(valores):
            if 0 < v < threshold:
                midpoint = tops[i] - v / 2
                x_pequeños.append(dias[i])
                y_pequeños.append(midpoint)
                text_pequeños.append(f"{v:,}")

        if x_pequeños:
            fig.add_trace(go.Scatter(
                x=x_pequeños,
                y=y_pequeños,
                mode="text",
                text=text_pequeños,
                textfont=dict(size=13, color=COLORES["azul_experto"]),
                textposition="middle center",
                showlegend=False,
                hoverinfo="skip",
                cliponaxis=False,
            ))


def construir_figura_logins_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return construir_figura_vacia("No hay logins de clientes del Excel para el período")

    anio = int(df_logins["fecha_inicio"].dt.year.mode().iloc[0])
    mes = int(df_logins["fecha_inicio"].dt.month.mode().iloc[0])
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))

    pivot = (
        df_logins.groupby(["dia", "canal_contacto"])
        .size()
        .unstack(fill_value=0)
        .reindex(dias, fill_value=0)
    )

    totales_dia = pivot.sum(axis=1).tolist()
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_financiero"], COLORES["azul_experto"]]
    _agregar_barras_apiladas(fig, pivot, dias, max_total, colores, "eventos")

    fig.add_trace(go.Scatter(
        x=dias,
        y=[t + separacion for t in totales_dia],
        mode="text",
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="top center",
        textfont=dict(size=11, color=COLORES["azul_experto"]),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        barmode="stack",
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1, range=[0.5, ultimo_dia + 0.5]),
        yaxis=dict(title="Eventos de login", range=[0, max_total + (separacion * 3)]),
        legend_title_text="Canal contacto (Excel)",
    )
    return fig


def construir_figura_clientes_unicos_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return construir_figura_vacia("No hay clientes con login para el período")

    anio = int(df_logins["fecha_inicio"].dt.year.mode().iloc[0])
    mes = int(df_logins["fecha_inicio"].dt.month.mode().iloc[0])
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))

    df_unicos = (
        df_logins[["dia", "canal_contacto", "padded_codigo_usuario"]]
        .drop_duplicates()
    )

    pivot = (
        df_unicos.groupby(["dia", "canal_contacto"])
        .size()
        .unstack(fill_value=0)
        .reindex(dias, fill_value=0)
    )

    totales_dia = pivot.sum(axis=1).tolist()
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_financiero"], COLORES["azul_experto"]]
    _agregar_barras_apiladas(fig, pivot, dias, max_total, colores, "clientes únicos")

    fig.add_trace(go.Scatter(
        x=dias,
        y=[t + separacion for t in totales_dia],
        mode="text",
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="top center",
        textfont=dict(size=11, color=COLORES["azul_experto"]),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        barmode="stack",
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1, range=[0.5, ultimo_dia + 0.5]),
        yaxis=dict(title="Clientes únicos", range=[0, max_total + (separacion * 3)]),
        legend_title_text="Canal contacto (Excel)",
    )
    return fig


def construir_figura_canales_login(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return construir_figura_vacia("No hay datos de canal de login para clientes del Excel")

    pivot = (
        df_logins.groupby(["canal_login", "canal_contacto"]) 
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_financiero"], COLORES["azul_experto"]]
    for idx, columna in enumerate(pivot.columns.tolist()):
        fig.add_trace(go.Bar(
            name=columna,
            x=pivot.index.tolist(),
            y=pivot[columna].tolist(),
            marker_color=colores[idx % len(colores)],
            hovertemplate=f"Canal login %{{x}}<br>{columna}: %{{y:,}} eventos<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Canal login"),
        yaxis=dict(title="Eventos"),
        legend_title_text="Canal contacto (Excel)",
    )
    return fig


def construir_figura_distribucion_contacto(df_logins: pd.DataFrame) -> go.Figure:
    conteos = df_logins["canal_contacto"].value_counts()
    if conteos.empty:
        return construir_figura_vacia("No hay datos de canal de contacto")

    fig = go.Figure(data=[
        go.Pie(
            labels=conteos.index.tolist(),
            values=conteos.values.tolist(),
            hole=0.45,
            texttemplate="%{label}<br>%{value:,} (%{percent})",
            textposition="outside",
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


def construir_figura_primer_login_por_fecha(df_clientes: pd.DataFrame) -> go.Figure:
    if df_clientes.empty or "primer_login" not in df_clientes.columns:
        return construir_figura_vacia("No hay datos de primer login para el período")

    tmp = df_clientes.copy()
    tmp = tmp[tmp["primer_login"].notna()].copy()
    if tmp.empty:
        return construir_figura_vacia("No hay datos de primer login para el período")

    anio = int(tmp["primer_login"].dt.year.mode().iloc[0])
    mes = int(tmp["primer_login"].dt.month.mode().iloc[0])
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    dias = list(range(1, ultimo_dia + 1))

    tmp["dia_primer_login"] = tmp["primer_login"].dt.day
    pivot = (
        tmp.groupby(["dia_primer_login", "canal_contacto"])
        .size()
        .unstack(fill_value=0)
        .reindex(dias, fill_value=0)
    )

    totales_dia = pivot.sum(axis=1).tolist()
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_financiero"], COLORES["azul_experto"]]
    _agregar_barras_apiladas(fig, pivot, dias, max_total, colores, "clientes")

    fig.add_trace(go.Scatter(
        x=dias,
        y=[t + separacion for t in totales_dia],
        mode="text",
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="top center",
        textfont=dict(size=11, color=COLORES["azul_experto"]),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        barmode="stack",
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día del primer login", tickmode="linear", tick0=1, dtick=1, range=[0.5, ultimo_dia + 0.5]),
        yaxis=dict(title="Clientes únicos", range=[0, max_total + (separacion * 3)]),
        legend_title_text="Canal contacto (Excel)",
    )
    return fig


# ── Carga y preparación de datos ─────────────────────────────────────────────
print("Cargando logins...")
df = run_query_file(RUTA_QUERY_LOGINS)
print(f"  {len(df)} eventos de login cargados")

df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
df = df[df["fecha_inicio"].notna()].copy()
df["dia"] = df["fecha_inicio"].dt.day
df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
df["id_usuario"] = df["nombre_usuario"].fillna("").astype(str).str.strip()
df.loc[df["id_usuario"] == "", "id_usuario"] = df["padded_codigo_usuario"]

contactados, ruta_contactados = cargar_contactados()
if ruta_contactados:
    print(f"Cargando contactados desde: {ruta_contactados}")
    print(f"  {contactados['padded_codigo_usuario'].nunique():,} clientes en Excel")
else:
    print("No se encontró archivo Contactados.")

if contactados.empty:
    df_filtrado = df.iloc[0:0].copy()
else:
    df_filtrado = df.merge(
        contactados,
        how="inner",
        on="padded_codigo_usuario",
    )

# Diagnóstico rápido para validar cobertura del cruce SQL vs Excel (días 1-4).
print("Clientes SQL únicos:", df["padded_codigo_usuario"].nunique())
print("Clientes Excel únicos:", contactados["padded_codigo_usuario"].nunique())
print("Intersección:", len(set(df["padded_codigo_usuario"]) & set(contactados["padded_codigo_usuario"])))
print("SQL por día 1-4:\n", df[df["dia"].isin([1, 2, 3, 4])].groupby("dia").size())
print("MERGE por día 1-4:\n", df_filtrado[df_filtrado["dia"].isin([1, 2, 3, 4])].groupby("dia").size())

opciones_canal = [{"label": "Todos", "value": VALOR_TODOS_CANALES}] + [
    {"label": c, "value": c}
    for c in sorted(contactados["canal_contacto"].dropna().astype(str).unique().tolist())
]

df_inicial = filtrar_por_canal(df_filtrado, VALOR_TODOS_CANALES)
contactados_inicial = filtrar_por_canal(contactados, VALOR_TODOS_CANALES)

total_eventos = int(df_inicial.shape[0])
usuarios_unicos = int(df_inicial["id_usuario"].nunique()) if not df_inicial.empty else 0
clientes_excel = int(contactados_inicial["padded_codigo_usuario"].nunique()) if not contactados_inicial.empty else 0
clientes_excel_con_login = int(df_inicial["padded_codigo_usuario"].nunique()) if not df_inicial.empty else 0

_df_primer_login = construir_tabla_clientes(df_inicial)
_df_tabla = preparar_tabla_clientes(_df_primer_login)


# ── App ───────────────────────────────────────────────────────────────────────
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
        html.H2("LoginUsuarios — Marzo 2026", style={"color": COLORES["azul_experto"], "marginBottom": "10px"}),
        html.P(
            f"Solo clientes del Excel (Cliente + Canal). Archivo: {ruta_contactados if ruta_contactados else 'No encontrado'}",
            style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "20px"},
        ),
        html.Div(
            style={
                "backgroundColor": COLORES["blanco"],
                "borderRadius": "8px",
                "padding": "14px 18px",
                "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
                "marginBottom": "20px",
            },
            children=[
                html.Label(
                    "Filtro general por canal (Excel)",
                    style={
                        "display": "block",
                        "marginBottom": "8px",
                        "fontWeight": "bold",
                        "color": COLORES["azul_experto"],
                    },
                ),
                dcc.Dropdown(
                    id="filtro-canal",
                    options=opciones_canal,
                    value=VALOR_TODOS_CANALES,
                    clearable=False,
                    placeholder="Selecciona canal",
                    style={"maxWidth": "420px"},
                ),
            ],
        ),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "16px", "marginBottom": "24px"},
            children=[
                html.Div(style=card_style, children=[
                    html.P("Eventos de login (solo Excel)", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{total_eventos:,}", id="kpi-total-eventos", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
                    html.P("Usuarios únicos con login", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{usuarios_unicos:,}", id="kpi-usuarios-unicos", style={"margin": "8px 0 0 0", "color": COLORES["azul_financiero"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.P("Clientes en Excel", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{clientes_excel:,}", id="kpi-clientes-excel", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                    html.P("Clientes Excel con login", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{clientes_excel_con_login:,}", id="kpi-clientes-con-login", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
                ]),
            ],
        ),

        html.Div(style={**card_style, "marginBottom": "20px"}, children=[
            html.H4("Eventos por día (segmentado por canal del Excel)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="graf-logins-dia", figure=construir_figura_logins_dia(df_inicial)),
        ]),

        html.Div(style={**card_style, "marginBottom": "20px", "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
            html.H4(
                "Clientes únicos por día",
                style={"color": COLORES["azul_experto"], "marginTop": 0},
            ),
            dcc.Graph(id="graf-clientes-unicos-dia", figure=construir_figura_clientes_unicos_dia(df_inicial)),
        ]),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))", "gap": "16px", "marginBottom": "20px"},
            children=[
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_experto']}"}, children=[
                    html.H4("Canal de login vs Canal del Excel", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="graf-canales-login", figure=construir_figura_canales_login(df_inicial)),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.H4("Distribución por canal del Excel", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="graf-distribucion-contacto", figure=construir_figura_distribucion_contacto(df_inicial)),
                ]),
            ],
        ),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
            html.H4("Clientes con login (1 fila por cliente)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dash_table.DataTable(
                columns=[
                    {"name": "Código cliente", "id": "padded_codigo_usuario"},
                    {"name": "Usuario", "id": "usuario"},
                    {"name": "Canal contacto (Excel)", "id": "canal_contacto"},
                    {"name": "Total logins", "id": "total_logins"},
                    {"name": "Primer login", "id": "primer_login"},
                    {"name": "Último login", "id": "ultimo_login"},
                ],
                data=_df_tabla[COLUMNAS_TABLA_CLIENTES].to_dict("records"),
                id="tabla-clientes",
                page_size=12,
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "fontFamily": "Arial", "fontSize": "13px"},
                style_header={"backgroundColor": COLORES["azul_experto"], "color": COLORES["blanco"], "fontWeight": "bold"},
                style_data={"backgroundColor": COLORES["blanco"], "color": COLORES["azul_experto"]},
            ),
        ]),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}", "marginTop": "20px"}, children=[
            html.H4(
                "Clientes por fecha de primer login",
                style={"color": COLORES["azul_experto"], "marginTop": 0},
            ),
            dcc.Graph(id="graf-primer-login", figure=construir_figura_primer_login_por_fecha(_df_primer_login)),
        ]),
    ],
)


@app.callback(
    Output("kpi-total-eventos", "children"),
    Output("kpi-usuarios-unicos", "children"),
    Output("kpi-clientes-excel", "children"),
    Output("kpi-clientes-con-login", "children"),
    Output("graf-logins-dia", "figure"),
    Output("graf-clientes-unicos-dia", "figure"),
    Output("graf-canales-login", "figure"),
    Output("graf-distribucion-contacto", "figure"),
    Output("tabla-clientes", "data"),
    Output("graf-primer-login", "figure"),
    Input("filtro-canal", "value"),
)
def actualizar_dashboard(canal_seleccionado: str):
    df_canal = filtrar_por_canal(df_filtrado, canal_seleccionado)
    contactados_canal = filtrar_por_canal(contactados, canal_seleccionado)

    total_eventos_canal = int(df_canal.shape[0])
    usuarios_unicos_canal = int(df_canal["id_usuario"].nunique()) if not df_canal.empty else 0
    clientes_excel_canal = int(contactados_canal["padded_codigo_usuario"].nunique()) if not contactados_canal.empty else 0
    clientes_excel_con_login_canal = int(df_canal["padded_codigo_usuario"].nunique()) if not df_canal.empty else 0

    df_primer_login_canal = construir_tabla_clientes(df_canal)
    df_tabla_canal = preparar_tabla_clientes(df_primer_login_canal)
    tabla_data = df_tabla_canal[COLUMNAS_TABLA_CLIENTES].to_dict("records")

    return (
        f"{total_eventos_canal:,}",
        f"{usuarios_unicos_canal:,}",
        f"{clientes_excel_canal:,}",
        f"{clientes_excel_con_login_canal:,}",
        construir_figura_logins_dia(df_canal),
        construir_figura_clientes_unicos_dia(df_canal),
        construir_figura_canales_login(df_canal),
        construir_figura_distribucion_contacto(df_canal),
        tabla_data,
        construir_figura_primer_login_por_fecha(df_primer_login_canal),
    )


if __name__ == "__main__":
    print("Dashboard LoginUsuarios corriendo en http://127.0.0.1:8054")
    app.run(debug=False, use_reloader=False, port=8054)
