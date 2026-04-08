"""
dashboard_rtm.py
----------------
Dashboard LoginUsuarios RTM - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python3 productos/LoginUsuarios/2026-03/dashboards/dashboard_rtm.py
"""

import sys
sys.path.insert(0, ".")

from pathlib import Path

import pandas as pd
from dash import Dash, html, dcc, dash_table
import plotly.graph_objects as go

from core.db import run_query
from core.colors import COLORES


RUTA_DATA_CONTACTADOS = Path("productos/LoginUsuarios/2026-03/archivoExcel")
CANAL_OBJETIVO = "RTM"
FECHA_INICIO = pd.Timestamp("2026-03-16")
FECHA_FIN = pd.Timestamp("2026-04-08")
RANGO_FECHAS = pd.date_range(FECHA_INICIO, FECHA_FIN, freq="D")
FECHAS_CAMPANA = {
    pd.Timestamp("2026-03-06").date(),
    pd.Timestamp("2026-03-13").date(),
    pd.Timestamp("2026-03-18").date(),
    pd.Timestamp("2026-03-21").date(),
    pd.Timestamp("2026-03-28").date(),
    pd.Timestamp("2026-04-04").date(),
}
COLOR_CAMPANA = "#D62828"
COLOR_NORMAL = COLORES["aqua_digital"]
SQL_LOGINS_RANGO = """
SELECT
    clccli as codigo_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(clccli)),8) as padded_codigo_usuario,
    uscode as nombre_usuario,
    secode as canal_login,
    dw_fecha_trx as fecha_inicio
FROM dw_bel_IBSTTRA_VIEW
WHERE dw_fecha_trx >= '2026-03-01'
  AND dw_fecha_trx < '2026-04-09'
  AND SECODE in ('app-login','web-login','login')
"""
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


def colores_por_fecha(fechas: list[pd.Timestamp]) -> list[str]:
    return [COLOR_CAMPANA if pd.Timestamp(f).date() in FECHAS_CAMPANA else COLOR_NORMAL for f in fechas]


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
        return construir_figura_vacia("No hay logins RTM para el período")

    fechas = list(RANGO_FECHAS)
    totales_dia = (
        df_logins.groupby("fecha_dia")
        .size()
        .reindex(RANGO_FECHAS, fill_value=0)
        .tolist()
    )
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))
    colores_dias = colores_por_fecha(fechas)
    custom_data = [["Sí" if pd.Timestamp(f).date() in FECHAS_CAMPANA else "No"] for f in fechas]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=fechas,
        y=totales_dia,
        marker_color=colores_dias,
        customdata=custom_data,
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="outside",
        hovertemplate="Fecha %{x|%Y-%m-%d}<br>Eventos: %{y:,}<br>Día de campaña: %{customdata[0]}<extra></extra>",
        showlegend=False,
    ))

    fig.update_layout(
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Fecha", tickmode="linear", dtick=86400000.0, tickformat="%d-%b"),
        yaxis=dict(title="Eventos de login", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
    )
    return fig


def construir_figura_clientes_unicos_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return construir_figura_vacia("No hay clientes RTM con login para el período")

    fechas = list(RANGO_FECHAS)

    df_unicos = (
        df_logins[["fecha_dia", "canal_contacto", "padded_codigo_usuario"]]
        .drop_duplicates()
    )

    totales_dia = (
        df_unicos.groupby("fecha_dia")
        .size()
        .reindex(RANGO_FECHAS, fill_value=0)
        .tolist()
    )
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))
    colores_dias = colores_por_fecha(fechas)
    custom_data = [["Sí" if pd.Timestamp(f).date() in FECHAS_CAMPANA else "No"] for f in fechas]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=fechas,
        y=totales_dia,
        marker_color=colores_dias,
        customdata=custom_data,
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="outside",
        hovertemplate="Fecha %{x|%Y-%m-%d}<br>Clientes únicos: %{y:,}<br>Día de campaña: %{customdata[0]}<extra></extra>",
        showlegend=False,
    ))

    fig.update_layout(
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Fecha", tickmode="linear", dtick=86400000.0, tickformat="%d-%b"),
        yaxis=dict(title="Clientes únicos", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
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
        return construir_figura_vacia("No hay datos de primer login RTM para el período")

    tmp = df_clientes.copy()
    tmp = tmp[tmp["primer_login"].notna()].copy()
    if tmp.empty:
        return construir_figura_vacia("No hay datos de primer login para el período")

    fechas = list(RANGO_FECHAS)
    tmp["fecha_primer_login_dia"] = tmp["primer_login"].dt.normalize()
    totales_dia = (
        tmp.groupby("fecha_primer_login_dia")
        .size()
        .reindex(RANGO_FECHAS, fill_value=0)
        .tolist()
    )
    max_total = max(totales_dia) if totales_dia else 0
    separacion = max(1, int(max_total * 0.04))
    colores_dias = colores_por_fecha(fechas)
    custom_data = [["Sí" if pd.Timestamp(f).date() in FECHAS_CAMPANA else "No"] for f in fechas]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=fechas,
        y=totales_dia,
        marker_color=colores_dias,
        customdata=custom_data,
        text=[f"{t:,}" if t > 0 else "" for t in totales_dia],
        textposition="outside",
        hovertemplate="Fecha %{x|%Y-%m-%d}<br>Clientes: %{y:,}<br>Día de campaña: %{customdata[0]}<extra></extra>",
        showlegend=False,
    ))

    fig.update_layout(
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Fecha del primer login", tickmode="linear", dtick=86400000.0, tickformat="%d-%b"),
        yaxis=dict(title="Clientes únicos", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
    )
    return fig


# ── Carga y preparación de datos ─────────────────────────────────────────────
print("Cargando logins (marzo-abril)...")
df = run_query(SQL_LOGINS_RANGO)
print(f"  {len(df)} eventos de login cargados (01-mar al 08-abr)")

df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
df = df[df["fecha_inicio"].notna()].copy()
df["fecha_dia"] = df["fecha_inicio"].dt.normalize()
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

# Diagnóstico rápido para validar cobertura del cruce SQL vs Excel en rango visible.
print("Clientes SQL únicos:", df["padded_codigo_usuario"].nunique())
print("Clientes Excel únicos:", contactados["padded_codigo_usuario"].nunique())
print("Intersección:", len(set(df["padded_codigo_usuario"]) & set(contactados["padded_codigo_usuario"])))
print(
    "SQL en rango 16-mar a 08-abr:",
    int(((df["fecha_dia"] >= FECHA_INICIO) & (df["fecha_dia"] <= FECHA_FIN)).sum()),
)
print(
    "MERGE en rango 16-mar a 08-abr:",
    int(((df_filtrado["fecha_dia"] >= FECHA_INICIO) & (df_filtrado["fecha_dia"] <= FECHA_FIN)).sum()),
)

df_rtm = df_filtrado[df_filtrado["canal_contacto"] == CANAL_OBJETIVO].copy()
contactados_rtm = contactados[contactados["canal_contacto"] == CANAL_OBJETIVO].copy()
df_rtm = df_rtm[(df_rtm["fecha_dia"] >= FECHA_INICIO) & (df_rtm["fecha_dia"] <= FECHA_FIN)].copy()

total_eventos = int(df_rtm.shape[0])
usuarios_unicos = int(df_rtm["id_usuario"].nunique()) if not df_rtm.empty else 0
clientes_excel = int(contactados_rtm["padded_codigo_usuario"].nunique()) if not contactados_rtm.empty else 0
clientes_excel_con_login = int(df_rtm["padded_codigo_usuario"].nunique()) if not df_rtm.empty else 0

_df_primer_login = construir_tabla_clientes(df_rtm)
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
        html.H2("LoginUsuarios RTM — 16 Mar al 08 Abr 2026", style={"color": COLORES["azul_experto"], "marginBottom": "10px"}),
        html.P(
            (
                f"Base dedicada al canal {CANAL_OBJETIVO}. "
                f"Días de campaña resaltados en rojo: {[d.strftime('%Y-%m-%d') for d in sorted(FECHAS_CAMPANA)]}. "
                f"Archivo: {ruta_contactados if ruta_contactados else 'No encontrado'}"
            ),
            style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "20px"},
        ),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "16px", "marginBottom": "24px"},
            children=[
                html.Div(style=card_style, children=[
                    html.P("Eventos de login (solo RTM)", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
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
            html.H4("Eventos por día (solo RTM)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(id="graf-logins-dia", figure=construir_figura_logins_dia(df_rtm)),
        ]),

        html.Div(style={**card_style, "marginBottom": "20px", "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
            html.H4(
                "Clientes únicos por día (solo RTM)",
                style={"color": COLORES["azul_experto"], "marginTop": 0},
            ),
            dcc.Graph(id="graf-clientes-unicos-dia", figure=construir_figura_clientes_unicos_dia(df_rtm)),
        ]),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))", "gap": "16px", "marginBottom": "20px"},
            children=[
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_experto']}"}, children=[
                    html.H4("Canal de login vs Canal del Excel", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="graf-canales-login", figure=construir_figura_canales_login(df_rtm)),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.H4("Distribución por canal del Excel", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(id="graf-distribucion-contacto", figure=construir_figura_distribucion_contacto(df_rtm)),
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

if __name__ == "__main__":
    print("Dashboard LoginUsuarios RTM corriendo en http://127.0.0.1:8055")
    app.run(debug=False, use_reloader=False, port=8055)
