"""
dashboard_qbr1_post_login.py
----------------------------
Dashboard Post Login - Q1 2026.

Universo:
- Clientes del archivo Excel en ArchivosExcel.
- Eventos del query local sin_login en Q1 2026.
- Se consideran eventos post-login por cliente (después de su primer login Q1).

Ejecucion:
    python3 productos/LoginUsuarios/QBR_1_2026/dashboards/dashboard_qbr1_post_login.py
"""

import re
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


RUTA_QUERY_EVENTOS = "productos/LoginUsuarios/QBR_1_2026/queries/sin_login/post_login_q1_2026.sql"
RUTA_EXCEL_BASE = Path("productos/LoginUsuarios/QBR_1_2026/ArchivosExcel")
VALOR_TODOS = "__TODOS__"
FECHA_INICIO_Q1 = pd.Timestamp("2026-01-01")
FECHA_FIN_Q1 = pd.Timestamp("2026-04-01")
MESES_MOSTRAR = ["2026-01", "2026-02", "2026-03"]

MESES_ES = {
    "2026-01": "Enero 2026",
    "2026-02": "Febrero 2026",
    "2026-03": "Marzo 2026",
}


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_nombre_columna(nombre: str) -> str:
    texto = (
        str(nombre)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return texto.strip("_")


def detectar_columna_codigo(columnas: list[str]) -> str | None:
    columnas_norm = {c: normalizar_nombre_columna(c) for c in columnas}
    for original, normalizada in columnas_norm.items():
        if "codigo" in normalizada and ("cliente" in normalizada or "usuario" in normalizada):
            return original
    for original, normalizada in columnas_norm.items():
        if normalizada in {"cif", "cldoc", "codigo", "cliente", "clientes"}:
            return original
    return None


def cargar_base_clientes() -> tuple[pd.DataFrame, str]:
    archivo = None
    preferidos = [
        RUTA_EXCEL_BASE / "Contactados_Enero_2026.xlsx",
        RUTA_EXCEL_BASE / "Contactados_Enero_2026.xls",
        RUTA_EXCEL_BASE / "Contactados_Enero_2026.csv",
        RUTA_EXCEL_BASE / "ArbolRTM.xlsx",
        RUTA_EXCEL_BASE / "ArbolRTM.xls",
        RUTA_EXCEL_BASE / "ArbolRTM.csv",
    ]

    for candidato in preferidos:
        if candidato.exists():
            archivo = candidato
            break

    if archivo is None and RUTA_EXCEL_BASE.exists():
        encontrados = sorted(
            p
            for p in RUTA_EXCEL_BASE.iterdir()
            if p.is_file()
            and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
            and ("contactados" in p.stem.lower() or ("arbol" in p.stem.lower() and "rtm" in p.stem.lower()))
        )
        if encontrados:
            archivo = encontrados[0]

    if archivo is None:
        raise FileNotFoundError(
            "No se encontró archivo base en productos/LoginUsuarios/QBR_1_2026/ArchivosExcel."
        )

    if archivo.suffix.lower() in {".xlsx", ".xls"}:
        df_base = pd.read_excel(archivo)
    else:
        df_base = pd.read_csv(archivo)

    col_codigo = detectar_columna_codigo(df_base.columns.tolist())
    if col_codigo is None:
        raise ValueError(
            f"No se encontró columna de código cliente/usuario en {archivo.name}. "
            "Incluye una columna como CIF, codigo_cliente, Cliente o Clientes."
        )

    df_base["padded_codigo_cliente"] = df_base[col_codigo].apply(normalizar_codigo)
    df_base = df_base[df_base["padded_codigo_cliente"].notna()].drop_duplicates("padded_codigo_cliente")
    return df_base[["padded_codigo_cliente"]].copy(), f"Excel: {archivo}"


def cargar_eventos_query() -> pd.DataFrame:
    df = run_query_file(RUTA_QUERY_EVENTOS)
    df.columns = [normalizar_nombre_columna(c) for c in df.columns]

    if "fecha" not in df.columns:
        raise ValueError("La query de eventos no devolvió la columna Fecha.")

    if "padded_codigo_cliente" not in df.columns:
        if "codigo_cliente" in df.columns:
            df["padded_codigo_cliente"] = df["codigo_cliente"].apply(normalizar_codigo)
        else:
            raise ValueError("La query de eventos no devolvió codigo cliente (padded o normal).")

    if "modulo" not in df.columns:
        df["modulo"] = ""
    if "operacion" not in df.columns:
        df["operacion"] = ""
    if "canal" not in df.columns:
        df["canal"] = ""

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["padded_codigo_cliente"] = df["padded_codigo_cliente"].apply(normalizar_codigo)
    df["modulo"] = df["modulo"].fillna("").astype(str).str.strip()
    df["operacion"] = df["operacion"].fillna("").astype(str).str.strip()
    df["canal"] = df["canal"].fillna("Sin canal").astype(str).str.strip()

    df = df[df["fecha"].notna() & df["padded_codigo_cliente"].notna()].copy()
    df = df[(df["fecha"] >= FECHA_INICIO_Q1) & (df["fecha"] < FECHA_FIN_Q1)].copy()

    modulo_lower = df["modulo"].str.lower()
    operacion_lower = df["operacion"].str.lower()
    df["es_login"] = modulo_lower.str.contains("login", na=False) | operacion_lower.str.contains("login", na=False)

    df["mes"] = df["fecha"].dt.to_period("M").astype(str)
    df["fecha_dia"] = df["fecha"].dt.date

    for col in ["valor", "valorlempirizado", "valordolarizado", "cantidad"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def preparar_datos() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    df_base, fuente_base = cargar_base_clientes()
    df_eventos = cargar_eventos_query()

    universo = set(df_base["padded_codigo_cliente"].tolist())
    df_eventos = df_eventos[df_eventos["padded_codigo_cliente"].isin(universo)].copy()

    df_logins = (
        df_eventos[df_eventos["es_login"]]
        .groupby("padded_codigo_cliente", as_index=False)["fecha"]
        .min()
        .rename(columns={"fecha": "primer_login_q1"})
    )

    if df_logins.empty:
        df_post = df_eventos.iloc[0:0].copy()
    else:
        df_post = df_eventos.merge(df_logins, how="inner", on="padded_codigo_cliente")
        df_post = df_post[(~df_post["es_login"]) & (df_post["fecha"] >= df_post["primer_login_q1"])].copy()

    return df_base, df_logins, df_post, fuente_base


def filtrar_por_canal(df: pd.DataFrame, canal: str) -> pd.DataFrame:
    if canal in (None, "", VALOR_TODOS):
        return df.copy()
    return df[df["canal"] == canal].copy()


def figura_vacia(mensaje: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
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
                font=dict(color=COLORES["gris_texto"], size=14),
            )
        ],
    )
    return fig


def kpi_card(titulo: str, valor: str, color_borde: str) -> html.Div:
    return html.Div(
        style={
            "backgroundColor": COLORES["blanco"],
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
            "borderTop": f"4px solid {color_borde}",
        },
        children=[
            html.P(titulo, style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
            html.H2(valor, style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
        ],
    )


def construir_kpis(df_base: pd.DataFrame, df_logins: pd.DataFrame, df_post: pd.DataFrame) -> list[html.Div]:
    clientes_base = int(df_base["padded_codigo_cliente"].nunique()) if not df_base.empty else 0
    clientes_con_login = int(df_logins["padded_codigo_cliente"].nunique()) if not df_logins.empty else 0
    clientes_post = int(df_post["padded_codigo_cliente"].nunique()) if not df_post.empty else 0
    eventos_post = int(len(df_post))

    monto_col = "valorlempirizado" if "valorlempirizado" in df_post.columns else "valor"
    monto_post = float(df_post[monto_col].fillna(0).sum()) if (not df_post.empty and monto_col in df_post.columns) else 0.0
    tasa_post = (clientes_post / clientes_con_login * 100) if clientes_con_login > 0 else 0.0

    return [
        kpi_card("Clientes base Q1", f"{clientes_base:,}", COLORES["azul_experto"]),
        kpi_card("Clientes con login Q1", f"{clientes_con_login:,}", COLORES["aqua_digital"]),
        kpi_card("Clientes con actividad post-login", f"{clientes_post:,}", COLORES["amarillo_opt"]),
        kpi_card("Eventos post-login", f"{eventos_post:,}", COLORES["azul_financiero"]),
        kpi_card("Monto post-login (L)", f"{monto_post:,.2f}", COLORES["amarillo_emp"]),
        kpi_card("Tasa post-login", f"{tasa_post:,.1f}%", COLORES["azul_financiero"]),
    ]


def grafico_post_mes(df_post: pd.DataFrame) -> go.Figure:
    if df_post.empty:
        return figura_vacia("Sin eventos post-login para el filtro seleccionado")

    resumen = df_post.groupby("mes").size().reindex(MESES_MOSTRAR, fill_value=0)
    etiquetas = [MESES_ES.get(m, m) for m in resumen.index.tolist()]

    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=resumen.tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{int(v):,}" for v in resumen.tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Eventos post-login: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Eventos post-login por mes",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Eventos"),
    )
    return fig


def grafico_clientes_post_mes(df_post: pd.DataFrame) -> go.Figure:
    if df_post.empty:
        return figura_vacia("Sin clientes post-login para el filtro seleccionado")

    resumen = (
        df_post.groupby("mes")["padded_codigo_cliente"]
        .nunique()
        .reindex(MESES_MOSTRAR, fill_value=0)
    )
    etiquetas = [MESES_ES.get(m, m) for m in resumen.index.tolist()]

    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=resumen.tolist(),
                marker_color=COLORES["azul_financiero"],
                text=[f"{int(v):,}" for v in resumen.tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Clientes únicos post-login: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes únicos con actividad post-login por mes",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Clientes"),
    )
    return fig


def grafico_modulo_post(df_post: pd.DataFrame, top_n: int = 12) -> go.Figure:
    if df_post.empty:
        return figura_vacia("Sin módulos post-login para el filtro seleccionado")

    resumen = (
        df_post.assign(modulo=df_post["modulo"].replace("", "SIN MODULO"))
        .groupby("modulo")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .sort_values(ascending=True)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen.index.tolist(),
                x=resumen.values.tolist(),
                orientation="h",
                marker_color=COLORES["amarillo_opt"],
                text=[f"{int(v):,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Módulo: %{y}<br>Eventos: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} módulos post-login",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=180, r=20),
        xaxis=dict(title="Eventos"),
        yaxis=dict(title=""),
    )
    return fig


def grafico_operacion_post(df_post: pd.DataFrame, top_n: int = 15) -> go.Figure:
    if df_post.empty:
        return figura_vacia("Sin operaciones post-login para el filtro seleccionado")

    resumen = (
        df_post.assign(operacion=df_post["operacion"].replace("", "SIN OPERACION"))
        .groupby("operacion")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .sort_values(ascending=True)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen.index.tolist(),
                x=resumen.values.tolist(),
                orientation="h",
                marker_color=COLORES["azul_experto"],
                text=[f"{int(v):,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Operación: %{y}<br>Eventos: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} operaciones post-login",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=220, r=20),
        xaxis=dict(title="Eventos"),
        yaxis=dict(title=""),
    )
    return fig


def tabla_top_clientes_post(df_post: pd.DataFrame, top_n: int = 20) -> go.Figure:
    if df_post.empty:
        return figura_vacia("Sin clientes post-login para el filtro seleccionado")

    monto_col = "valorlempirizado" if "valorlempirizado" in df_post.columns else "valor"
    if monto_col not in df_post.columns:
        df_post[monto_col] = 0

    resumen = (
        df_post.groupby("padded_codigo_cliente", as_index=False)
        .agg(
            total_eventos=("fecha", "count"),
            primer_login_q1=("primer_login_q1", "min"),
            primer_evento_post=("fecha", "min"),
            ultimo_evento_post=("fecha", "max"),
            monto_total=(monto_col, "sum"),
        )
        .sort_values("total_eventos", ascending=False)
        .head(top_n)
    )

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=["Cliente", "Eventos", "Primer login Q1", "Primer post-login", "Último post-login", "Monto total (L)"],
                    fill_color=COLORES["azul_experto"],
                    font=dict(color=COLORES["blanco"], size=12),
                    align="left",
                ),
                cells=dict(
                    values=[
                        resumen["padded_codigo_cliente"].astype(str).tolist(),
                        [f"{int(v):,}" for v in resumen["total_eventos"].tolist()],
                        resumen["primer_login_q1"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                        resumen["primer_evento_post"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                        resumen["ultimo_evento_post"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                        [f"{float(v):,.2f}" for v in resumen["monto_total"].fillna(0).tolist()],
                    ],
                    fill_color=COLORES["blanco"],
                    font=dict(color=COLORES["azul_experto"], size=11),
                    align="left",
                ),
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} clientes por actividad post-login",
        margin=dict(t=48, b=10, l=10, r=10),
        paper_bgcolor=COLORES["blanco"],
    )
    return fig


def construir_layout(df_post: pd.DataFrame, fuente_base: str) -> html.Div:
    canales = sorted(df_post["canal"].dropna().astype(str).unique().tolist()) if not df_post.empty else []
    opciones_canal = [{"label": "Todos", "value": VALOR_TODOS}] + [{"label": c, "value": c} for c in canales]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("QBR1 2026 - Comportamiento Post Login", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                f"Eventos de clientes del Excel después de su primer login en Q1 2026. Base: {fuente_base}",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "22px"},
            ),
            html.Div(
                style={"maxWidth": "300px", "marginBottom": "22px"},
                children=[
                    html.Label("Filtro de canal del evento", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                    dcc.Dropdown(id="filtro-canal-post", options=opciones_canal, value=VALOR_TODOS, clearable=False),
                ],
            ),
            html.Div(
                id="kpis-contenedor",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr", "gap": "30px"},
                children=[
                    dcc.Graph(id="g-post-mes", style={"width": "100%"}),
                    dcc.Graph(id="g-clientes-post-mes", style={"width": "100%"}),
                    dcc.Graph(id="g-modulo-post", style={"width": "100%"}),
                    dcc.Graph(id="g-operacion-post", style={"width": "100%"}),
                    dcc.Graph(id="g-top-clientes-post", style={"width": "100%"}),
                ],
            ),
        ],
    )


def construir_app(df_base: pd.DataFrame, df_logins: pd.DataFrame, df_post: pd.DataFrame, fuente_base: str) -> Dash:
    app = Dash(__name__)
    app.layout = construir_layout(df_post, fuente_base)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-post-mes", "figure"),
        Output("g-clientes-post-mes", "figure"),
        Output("g-modulo-post", "figure"),
        Output("g-operacion-post", "figure"),
        Output("g-top-clientes-post", "figure"),
        Input("filtro-canal-post", "value"),
    )
    def actualizar(canal: str):
        post_filtrado = filtrar_por_canal(df_post, canal)
        return (
            construir_kpis(df_base, df_logins, post_filtrado),
            grafico_post_mes(post_filtrado),
            grafico_clientes_post_mes(post_filtrado),
            grafico_modulo_post(post_filtrado),
            grafico_operacion_post(post_filtrado),
            tabla_top_clientes_post(post_filtrado),
        )

    return app


def main() -> None:
    print(f"Cargando eventos desde: {RUTA_QUERY_EVENTOS}")
    try:
        df_base, df_logins, df_post, fuente_base = preparar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos del dashboard: {exc}")
        raise SystemExit(1) from exc

    print(f"Fuente base clientes: {fuente_base}")
    print(f"Clientes base Q1: {df_base['padded_codigo_cliente'].nunique():,}")
    print(f"Clientes con login Q1: {df_logins['padded_codigo_cliente'].nunique():,}")
    print(f"Eventos post-login Q1: {len(df_post):,}")
    print(f"Clientes con actividad post-login Q1: {df_post['padded_codigo_cliente'].nunique():,}")

    app = construir_app(df_base, df_logins, df_post, fuente_base)
    print("Dashboard corriendo en http://127.0.0.1:8068")
    app.run(debug=True, use_reloader=False, port=8068)


if __name__ == "__main__":
    main()
