"""
dashboard_qbr1_arbol_rtm.py
----------------------
Dashboard Login Usuarios - Q1 2026 (Arbol RTM).

Universo:
- Clientes del archivo Excel Arbol RTM.
- Logins ocurridos en marzo 2026.

Ejecucion:
    python3 productos/LoginUsuarios/QBR_1_2026/dashboards/dashboard_qbr1_arbol_rtm.py
"""

import sys
import time

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


RUTA_QUERY_LOGINS = "productos/LoginUsuarios/QBR_1_2026/queries/Logins_Marzo.sql"
RUTA_EXCEL_BASE = Path("productos/LoginUsuarios/QBR_1_2026/ArchivosExcel")
VALOR_TODOS = "__TODOS__"
FECHA_INICIO_MARZO = pd.Timestamp("2026-03-01")
FECHA_FIN_MARZO = pd.Timestamp("2026-04-01")
MESES_MOSTRAR = ["2026-03"]

MESES_ES = {
    "2026-03": "Marzo 2026",
}
MENSAJES_ERROR_TRANSITORIO = ("08S01", "10054", "communication link failure", "tcp provider")


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_canal_login(valor: str) -> str:
    canal = str(valor).strip().lower()
    if canal == "app-login":
        return "App"
    if canal == "web-login":
        return "Web"
    if canal == "login":
        return "Web Legacy"
    return "Otro"


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
        if normalizada in {"cif", "cldoc", "codigo", "cliente", "clientes"}:
            return original
    return None


def cargar_base_clientes() -> tuple[pd.DataFrame, str]:
    """Carga base exclusivamente desde Excel."""
    archivo = None
    preferidos = [
        RUTA_EXCEL_BASE / "ArbolRTM.xlsx",
        RUTA_EXCEL_BASE / "ArbolRTM.xls",
        RUTA_EXCEL_BASE / "ArbolRTM.csv",
        RUTA_EXCEL_BASE / "arbolrtm.xlsx",
        RUTA_EXCEL_BASE / "arbolrtm.xls",
        RUTA_EXCEL_BASE / "arbolrtm.csv",
        RUTA_EXCEL_BASE / "Arbol RTM.xlsx",
        RUTA_EXCEL_BASE / "Arbol RTM.xls",
        RUTA_EXCEL_BASE / "Arbol RTM.csv",
        RUTA_EXCEL_BASE / "Arbol_RTM.xlsx",
        RUTA_EXCEL_BASE / "Arbol_RTM.xls",
        RUTA_EXCEL_BASE / "Arbol_RTM.csv",
        RUTA_EXCEL_BASE / "arbol rtm.xlsx",
        RUTA_EXCEL_BASE / "arbol rtm.xls",
        RUTA_EXCEL_BASE / "arbol rtm.csv",
        RUTA_EXCEL_BASE / "arbol_rtm.xlsx",
        RUTA_EXCEL_BASE / "arbol_rtm.xls",
        RUTA_EXCEL_BASE / "arbol_rtm.csv",
    ]

    for candidato in preferidos:
        if candidato.exists():
            archivo = candidato
            break

    if archivo is None and RUTA_EXCEL_BASE.exists():
        encontrados = sorted(
            p for p in RUTA_EXCEL_BASE.iterdir()
            if p.is_file() and "arbol" in p.stem.lower() and "rtm" in p.stem.lower() and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
        )
        if encontrados:
            archivo = encontrados[0]

    if archivo is not None:
        if archivo.suffix.lower() in {".xlsx", ".xls"}:
            df_base = pd.read_excel(archivo)
        else:
            df_base = pd.read_csv(archivo)

        col_codigo = detectar_columna_codigo(df_base.columns.tolist())
        if col_codigo is None:
            raise ValueError(
                f"No se encontró columna de código cliente/usuario en {archivo.name}. "
                "Incluye una columna como CIF, codigo_cliente o similar."
            )

        df_base["padded_codigo_cliente"] = df_base[col_codigo].apply(normalizar_codigo)
        df_base = df_base[df_base["padded_codigo_cliente"].notna()].drop_duplicates("padded_codigo_cliente")
        return df_base[["padded_codigo_cliente"]].copy(), f"Excel: {archivo}"

    raise FileNotFoundError(
        "No se encontró archivo base en productos/LoginUsuarios/QBR_1_2026/ArchivosExcel. "
        "Agrega ArbolRTM.xlsx (o .xls/.csv) para ejecutar el dashboard."
    )


def es_error_transitorio_sql(exc: Exception) -> bool:
    texto = str(exc).lower()
    return any(token in texto for token in MENSAJES_ERROR_TRANSITORIO)


def cargar_logins_con_reintento(intentos: int = 3, espera_seg: int = 2) -> pd.DataFrame:
    ultimo_error = None
    for intento in range(1, intentos + 1):
        try:
            return run_query_file(RUTA_QUERY_LOGINS)
        except SQLAlchemyError as exc:
            ultimo_error = exc
            if not es_error_transitorio_sql(exc) or intento == intentos:
                raise
            print(
                f"[WARN] Falla transitoria de conexión SQL (intento {intento}/{intentos}). "
                f"Reintentando en {espera_seg}s..."
            )
            time.sleep(espera_seg)
    raise ultimo_error if ultimo_error else RuntimeError("No se pudo cargar la query de logins.")


def cargar_datos() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    df_base, fuente_base = cargar_base_clientes()
    df_logins = cargar_logins_con_reintento()

    df_logins.columns = [str(c) for c in df_logins.columns]

    df_logins["padded_codigo_usuario"] = df_logins["padded_codigo_usuario"].apply(normalizar_codigo)
    df_logins["fecha_inicio"] = pd.to_datetime(df_logins["fecha_inicio"], errors="coerce")
    df_logins = df_logins[
        df_logins["padded_codigo_usuario"].notna() & df_logins["fecha_inicio"].notna()
    ].copy()
    df_logins = df_logins[
        (df_logins["fecha_inicio"] >= FECHA_INICIO_MARZO)
        & (df_logins["fecha_inicio"] < FECHA_FIN_MARZO)
    ].copy()
    df_logins["canal_login_norm"] = df_logins["canal_login"].apply(normalizar_canal_login)

    universo = set(df_base["padded_codigo_cliente"].tolist())
    df_logins = df_logins[df_logins["padded_codigo_usuario"].isin(universo)].copy()

    df_logins["mes"] = df_logins["fecha_inicio"].dt.to_period("M").astype(str)
    df_logins["fecha_dia"] = df_logins["fecha_inicio"].dt.date

    return df_base, df_logins, fuente_base


def filtrar_por_canal(df: pd.DataFrame, canal: str) -> pd.DataFrame:
    if canal in (None, "", VALOR_TODOS):
        return df.copy()
    return df[df["canal_login_norm"] == canal].copy()


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


def construir_kpis(df_base: pd.DataFrame, df_logins: pd.DataFrame) -> list[html.Div]:
    clientes_base = int(df_base["padded_codigo_cliente"].nunique()) if not df_base.empty else 0
    clientes_con_login = int(df_logins["padded_codigo_usuario"].nunique()) if not df_logins.empty else 0
    total_logins = int(len(df_logins))
    tasa_activacion = (clientes_con_login / clientes_base * 100) if clientes_base > 0 else 0.0
    prom_logins = (total_logins / clientes_con_login) if clientes_con_login > 0 else 0.0

    return [
        kpi_card("Clientes base marzo", f"{clientes_base:,}", COLORES["azul_experto"]),
        kpi_card("Clientes con login", f"{clientes_con_login:,}", COLORES["aqua_digital"]),
        kpi_card("Total logins marzo", f"{total_logins:,}", COLORES["amarillo_opt"]),
        kpi_card("Tasa de activacion", f"{tasa_activacion:,.1f}%", COLORES["azul_financiero"]),
        kpi_card("Promedio logins por cliente", f"{prom_logins:,.2f}", COLORES["amarillo_emp"]),
    ]


def grafico_logins_por_mes_canal(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    pivot = (
        df_logins.groupby(["mes", "canal_login_norm"])
        .size()
        .unstack(fill_value=0)
        .reindex(MESES_MOSTRAR, fill_value=0)
    )
    etiquetas = [MESES_ES.get(m, m) for m in pivot.index.tolist()]
    totales = pivot.sum(axis=1).tolist()
    max_total = max(totales) if totales else 0
    offset = max(1, int(max_total * 0.06))

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_experto"], COLORES["azul_financiero"]]
    for idx, canal in enumerate(pivot.columns.tolist()):
        valores = pivot[canal].tolist()
        fig.add_trace(
            go.Bar(
                name=canal,
                x=etiquetas,
                y=valores,
                marker_color=colores[idx % len(colores)],
                text=[f"{v:,}" if v > 0 else "" for v in valores],
                textposition="inside",
                hovertemplate="Mes: %{x}<br>Canal: " + canal + "<br>Logins: %{y:,}<extra></extra>",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=etiquetas,
            y=[t + offset for t in totales],
            mode="text",
            text=[f"{t:,}" for t in totales],
            textposition="top center",
            showlegend=False,
            hoverinfo="skip",
            textfont=dict(color=COLORES["azul_experto"], size=12),
        )
    )

    fig.update_layout(
        title="Logins por mes y canal (marzo 2026)",
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Logins", range=[0, max_total + (offset * 3)] if max_total > 0 else None),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def grafico_clientes_unicos_mes(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    resumen = (
        df_logins.groupby("mes")["padded_codigo_usuario"]
        .nunique()
        .reindex(MESES_MOSTRAR, fill_value=0)
    )
    etiquetas = [MESES_ES.get(m, m) for m in resumen.index.tolist()]
    valores = resumen.values.tolist()

    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=valores,
                marker_color=COLORES["azul_financiero"],
                text=[f"{v:,}" for v in valores],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Clientes unicos: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes unicos con login por mes",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Clientes unicos"),
    )
    return fig


def grafico_primer_login_mes(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    primer_login = (
        df_logins.groupby("padded_codigo_usuario", as_index=False)["fecha_inicio"]
        .min()
        .rename(columns={"fecha_inicio": "primer_login"})
    )
    primer_login["mes"] = primer_login["primer_login"].dt.to_period("M").astype(str)

    resumen = (
        primer_login.groupby("mes")["padded_codigo_usuario"]
        .nunique()
        .reindex(MESES_MOSTRAR, fill_value=0)
    )
    etiquetas = [MESES_ES.get(m, m) for m in resumen.index.tolist()]
    valores = resumen.values.tolist()

    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=valores,
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in valores],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Clientes (primer login): %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes por mes de primer login (sin duplicar clientes)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Clientes"),
    )
    return fig


def grafico_logins_por_canal(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    resumen = df_logins["canal_login_norm"].value_counts().sort_values(ascending=True)

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen.index.tolist(),
                x=resumen.values.tolist(),
                orientation="h",
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Canal: %{y}<br>Logins: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Logins por canal (marzo 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=140, r=20),
        xaxis=dict(title="Logins"),
        yaxis=dict(title=""),
    )
    return fig


def grafico_primer_login_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    primer_login = (
        df_logins.groupby("padded_codigo_usuario", as_index=False)["fecha_inicio"]
        .min()
        .rename(columns={"fecha_inicio": "primer_login"})
    )
    primer_login["dia"] = primer_login["primer_login"].dt.date
    resumen = primer_login.groupby("dia").size().reset_index(name="clientes")

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen["dia"].astype(str).tolist(),
                y=resumen["clientes"].tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in resumen["clientes"].tolist()],
                textposition="outside",
                hovertemplate="Fecha: %{x}<br>Primer login (clientes): %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes por fecha de primer login (marzo 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=70, l=40, r=20),
        xaxis=dict(title="Fecha", tickangle=-45),
        yaxis=dict(title="Clientes"),
    )
    return fig


def tabla_top_clientes(df_logins: pd.DataFrame, top_n: int = 20) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("Sin logins para el filtro seleccionado")

    def usuario_representativo(s: pd.Series) -> str:
        ss = s.fillna("").astype(str).str.strip()
        ss = ss[ss != ""]
        return ss.value_counts().index[0] if not ss.empty else ""

    resumen = (
        df_logins.groupby("padded_codigo_usuario", as_index=False)
        .agg(
            total_logins=("fecha_inicio", "count"),
            primer_login=("fecha_inicio", "min"),
            ultimo_login=("fecha_inicio", "max"),
            usuario=("nombre_usuario", usuario_representativo),
            canal_principal=("canal_login_norm", usuario_representativo),
        )
        .sort_values("total_logins", ascending=False)
        .head(top_n)
    )

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=["Cliente", "Usuario", "Canal", "Total logins", "Primer login", "Ultimo login"],
                    fill_color=COLORES["azul_experto"],
                    font=dict(color=COLORES["blanco"], size=12),
                    align="left",
                ),
                cells=dict(
                    values=[
                        resumen["padded_codigo_usuario"].astype(str).tolist(),
                        resumen["usuario"].astype(str).tolist(),
                        resumen["canal_principal"].astype(str).tolist(),
                        [f"{int(v):,}" for v in resumen["total_logins"].tolist()],
                        resumen["primer_login"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                        resumen["ultimo_login"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                    ],
                    fill_color=COLORES["blanco"],
                    font=dict(color=COLORES["azul_experto"], size=11),
                    align="left",
                ),
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} clientes por total de logins",
        margin=dict(t=48, b=10, l=10, r=10),
        paper_bgcolor=COLORES["blanco"],
    )
    return fig


def construir_layout(df_logins: pd.DataFrame) -> html.Div:
    canales = sorted(df_logins["canal_login_norm"].dropna().unique().tolist()) if not df_logins.empty else []
    opciones_canal = [{"label": "Todos", "value": VALOR_TODOS}] + [
        {"label": c, "value": c} for c in canales
    ]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("Login Usuarios - Marzo 2026 (Arbol RTM)", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                "Medicion de logins para clientes del archivo Arbol RTM durante marzo 2026.",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "22px"},
            ),
            html.Div(
                style={"maxWidth": "260px", "marginBottom": "22px"},
                children=[
                    html.Label("Filtro de canal", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                    dcc.Dropdown(id="filtro-canal", options=opciones_canal, value=VALOR_TODOS, clearable=False),
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
                    dcc.Graph(id="g-logins-mes-canal", style={"width": "100%"}),
                    dcc.Graph(id="g-clientes-mes", style={"width": "100%"}),
                    dcc.Graph(id="g-primer-login-mes", style={"width": "100%"}),
                    dcc.Graph(id="g-logins-canal", style={"width": "100%"}),
                    dcc.Graph(id="g-primer-login-dia", style={"width": "100%"}),
                    dcc.Graph(id="g-top-clientes", style={"width": "100%"}),
                ],
            ),
        ],
    )


def construir_app(df_base: pd.DataFrame, df_logins: pd.DataFrame) -> Dash:
    app = Dash(__name__)
    app.layout = construir_layout(df_logins)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-logins-mes-canal", "figure"),
        Output("g-clientes-mes", "figure"),
        Output("g-primer-login-mes", "figure"),
        Output("g-logins-canal", "figure"),
        Output("g-primer-login-dia", "figure"),
        Output("g-top-clientes", "figure"),
        Input("filtro-canal", "value"),
    )
    def actualizar_vista(canal: str):
        filtrado = filtrar_por_canal(df_logins, canal)
        return (
            construir_kpis(df_base, filtrado),
            grafico_logins_por_mes_canal(filtrado),
            grafico_clientes_unicos_mes(filtrado),
            grafico_primer_login_mes(filtrado),
            grafico_logins_por_canal(filtrado),
            grafico_primer_login_dia(filtrado),
            tabla_top_clientes(filtrado),
        )

    return app


def main() -> None:
    print(f"Cargando logins desde: {RUTA_QUERY_LOGINS}")
    try:
        df_base, df_logins, fuente_base = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos del dashboard: {exc}")
        raise SystemExit(1) from exc

    print(f"Fuente base clientes: {fuente_base}")
    print(f"Clientes base marzo: {df_base['padded_codigo_cliente'].nunique():,}")
    print(f"Logins marzo (filtrados por base): {len(df_logins):,}")
    print(f"Clientes con login marzo: {df_logins['padded_codigo_usuario'].nunique():,}")

    app = construir_app(df_base, df_logins)
    print("Dashboard corriendo en http://127.0.0.1:8067")
    app.run(debug=True, use_reloader=False, port=8067)


if __name__ == "__main__":
    main()
