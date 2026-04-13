"""
dashboard_general_marzo.py
--------------------------
Dashboard LoginUsuarios - Marzo 2026 (enfoque general, sin canal).

Ejecucion:
    python3 productos/LoginUsuarios/2026-03/dashboards/dashboard_general_marzo.py
"""

import calendar
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


RUTA_QUERY_LOGINS = "productos/LoginUsuarios/2026-03/queries/Logins.sql"
RUTA_DATA_CONTACTADOS = Path("productos/LoginUsuarios/2026-03/archivoExcel")
TOP_CLIENTES = 20


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
        if normalizada in {"cldoc", "codigo", "cliente", "clientes", "cif"}:
            return original
    return None


def cargar_contactados() -> tuple[pd.DataFrame, str]:
    archivos_preferidos = [
        RUTA_DATA_CONTACTADOS / "Contactados_Marzo.xlsx",
        RUTA_DATA_CONTACTADOS / "contactados_marzo.xlsx",
        RUTA_DATA_CONTACTADOS / "Contactados_Marzo.csv",
        RUTA_DATA_CONTACTADOS / "contactados_marzo.csv",
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
            p
            for p in RUTA_DATA_CONTACTADOS.iterdir()
            if p.is_file() and "contact" in p.name.lower() and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
        )
        if encontrados:
            archivo = encontrados[0]

    if archivo is None:
        raise FileNotFoundError(
            "No se encontró archivo de contactados en "
            "productos/LoginUsuarios/2026-03/archivoExcel."
        )

    if archivo.suffix.lower() in {".xlsx", ".xls"}:
        df_contactados = pd.read_excel(archivo)
    else:
        df_contactados = pd.read_csv(archivo)

    col_codigo = detectar_columna_codigo(df_contactados.columns.tolist())
    if col_codigo is None:
        raise ValueError(
            f"No se encontró columna de cliente en {archivo.name}. "
            "Debe incluir una columna como Cliente, Clientes, codigo_cliente o CIF."
        )

    df_contactados["padded_codigo_usuario"] = df_contactados[col_codigo].apply(normalizar_codigo)
    df_contactados = df_contactados[df_contactados["padded_codigo_usuario"].notna()].copy()
    df_contactados = df_contactados.drop_duplicates(subset=["padded_codigo_usuario"])

    return df_contactados[["padded_codigo_usuario"]].copy(), str(archivo)


def cargar_logins() -> pd.DataFrame:
    df = run_query_file(RUTA_QUERY_LOGINS)
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
    df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
    df["id_usuario"] = df["nombre_usuario"].fillna("").astype(str).str.strip()
    df.loc[df["id_usuario"] == "", "id_usuario"] = df["padded_codigo_usuario"]

    df = df[df["fecha_inicio"].notna() & df["padded_codigo_usuario"].notna()].copy()
    df["dia"] = df["fecha_inicio"].dt.day
    return df


def figura_vacia(mensaje: str) -> go.Figure:
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


def _dias_del_mes(df_logins: pd.DataFrame) -> list[int]:
    if df_logins.empty:
        return list(range(1, 32))
    anio = int(df_logins["fecha_inicio"].dt.year.mode().iloc[0])
    mes = int(df_logins["fecha_inicio"].dt.month.mode().iloc[0])
    ultimo_dia = calendar.monthrange(anio, mes)[1]
    return list(range(1, ultimo_dia + 1))


def grafico_eventos_por_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("No hay logins para clientes del Excel en marzo 2026")

    dias = _dias_del_mes(df_logins)
    resumen = df_logins.groupby("dia").size().reindex(dias, fill_value=0)

    fig = go.Figure(
        data=[
            go.Bar(
                x=dias,
                y=resumen.values.tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" if v > 0 else "" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Día %{x}<br>Eventos: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Eventos de login por día (clientes del Excel)",
        height=520,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1),
        yaxis=dict(title="Eventos"),
    )
    return fig


def grafico_clientes_unicos_por_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("No hay clientes con login para el período")

    dias = _dias_del_mes(df_logins)
    df_unicos = df_logins[["dia", "padded_codigo_usuario"]].drop_duplicates()
    resumen = df_unicos.groupby("dia").size().reindex(dias, fill_value=0)

    fig = go.Figure(
        data=[
            go.Bar(
                x=dias,
                y=resumen.values.tolist(),
                marker_color=COLORES["azul_financiero"],
                text=[f"{v:,}" if v > 0 else "" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Día %{x}<br>Clientes únicos: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes únicos con login por día",
        height=520,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1),
        yaxis=dict(title="Clientes"),
    )
    return fig


def grafico_primer_login_por_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("No hay datos de primer login para el período")

    primer_login = (
        df_logins.groupby("padded_codigo_usuario", as_index=False)["fecha_inicio"]
        .min()
        .rename(columns={"fecha_inicio": "primer_login"})
    )
    primer_login["dia_primer_login"] = primer_login["primer_login"].dt.day

    dias = _dias_del_mes(df_logins)
    resumen = primer_login.groupby("dia_primer_login").size().reindex(dias, fill_value=0)

    fig = go.Figure(
        data=[
            go.Bar(
                x=dias,
                y=resumen.values.tolist(),
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" if v > 0 else "" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Día %{x}<br>Clientes (primer login): %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes por fecha de primer login",
        height=520,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=10),
        xaxis=dict(title="Día", tickmode="linear", tick0=1, dtick=1),
        yaxis=dict(title="Clientes"),
    )
    return fig


def tabla_top_clientes(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return figura_vacia("No hay datos para tabla de clientes")

    resumen = (
        df_logins.groupby("padded_codigo_usuario", as_index=False)
        .agg(
            total_logins=("fecha_inicio", "count"),
            primer_login=("fecha_inicio", "min"),
            ultimo_login=("fecha_inicio", "max"),
            usuario=("id_usuario", lambda s: s.value_counts().index[0] if not s.empty else ""),
        )
        .sort_values("total_logins", ascending=False)
        .head(TOP_CLIENTES)
    )

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=["Cliente", "Usuario", "Total logins", "Primer login", "Último login"],
                    fill_color=COLORES["azul_experto"],
                    font=dict(color=COLORES["blanco"], size=12),
                    align="left",
                ),
                cells=dict(
                    values=[
                        resumen["padded_codigo_usuario"].astype(str).tolist(),
                        resumen["usuario"].astype(str).tolist(),
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
        title=f"Top {TOP_CLIENTES} clientes por total de logins",
        margin=dict(t=48, b=10, l=10, r=10),
        paper_bgcolor=COLORES["blanco"],
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


def construir_app(df_excel: pd.DataFrame, df_logins_filtrados: pd.DataFrame, ruta_excel: str) -> Dash:
    total_eventos = int(len(df_logins_filtrados))
    clientes_excel = int(df_excel["padded_codigo_usuario"].nunique()) if not df_excel.empty else 0
    clientes_con_login = int(df_logins_filtrados["padded_codigo_usuario"].nunique()) if not df_logins_filtrados.empty else 0
    tasa_match = (clientes_con_login / clientes_excel * 100) if clientes_excel > 0 else 0.0
    prom_logins = (total_eventos / clientes_con_login) if clientes_con_login > 0 else 0.0

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("LoginUsuarios - Marzo 2026 (General)", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                f"Clientes evaluados desde Excel (sin segmentación por canal). Archivo: {ruta_excel}",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "22px"},
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
                children=[
                    kpi_card("Clientes en Excel", f"{clientes_excel:,}", COLORES["azul_experto"]),
                    kpi_card("Clientes con login", f"{clientes_con_login:,}", COLORES["aqua_digital"]),
                    kpi_card("Eventos de login", f"{total_eventos:,}", COLORES["amarillo_opt"]),
                    kpi_card("Cobertura sobre Excel", f"{tasa_match:,.1f}%", COLORES["azul_financiero"]),
                    kpi_card("Promedio logins por cliente", f"{prom_logins:,.2f}", COLORES["amarillo_emp"]),
                ],
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr", "gap": "30px"},
                children=[
                    dcc.Graph(figure=grafico_eventos_por_dia(df_logins_filtrados)),
                    dcc.Graph(figure=grafico_clientes_unicos_por_dia(df_logins_filtrados)),
                    dcc.Graph(figure=grafico_primer_login_por_dia(df_logins_filtrados)),
                    dcc.Graph(figure=tabla_top_clientes(df_logins_filtrados)),
                ],
            ),
        ],
    )
    return app


def main() -> None:
    print(f"Cargando logins desde: {RUTA_QUERY_LOGINS}")
    try:
        df_excel, ruta_excel = cargar_contactados()
        df_logins = cargar_logins()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    df_filtrado = df_logins.merge(
        df_excel,
        how="inner",
        on="padded_codigo_usuario",
    )

    print(f"Archivo Excel base: {ruta_excel}")
    print(f"Clientes en Excel: {df_excel['padded_codigo_usuario'].nunique():,}")
    print(f"Eventos login (marzo) para clientes Excel: {len(df_filtrado):,}")
    print(f"Clientes Excel con login: {df_filtrado['padded_codigo_usuario'].nunique():,}")

    app = construir_app(df_excel, df_filtrado, ruta_excel)
    print("Dashboard general corriendo en http://127.0.0.1:8066")
    app.run(debug=True, use_reloader=False, port=8066)


if __name__ == "__main__":
    main()
