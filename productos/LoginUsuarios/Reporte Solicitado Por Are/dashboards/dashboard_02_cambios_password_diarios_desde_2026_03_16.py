"""
dashboard_02_cambios_password_diarios_desde_2026_03_16.py
---------------------------------------------------------
Segundo dashboard (Reporte Solicitado Por Are):
- Grafico diario de clientes unicos con cambio de password desde 2026-03-16.
- Incluye filtro por mes (Todos / YYYY-MM).

Ejecucion:
    python3 "productos/LoginUsuarios/Reporte Solicitado Por Are/dashboards/dashboard_02_cambios_password_diarios_desde_2026_03_16.py"
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.colors import COLORES
from core.db import run_query, run_query_file


BASE_REPORTE = PROJECT_ROOT / "productos" / "LoginUsuarios" / "Reporte Solicitado Por Are"
RUTA_QUERY_CLIENTES = (
    BASE_REPORTE / "queries" / "clientes_contactados_arbol_sin_login_72049_desde_2026_03_16.sql"
)

FECHA_INICIO = pd.Timestamp("2026-03-16")
FECHA_FIN_EXCLUSIVA = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
PORT = 8063

COLOR_NORMAL = COLORES["aqua_digital"]
PLACEHOLDER_CLIENTES_VALUES = "{{CLIENTES_VALUES}}"

SQL_CAMBIOS_PASSWORD = """
WITH clientes AS (
    SELECT DISTINCT
        v.padded_codigo_cliente
    FROM (VALUES
        {{CLIENTES_VALUES}}
    ) v(padded_codigo_cliente)
),
base AS (
    SELECT
        CAST(u.dw_fecha_cambio_pass AS DATE) AS fecha,
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBUSER u
    INNER JOIN clientes c
        ON RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) = c.padded_codigo_cliente
    WHERE u.dw_fecha_cambio_pass IS NOT NULL
      AND u.dw_fecha_cambio_pass >= :fecha_inicio
      AND u.dw_fecha_cambio_pass < :fecha_fin_exclusiva
)
SELECT
    b.fecha,
    COUNT(*) AS total_cambios_password_dia,
    COUNT(DISTINCT b.padded_codigo_cliente) AS clientes_unicos_cambio_dia,
    t.total_cambios_password_periodo,
    t.clientes_con_cambio_periodo
FROM base b
CROSS JOIN (
    SELECT
        COUNT(*) AS total_cambios_password_periodo,
        COUNT(DISTINCT padded_codigo_cliente) AS clientes_con_cambio_periodo
    FROM base
) t
GROUP BY
    b.fecha,
    t.total_cambios_password_periodo,
    t.clientes_con_cambio_periodo
ORDER BY b.fecha;
"""


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


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


def cargar_clientes_contactados() -> pd.DataFrame:
    if not RUTA_QUERY_CLIENTES.exists():
        raise FileNotFoundError(f"No existe query de clientes: {RUTA_QUERY_CLIENTES}")

    df = run_query_file(str(RUTA_QUERY_CLIENTES))
    if df.empty:
        return pd.DataFrame(columns=["padded_codigo_cliente"])

    cols = {str(c).strip().lower(): c for c in df.columns}
    if "padded_codigo_cliente" not in cols:
        raise ValueError(
            "La query de clientes contactados debe devolver padded_codigo_cliente. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    out = df.copy()
    out["padded_codigo_cliente"] = out[cols["padded_codigo_cliente"]].apply(normalizar_codigo)
    out = out[out["padded_codigo_cliente"].notna()].drop_duplicates(subset=["padded_codigo_cliente"])
    return out[["padded_codigo_cliente"]].copy()


def cargar_cambios_password_diario_resumen(
    codigos_clientes: list[str],
    fecha_inicio: pd.Timestamp,
    fecha_fin_exclusiva: pd.Timestamp,
) -> tuple[pd.DataFrame, int, int]:
    codigos_validos = sorted(
        {
            codigo
            for codigo in codigos_clientes
            if isinstance(codigo, str) and len(codigo) == 8 and codigo.isdigit()
        }
    )
    if not codigos_validos:
        return pd.DataFrame(columns=["fecha", "total_cambios_password_dia", "clientes_unicos_cambio_dia"]), 0, 0

    values_sql = ",\n        ".join(f"('{codigo}')" for codigo in codigos_validos)
    if PLACEHOLDER_CLIENTES_VALUES not in SQL_CAMBIOS_PASSWORD:
        raise ValueError(f"La query no contiene placeholder {PLACEHOLDER_CLIENTES_VALUES}.")
    query_sql = SQL_CAMBIOS_PASSWORD.replace(PLACEHOLDER_CLIENTES_VALUES, values_sql)

    params = {
        "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
        "fecha_fin_exclusiva": fecha_fin_exclusiva.strftime("%Y-%m-%d"),
    }
    df = run_query(query_sql, params=params)
    if df.empty:
        return pd.DataFrame(columns=["fecha", "total_cambios_password_dia", "clientes_unicos_cambio_dia"]), 0, 0

    out = df.copy()
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.normalize()
    out["total_cambios_password_dia"] = (
        pd.to_numeric(out["total_cambios_password_dia"], errors="coerce").fillna(0).astype(int)
    )
    out["clientes_unicos_cambio_dia"] = (
        pd.to_numeric(out["clientes_unicos_cambio_dia"], errors="coerce").fillna(0).astype(int)
    )
    out = out[out["fecha"].notna()].copy()

    total_cambios_password_periodo = int(
        pd.to_numeric(out["total_cambios_password_periodo"], errors="coerce").fillna(0).max()
    )
    clientes_con_cambio_periodo = int(
        pd.to_numeric(out["clientes_con_cambio_periodo"], errors="coerce").fillna(0).max()
    )

    return (
        out[["fecha", "total_cambios_password_dia", "clientes_unicos_cambio_dia"]],
        total_cambios_password_periodo,
        clientes_con_cambio_periodo,
    )


def construir_base_diaria(
    cambios_password_diario: pd.DataFrame,
    fecha_inicio: pd.Timestamp,
    fecha_fin_exclusiva: pd.Timestamp,
 ) -> pd.DataFrame:
    fechas = pd.date_range(fecha_inicio, fecha_fin_exclusiva - pd.Timedelta(days=1), freq="D")

    if cambios_password_diario.empty:
        total_cambios_dia = pd.Series(0, index=fechas)
        clientes_unicos_dia = pd.Series(0, index=fechas)
    else:
        diario_idx = cambios_password_diario.set_index("fecha")
        total_cambios_dia = diario_idx["total_cambios_password_dia"].reindex(fechas, fill_value=0)
        clientes_unicos_dia = diario_idx["clientes_unicos_cambio_dia"].reindex(fechas, fill_value=0)

    diario = pd.DataFrame({"fecha": fechas})
    diario["total_cambios_password"] = total_cambios_dia.values
    diario["clientes_unicos_cambio"] = clientes_unicos_dia.values
    return diario


def construir_figura_cambios_diarios(diario: pd.DataFrame) -> go.Figure:
    if diario.empty:
        return figura_vacia("Sin datos para construir el grafico")

    max_total = int(diario["clientes_unicos_cambio"].max()) if not diario.empty else 0
    separacion = max(1, int(max_total * 0.04))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=diario["fecha"],
            y=diario["clientes_unicos_cambio"],
            marker_color=COLOR_NORMAL,
            customdata=diario["total_cambios_password"],
            text=[f"{int(v):,}" if int(v) > 0 else "" for v in diario["clientes_unicos_cambio"]],
            textposition="outside",
            hovertemplate=(
                "Fecha %{x|%Y-%m-%d}<br>"
                "Clientes unicos con cambio: %{y:,}<br>"
                "Total cambios password: %{customdata:,}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    fig.update_layout(
        height=650,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=35, b=50, l=50, r=20),
        xaxis=dict(title="Fecha", tickmode="linear", dtick=86400000.0, tickformat="%d-%m"),
        yaxis=dict(title="Clientes unicos con cambio de password", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
    )
    return fig


def opciones_mes(diario: pd.DataFrame) -> list[dict[str, str]]:
    if diario.empty:
        return [{"label": "Todos", "value": "ALL"}]

    meses = sorted(diario["fecha"].dt.strftime("%Y-%m").dropna().unique().tolist())
    opciones = [{"label": "Todos", "value": "ALL"}]
    opciones.extend({"label": mes, "value": mes} for mes in meses)
    return opciones


def filtrar_diario_por_mes(diario: pd.DataFrame, mes: str | None) -> pd.DataFrame:
    if diario.empty or not mes or mes == "ALL":
        return diario.copy()
    return diario[diario["fecha"].dt.strftime("%Y-%m") == mes].copy()


def kpi_card(titulo: str, valor: str, color_borde: str) -> html.Div:
    return html.Div(
        style={
            "backgroundColor": COLORES["blanco"],
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
            "borderTop": f"4px solid {color_borde}",
            "minWidth": "220px",
        },
        children=[
            html.P(titulo, style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
            html.H2(valor, style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
        ],
    )


def construir_dashboard() -> Dash:
    clientes = cargar_clientes_contactados()
    codigos_clientes = clientes["padded_codigo_cliente"].dropna().astype(str).tolist()
    cambios_diario, total_cambios_periodo, clientes_con_cambio_periodo = cargar_cambios_password_diario_resumen(
        codigos_clientes=codigos_clientes,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
    )
    total_base = int(clientes["padded_codigo_cliente"].nunique())

    diario = construir_base_diaria(
        cambios_password_diario=cambios_diario,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
    )

    total_cambios = total_cambios_periodo
    clientes_con_cambio = clientes_con_cambio_periodo
    fig = construir_figura_cambios_diarios(diario)
    opciones_filtro_mes = opciones_mes(diario)

    app = Dash(__name__)
    app.title = "Dashboard 02 - Cambios de password diarios"

    app.layout = html.Div(
        style={
            "backgroundColor": COLORES["gris_fondo"],
            "minHeight": "100vh",
            "padding": "16px",
            "fontFamily": "Arial, sans-serif",
        },
        children=[
            html.H2(
                "Dashboard 02: Cambios de Password Diarios (desde 2026-03-16)",
                style={"color": COLORES["azul_experto"], "margin": "0 0 8px 0"},
            ),
            html.P(
                (
                    f"Periodo: {FECHA_INICIO.strftime('%Y-%m-%d')} a "
                    f"{(FECHA_FIN_EXCLUSIVA - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}"
                ),
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "14px"},
            ),
            html.Div(
                style={
                    "display": "flex",
                    "gap": "12px",
                    "flexWrap": "wrap",
                    "marginBottom": "14px",
                },
                children=[
                    kpi_card("Clientes contactados", f"{total_base:,}", COLORES["azul_experto"]),
                    kpi_card("Total cambios password", f"{total_cambios:,}", COLORES["aqua_digital"]),
                    kpi_card("Clientes con cambio", f"{clientes_con_cambio:,}", COLORES["amarillo_opt"]),
                ],
            ),
            html.Div(
                style={"marginBottom": "12px"},
                children=[
                    html.Label(
                        "Filtrar por mes",
                        htmlFor="filtro-mes-cambio-password",
                        style={"color": COLORES["azul_experto"], "fontWeight": "600", "display": "block"},
                    ),
                    dcc.Dropdown(
                        id="filtro-mes-cambio-password",
                        options=opciones_filtro_mes,
                        value="ALL",
                        clearable=False,
                        style={"maxWidth": "240px"},
                    ),
                ],
            ),
            html.P(
                id="subtitulo-filtro-mes-cambio-password",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "10px"},
            ),
            html.Div(
                style={
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px",
                    "padding": "10px",
                    "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                },
                children=[dcc.Graph(id="graf-cambios-password-diarios", figure=fig)],
            ),
        ],
    )

    @app.callback(
        Output("graf-cambios-password-diarios", "figure"),
        Output("subtitulo-filtro-mes-cambio-password", "children"),
        Input("filtro-mes-cambio-password", "value"),
    )
    def actualizar_grafico_por_mes(mes: str | None) -> tuple[go.Figure, str]:
        diario_filtrado = filtrar_diario_por_mes(diario, mes)
        figura = construir_figura_cambios_diarios(diario_filtrado)
        if mes and mes != "ALL":
            subtitulo = f"Mes seleccionado: {mes}"
        else:
            subtitulo = "Mes seleccionado: TODOS"
        return figura, subtitulo

    return app


def main() -> None:
    try:
        app = construir_dashboard()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar consulta SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc

    print(f"Dashboard 02 corriendo en http://127.0.0.1:{PORT}")
    app.run(debug=False, use_reloader=False, host="127.0.0.1", port=PORT)


if __name__ == "__main__":
    main()
