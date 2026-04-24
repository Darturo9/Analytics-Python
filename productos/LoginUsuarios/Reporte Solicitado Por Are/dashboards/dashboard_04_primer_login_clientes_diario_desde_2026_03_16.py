"""
dashboard_04_primer_login_clientes_diario_desde_2026_03_16.py
-------------------------------------------------------------
Dashboard 04 (Reporte Solicitado Por Are):
- Grafico diario de primer login por cliente desde 2026-03-16.
- Si un cliente hace varios logins, solo cuenta su primer login del periodo.

Ejecucion:
    python3 "productos/LoginUsuarios/Reporte Solicitado Por Are/dashboards/dashboard_04_primer_login_clientes_diario_desde_2026_03_16.py"
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
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
PORT = 8065
COLOR_BARRA = COLORES["azul_financiero"]
PLACEHOLDER_CLIENTES_VALUES = "{{CLIENTES_VALUES}}"

SQL_PRIMER_LOGIN_DIARIO = """
WITH clientes AS (
    SELECT DISTINCT
        v.padded_codigo_cliente
    FROM (VALUES
        {{CLIENTES_VALUES}}
    ) v(padded_codigo_cliente)
),
base AS (
    SELECT
        st.dw_fecha_trx AS fecha_login,
        RIGHT('00000000' + RTRIM(LTRIM(st.clccli)), 8) AS padded_codigo_usuario
    FROM dw_bel_IBSTTRA_VIEW st
    INNER JOIN clientes c
        ON RIGHT('00000000' + RTRIM(LTRIM(st.clccli)), 8) = c.padded_codigo_cliente
    WHERE st.dw_fecha_trx >= :fecha_inicio
      AND st.dw_fecha_trx < :fecha_fin_exclusiva
      AND st.secode IN ('app-login', 'web-login', 'login')
      AND st.clccli IS NOT NULL
),
primer_login AS (
    SELECT
        padded_codigo_usuario,
        CAST(MIN(fecha_login) AS DATE) AS fecha_primer_login
    FROM base
    GROUP BY padded_codigo_usuario
)
SELECT
    p.fecha_primer_login AS fecha,
    COUNT(*) AS clientes_primer_login_dia,
    t.clientes_con_primer_login_periodo,
    t.total_eventos_login_periodo
FROM primer_login p
CROSS JOIN (
    SELECT
        COUNT(*) AS clientes_con_primer_login_periodo,
        (SELECT COUNT(*) FROM base) AS total_eventos_login_periodo
    FROM primer_login
) t
GROUP BY
    p.fecha_primer_login,
    t.clientes_con_primer_login_periodo,
    t.total_eventos_login_periodo
ORDER BY p.fecha_primer_login;
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


def cargar_primer_login_diario(
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
        return pd.DataFrame(columns=["fecha", "clientes_primer_login_dia"]), 0, 0

    values_sql = ",\n        ".join(f"('{codigo}')" for codigo in codigos_validos)
    if PLACEHOLDER_CLIENTES_VALUES not in SQL_PRIMER_LOGIN_DIARIO:
        raise ValueError(f"La query no contiene placeholder {PLACEHOLDER_CLIENTES_VALUES}.")
    query_sql = SQL_PRIMER_LOGIN_DIARIO.replace(PLACEHOLDER_CLIENTES_VALUES, values_sql)

    params = {
        "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
        "fecha_fin_exclusiva": fecha_fin_exclusiva.strftime("%Y-%m-%d"),
    }
    df = run_query(query_sql, params=params)
    if df.empty:
        return pd.DataFrame(columns=["fecha", "clientes_primer_login_dia"]), 0, 0

    out = df.copy()
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.normalize()
    out["clientes_primer_login_dia"] = (
        pd.to_numeric(out["clientes_primer_login_dia"], errors="coerce").fillna(0).astype(int)
    )
    out = out[out["fecha"].notna()].copy()

    clientes_con_primer_login = int(
        pd.to_numeric(out["clientes_con_primer_login_periodo"], errors="coerce").fillna(0).max()
    )
    total_eventos_login = int(pd.to_numeric(out["total_eventos_login_periodo"], errors="coerce").fillna(0).max())

    return out[["fecha", "clientes_primer_login_dia"]], clientes_con_primer_login, total_eventos_login


def construir_base_diaria(
    primer_login_diario: pd.DataFrame,
    fecha_inicio: pd.Timestamp,
    fecha_fin_exclusiva: pd.Timestamp,
) -> pd.DataFrame:
    fechas = pd.date_range(fecha_inicio, fecha_fin_exclusiva - pd.Timedelta(days=1), freq="D")
    if primer_login_diario.empty:
        primer_login = pd.Series(0, index=fechas)
    else:
        diario_idx = primer_login_diario.set_index("fecha")
        primer_login = diario_idx["clientes_primer_login_dia"].reindex(fechas, fill_value=0)

    diario = pd.DataFrame({"fecha": fechas})
    diario["clientes_primer_login"] = primer_login.values
    return diario


def construir_figura(diario: pd.DataFrame) -> go.Figure:
    if diario.empty:
        return figura_vacia("Sin datos para construir el grafico")

    max_total = int(diario["clientes_primer_login"].max()) if not diario.empty else 0
    separacion = max(1, int(max_total * 0.04))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=diario["fecha"],
            y=diario["clientes_primer_login"],
            marker_color=COLOR_BARRA,
            text=[f"{int(v):,}" if int(v) > 0 else "" for v in diario["clientes_primer_login"]],
            textposition="outside",
            hovertemplate=(
                "Fecha %{x|%Y-%m-%d}<br>"
                "Clientes con primer login: %{y:,}<extra></extra>"
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
        yaxis=dict(title="Clientes con primer login", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
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

    primer_login_diario, clientes_con_primer_login, total_eventos_login = cargar_primer_login_diario(
        codigos_clientes=codigos_clientes,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
    )

    diario = construir_base_diaria(
        primer_login_diario=primer_login_diario,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
    )

    total_base = int(clientes["padded_codigo_cliente"].nunique())
    fig = construir_figura(diario)

    app = Dash(__name__)
    app.title = "Dashboard 04 - Primer login cliente"

    app.layout = html.Div(
        style={
            "backgroundColor": COLORES["gris_fondo"],
            "minHeight": "100vh",
            "padding": "16px",
            "fontFamily": "Arial, sans-serif",
        },
        children=[
            html.H2(
                "Dashboard 04: Primer Login por Cliente (Diario)",
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
                    kpi_card("Clientes con primer login", f"{clientes_con_primer_login:,}", COLORES["azul_financiero"]),
                    kpi_card("Total eventos login", f"{total_eventos_login:,}", COLORES["amarillo_opt"]),
                ],
            ),
            html.Div(
                style={
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px",
                    "padding": "10px",
                    "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                },
                children=[dcc.Graph(id="graf-primer-login-diario", figure=fig)],
            ),
        ],
    )

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

    print(f"Dashboard 04 corriendo en http://127.0.0.1:{PORT}")
    app.run(debug=False, use_reloader=False, host="127.0.0.1", port=PORT)


if __name__ == "__main__":
    main()
