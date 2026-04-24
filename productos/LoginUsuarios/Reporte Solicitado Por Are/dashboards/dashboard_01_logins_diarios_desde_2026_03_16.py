"""
dashboard_01_logins_diarios_desde_2026_03_16.py
------------------------------------------------
Primer dashboard (Reporte Solicitado Por Are):
- Grafico diario de cantidad de logins desde 2026-03-16.
- Dias con envio de campana se muestran con color distinto.

Ejecucion:
    python3 "productos/LoginUsuarios/Reporte Solicitado Por Are/dashboards/dashboard_01_logins_diarios_desde_2026_03_16.py"
"""

from __future__ import annotations

import json
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
RUTA_QUERY_CAMPANAS = (
    BASE_REPORTE / "queries" / "campanas_arbol_sin_login_72049_desde_2026_03_16.sql"
)

FECHA_INICIO = pd.Timestamp("2026-03-16")
FECHA_FIN_EXCLUSIVA = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
PORT = 8062

COLOR_NORMAL = COLORES["aqua_digital"]
COLOR_CAMPANA = "#D62828"

SQL_LOGINS = """
WITH clientes AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM([value])), 8) AS padded_codigo_cliente
    FROM OPENJSON(CAST(:codigos_json AS NVARCHAR(MAX)))
    WHERE [value] IS NOT NULL
      AND LTRIM(RTRIM([value])) <> ''
),
base AS (
    SELECT
        CAST(st.dw_fecha_trx AS DATE) AS fecha,
        RIGHT('00000000' + RTRIM(LTRIM(st.clccli)), 8) AS padded_codigo_usuario
    FROM dw_bel_IBSTTRA_VIEW st
    INNER JOIN clientes c
        ON RIGHT('00000000' + RTRIM(LTRIM(st.clccli)), 8) = c.padded_codigo_cliente
    WHERE st.dw_fecha_trx >= :fecha_inicio
      AND st.dw_fecha_trx < :fecha_fin_exclusiva
      AND st.secode IN ('app-login', 'web-login', 'login')
      AND st.clccli IS NOT NULL
)
SELECT
    b.fecha,
    COUNT(*) AS total_logins_dia,
    COUNT(DISTINCT b.padded_codigo_usuario) AS clientes_unicos_login_dia,
    t.total_logins_periodo,
    t.clientes_con_login_periodo
FROM base b
CROSS JOIN (
    SELECT
        COUNT(*) AS total_logins_periodo,
        COUNT(DISTINCT padded_codigo_usuario) AS clientes_con_login_periodo
    FROM base
) t
GROUP BY
    b.fecha,
    t.total_logins_periodo,
    t.clientes_con_login_periodo
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


def cargar_campanas() -> pd.DataFrame:
    if not RUTA_QUERY_CAMPANAS.exists():
        raise FileNotFoundError(f"No existe query de campanas: {RUTA_QUERY_CAMPANAS}")

    df = run_query_file(str(RUTA_QUERY_CAMPANAS))
    if df.empty:
        return pd.DataFrame(columns=["fecha_campana", "campanas"])

    cols = {str(c).strip().lower(): c for c in df.columns}
    if "name" not in cols or "start_date" not in cols:
        raise ValueError(
            "La query de campanas debe devolver name y start_date. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    out = df.rename(columns={cols["name"]: "name", cols["start_date"]: "start_date"}).copy()
    out["name"] = out["name"].fillna("SIN NOMBRE").astype(str).str.strip()
    out["fecha_campana"] = pd.to_datetime(out["start_date"], errors="coerce").dt.normalize()
    out = out[out["fecha_campana"].notna()].copy()

    out = (
        out.groupby("fecha_campana", as_index=False)["name"]
        .agg(lambda s: " | ".join(sorted(set(v for v in s if v))))
        .rename(columns={"name": "campanas"})
    )
    return out


def cargar_logins_diario_resumen(
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
        return pd.DataFrame(columns=["fecha", "total_logins_dia", "clientes_unicos_login_dia"]), 0, 0

    params = {
        "codigos_json": json.dumps(codigos_validos),
        "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
        "fecha_fin_exclusiva": fecha_fin_exclusiva.strftime("%Y-%m-%d"),
    }
    df = run_query(SQL_LOGINS, params=params)
    if df.empty:
        return pd.DataFrame(columns=["fecha", "total_logins_dia", "clientes_unicos_login_dia"]), 0, 0

    out = df.copy()
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.normalize()
    out["total_logins_dia"] = pd.to_numeric(out["total_logins_dia"], errors="coerce").fillna(0).astype(int)
    out["clientes_unicos_login_dia"] = (
        pd.to_numeric(out["clientes_unicos_login_dia"], errors="coerce").fillna(0).astype(int)
    )
    out = out[out["fecha"].notna()].copy()

    total_logins_periodo = int(pd.to_numeric(out["total_logins_periodo"], errors="coerce").fillna(0).max())
    clientes_con_login_periodo = int(
        pd.to_numeric(out["clientes_con_login_periodo"], errors="coerce").fillna(0).max()
    )

    return (
        out[["fecha", "total_logins_dia", "clientes_unicos_login_dia"]],
        total_logins_periodo,
        clientes_con_login_periodo,
    )


def construir_base_diaria(
    total_clientes_base: int,
    logins_diario: pd.DataFrame,
    campanas: pd.DataFrame,
    fecha_inicio: pd.Timestamp,
    fecha_fin_exclusiva: pd.Timestamp,
    total_logins_periodo: int,
    clientes_con_login_periodo: int,
) -> tuple[pd.DataFrame, int, int, int]:
    fechas = pd.date_range(fecha_inicio, fecha_fin_exclusiva - pd.Timedelta(days=1), freq="D")

    if logins_diario.empty:
        total_logins_dia = pd.Series(0, index=fechas)
        clientes_unicos_dia = pd.Series(0, index=fechas)
    else:
        diario_idx = logins_diario.set_index("fecha")
        total_logins_dia = diario_idx["total_logins_dia"].reindex(fechas, fill_value=0)
        clientes_unicos_dia = diario_idx["clientes_unicos_login_dia"].reindex(fechas, fill_value=0)

    mapa_campanas = {
        row["fecha_campana"]: row["campanas"]
        for _, row in campanas.iterrows()
    }

    diario = pd.DataFrame({"fecha": fechas})
    diario["total_logins"] = total_logins_dia.values
    diario["clientes_unicos_login"] = clientes_unicos_dia.values
    diario["campanas"] = diario["fecha"].map(mapa_campanas).fillna("Sin envio")
    diario["es_dia_campana"] = diario["fecha"].isin(set(mapa_campanas.keys()))
    diario["color"] = diario["es_dia_campana"].map({True: COLOR_CAMPANA, False: COLOR_NORMAL})

    return diario, total_clientes_base, total_logins_periodo, clientes_con_login_periodo


def construir_figura_logins_diarios(diario: pd.DataFrame) -> go.Figure:
    if diario.empty:
        return figura_vacia("Sin datos para construir el grafico")

    max_total = int(diario["total_logins"].max()) if not diario.empty else 0
    separacion = max(1, int(max_total * 0.04))

    custom_data = list(
        zip(
            diario["es_dia_campana"].map({True: "Si", False: "No"}),
            diario["campanas"],
            diario["clientes_unicos_login"],
        )
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=diario["fecha"],
            y=diario["total_logins"],
            marker_color=diario["color"],
            customdata=custom_data,
            text=[f"{int(v):,}" if int(v) > 0 else "" for v in diario["total_logins"]],
            textposition="outside",
            hovertemplate=(
                "Fecha %{x|%Y-%m-%d}<br>"
                "Logins: %{y:,}<br>"
                "Clientes unicos login: %{customdata[2]:,}<br>"
                "Dia con campana: %{customdata[0]}<br>"
                "Campana(s): %{customdata[1]}<extra></extra>"
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
        yaxis=dict(title="Cantidad de logins", range=[0, max_total + (separacion * 3)]),
        bargap=0.15,
    )

    fig.add_annotation(
        x=0.01,
        y=1.12,
        xref="paper",
        yref="paper",
        text=(
            f"<b>Color campana:</b> {COLOR_CAMPANA} (dias con envio) | "
            f"<b>Color normal:</b> {COLOR_NORMAL}"
        ),
        showarrow=False,
        font=dict(size=12, color=COLORES["gris_texto"]),
        align="left",
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
    campanas = cargar_campanas()
    codigos_clientes = clientes["padded_codigo_cliente"].dropna().astype(str).tolist()
    logins_diario, total_logins_periodo, clientes_con_login_periodo = cargar_logins_diario_resumen(
        codigos_clientes=codigos_clientes,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
    )
    total_base = int(clientes["padded_codigo_cliente"].nunique())

    diario, total_base, total_logins, clientes_con_login = construir_base_diaria(
        total_clientes_base=total_base,
        logins_diario=logins_diario,
        campanas=campanas,
        fecha_inicio=FECHA_INICIO,
        fecha_fin_exclusiva=FECHA_FIN_EXCLUSIVA,
        total_logins_periodo=total_logins_periodo,
        clientes_con_login_periodo=clientes_con_login_periodo,
    )

    dias_campana = int(diario["es_dia_campana"].sum()) if not diario.empty else 0

    fig = construir_figura_logins_diarios(diario)

    app = Dash(__name__)
    app.title = "Dashboard 01 - Logins diarios"

    app.layout = html.Div(
        style={
            "backgroundColor": COLORES["gris_fondo"],
            "minHeight": "100vh",
            "padding": "16px",
            "fontFamily": "Arial, sans-serif",
        },
        children=[
            html.H2(
                "Dashboard 01: Logins Diarios (desde 2026-03-16)",
                style={"color": COLORES["azul_experto"], "margin": "0 0 8px 0"},
            ),
            html.P(
                (
                    f"Periodo: {FECHA_INICIO.strftime('%Y-%m-%d')} a "
                    f"{(FECHA_FIN_EXCLUSIVA - pd.Timedelta(days=1)).strftime('%Y-%m-%d')} "
                    f"(dias con campana detectados: {dias_campana})"
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
                    kpi_card("Total logins", f"{total_logins:,}", COLORES["aqua_digital"]),
                    kpi_card("Clientes con login", f"{clientes_con_login:,}", COLORES["amarillo_opt"]),
                ],
            ),
            html.Div(
                style={
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px",
                    "padding": "10px",
                    "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                },
                children=[dcc.Graph(id="graf-logins-diarios", figure=fig)],
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

    print(f"Dashboard 01 corriendo en http://127.0.0.1:{PORT}")
    app.run(debug=False, use_reloader=False, host="127.0.0.1", port=PORT)


if __name__ == "__main__":
    main()
