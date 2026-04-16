"""
dashboard_movimiento_abril_2026_1a15.py
---------------------------------------
Dashboard (grafico de pastel) de cuentas con movimiento vs sin movimiento
para cuentas de Cuenta Digital creadas del 1 al 15 de abril 2026.

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/dashboard/dashboard_movimiento_abril_2026_1a15.py
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
from core.db import run_query_file


QUERY_PATH = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "2026-04"
    / "queries"
    / "AnalisisFondeoDemograficoAbril2026_1a15.sql"
)


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(QUERY_PATH))
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "numero_cuenta" not in df.columns or "con_movimiento" not in df.columns:
        raise ValueError("La query debe devolver 'numero_cuenta' y 'con_movimiento'.")

    df["numero_cuenta"] = df["numero_cuenta"].astype(str).str.strip()
    df["con_movimiento"] = pd.to_numeric(df["con_movimiento"], errors="coerce").fillna(0).astype(int)
    df = df.drop_duplicates(subset=["numero_cuenta"]).reset_index(drop=True)
    return df


def construir_resumen(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    con_mov = int((df["con_movimiento"] == 1).sum())
    sin_mov = total - con_mov

    resumen = pd.DataFrame(
        {
            "categoria": ["Con movimiento", "Sin movimiento"],
            "cuentas": [con_mov, sin_mov],
        }
    )
    resumen["porcentaje"] = (
        resumen["cuentas"] / total * 100.0 if total > 0 else 0.0
    )
    return resumen


def grafico_pastel(resumen: pd.DataFrame) -> go.Figure:
    if resumen["cuentas"].sum() == 0:
        fig = go.Figure()
        fig.update_layout(
            plot_bgcolor=COLORES["blanco"],
            paper_bgcolor=COLORES["blanco"],
            annotations=[
                dict(
                    text="Sin datos para mostrar",
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

    fig = go.Figure(
        data=[
            go.Pie(
                labels=resumen["categoria"],
                values=resumen["cuentas"],
                hole=0.45,
                marker=dict(colors=[COLORES["aqua_digital"], COLORES["gris_texto"]]),
                text=[f"{int(v):,}" for v in resumen["cuentas"]],
                textinfo="label+percent",
                hovertemplate="%{label}<br>Cuentas: %{value:,}<br>Participacion: %{percent}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas con movimiento vs sin movimiento (1 al 15 de abril 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=60, b=20, l=20, r=20),
        legend_title_text="",
    )
    return fig


def construir_layout(resumen: pd.DataFrame) -> html.Div:
    total = int(resumen["cuentas"].sum())
    con_mov = int(resumen.loc[resumen["categoria"] == "Con movimiento", "cuentas"].sum())
    pct_con_mov = (con_mov / total * 100.0) if total > 0 else 0.0

    card_style = {
        "backgroundColor": COLORES["blanco"],
        "borderRadius": "10px",
        "padding": "12px 14px",
        "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
    }

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Movimiento de Cuentas - Abril 2026 (1 al 15)",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Universo: cuentas de Cuenta Digital creadas del 1 al 15 de abril 2026.",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "18px"},
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "20px",
                },
                children=[
                    html.Div(
                        style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"},
                        children=[
                            html.P("Total cuentas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{total:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                    html.Div(
                        style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"},
                        children=[
                            html.P("Cuentas con movimiento", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{con_mov:,}", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
                        ],
                    ),
                    html.Div(
                        style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"},
                        children=[
                            html.P("% con movimiento", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{pct_con_mov:.2f}%", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
                        ],
                    ),
                ],
            ),
            dcc.Graph(figure=grafico_pastel(resumen), style={"width": "100%"}),
        ],
    )


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")
    try:
        df = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    resumen = construir_resumen(df)
    print("Resumen movimiento:")
    print(resumen.to_string(index=False))

    app = Dash(__name__)
    app.layout = construir_layout(resumen)
    print("Dashboard corriendo en http://127.0.0.1:8064")
    app.run(debug=False, use_reloader=False, port=8064)


if __name__ == "__main__":
    main()
