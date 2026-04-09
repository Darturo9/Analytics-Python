"""
dashboard_fondeadas_q1_cd.py
----------------------------
Dashboard simple para ver cuentas fondeadas unicas por mes (Q1 2026).
Muestra Enero, Febrero y Marzo en barras.

Regla de unicidad:
Una cuenta que fondea varias veces dentro del mismo mes cuenta solo 1 vez
(se respalda en COUNT(DISTINCT ...) de FondeadasMensualQ1.sql).
No importa en que mes del Q1 abrio la cuenta; se toma el mes donde tuvo fondeo.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_fondeadas_q1_cd.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/FondeadasMensualQ1.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c) for c in df.columns]
    df["orden"] = pd.to_numeric(df.get("orden"), errors="coerce").fillna(0).astype(int)
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df = df.sort_values("orden").reset_index(drop=True)
    return df[["mes", "orden", "cuentas_fondeadas"]]


def figura_vacia(mensaje: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
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


def grafico_fondeadas(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Bar(
                x=df["mes"].tolist(),
                y=df["cuentas_fondeadas"].tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in df["cuentas_fondeadas"].tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Cuentas fondeadas unicas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas fondeadas unicas por mes (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Cantidad de cuentas fondeadas unicas"),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    total_q1 = int(df["cuentas_fondeadas"].sum()) if not df.empty else 0
    mejor_fila = df.loc[df["cuentas_fondeadas"].idxmax()] if not df.empty else None
    mejor_mes = str(mejor_fila["mes"]) if mejor_fila is not None else "-"
    mejor_valor = int(mejor_fila["cuentas_fondeadas"]) if mejor_fila is not None else 0

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Fondeo Cuenta Digital - Q1 2026 (Unicas por mes)",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                (
                    "Enero, Febrero y Marzo 2026. Una cuenta que fondea varias veces en el mismo mes "
                    "cuenta solo una vez. No importa en que mes del Q1 abrio la cuenta."
                ),
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
                        style={
                            "backgroundColor": COLORES["blanco"],
                            "borderRadius": "10px",
                            "padding": "12px 14px",
                            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                            "borderTop": f"4px solid {COLORES['aqua_digital']}",
                        },
                        children=[
                            html.P("Total Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{total_q1:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                    html.Div(
                        style={
                            "backgroundColor": COLORES["blanco"],
                            "borderRadius": "10px",
                            "padding": "12px 14px",
                            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                            "borderTop": f"4px solid {COLORES['azul_financiero']}",
                        },
                        children=[
                            html.P("Mes con mayor fondeo", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{mejor_mes}: {mejor_valor:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                ],
            ),
            dcc.Graph(figure=grafico_fondeadas(df), style={"width": "100%"}),
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

    print(f"Meses cargados: {len(df)}")
    print(df.to_string(index=False))

    app = Dash(__name__)
    app.layout = construir_layout(df)
    print("Dashboard corriendo en http://127.0.0.1:8061")
    app.run(debug=True, use_reloader=False, port=8061)


if __name__ == "__main__":
    main()
