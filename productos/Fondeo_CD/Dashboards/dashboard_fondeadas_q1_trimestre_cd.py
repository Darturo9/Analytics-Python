"""
dashboard_fondeadas_q1_trimestre_cd.py
--------------------------------------
Dashboard trimestral de cuentas fondeadas unicas (Q1 2026).

Regla de primer fondeo trimestral:
Cada cuenta cuenta una sola vez en Q1, asignada al primer mes donde fondeo.
Si fondea en enero y tambien en marzo, solo cuenta en enero.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_fondeadas_q1_trimestre_cd.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/FondeadasUnicasTrimestreQ1.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c) for c in df.columns]
    df["orden"] = pd.to_numeric(df.get("orden"), errors="coerce").fillna(0).astype(int)
    df["cuentas_abiertas_q1"] = pd.to_numeric(df.get("cuentas_abiertas_q1"), errors="coerce").fillna(0).astype(int)
    df["cuentas_primer_fondeo_mes"] = (
        pd.to_numeric(df.get("cuentas_primer_fondeo_mes"), errors="coerce").fillna(0).astype(int)
    )
    return df.sort_values("orden").reset_index(drop=True)


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


def grafico_mensual_primer_fondeo(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Bar(
                x=df["mes"].tolist(),
                y=df["cuentas_primer_fondeo_mes"].tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in df["cuentas_primer_fondeo_mes"].tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Cuentas (primer fondeo): %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas por primer mes de fondeo (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Cantidad de cuentas"),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    abiertas = int(df["cuentas_abiertas_q1"].max()) if not df.empty else 0
    fondeadas = int(df["cuentas_primer_fondeo_mes"].sum()) if not df.empty else 0
    tasa = round((fondeadas / abiertas * 100), 1) if abiertas > 0 else 0.0

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Fondeo Cuenta Digital - Q1 2026 (Primer mes de fondeo)",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Cada cuenta se asigna una sola vez al primer mes donde fondeo (ene/feb/mar).",
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
                            "borderTop": f"4px solid {COLORES['azul_experto']}",
                        },
                        children=[
                            html.P("Cuentas abiertas Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{abiertas:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                    html.Div(
                        style={
                            "backgroundColor": COLORES["blanco"],
                            "borderRadius": "10px",
                            "padding": "12px 14px",
                            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                            "borderTop": f"4px solid {COLORES['aqua_digital']}",
                        },
                        children=[
                            html.P("Fondeadas unicas Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{fondeadas:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                            html.P("Tasa activacion Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{tasa:.1f}%", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                ],
            ),
            dcc.Graph(figure=grafico_mensual_primer_fondeo(df), style={"width": "100%"}),
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

    print(df.to_string(index=False))

    app = Dash(__name__)
    app.layout = construir_layout(df)
    print("Dashboard corriendo en http://127.0.0.1:8062")
    app.run(debug=True, use_reloader=False, port=8062)


if __name__ == "__main__":
    main()
