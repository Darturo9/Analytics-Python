"""
dashboard_monto_promedio_fondeado_qbr1.py
-----------------------------------------
Dashboard QBR1 para monto fondeado promedio por cuenta.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_monto_promedio_fondeado_qbr1.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/MontoPromedioFondeadoPorCuentaQ1.sql"
TOP_N = 15


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce")
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df["monto_total_fondeado"] = pd.to_numeric(df.get("monto_total_fondeado"), errors="coerce").fillna(0.0)
    df["monto_promedio_por_cuenta"] = pd.to_numeric(
        df.get("monto_promedio_por_cuenta"), errors="coerce"
    ).fillna(0.0)

    df = df[df["fecha"].notna()].sort_values("fecha").reset_index(drop=True)
    return df


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


def grafico_evolucion_promedio(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["fecha"].dt.strftime("%Y-%m-%d").tolist(),
                y=df["monto_promedio_por_cuenta"].tolist(),
                mode="lines+markers",
                line=dict(color=COLORES["azul_financiero"], width=3),
                marker=dict(size=6, color=COLORES["aqua_digital"]),
                hovertemplate="Fecha: %{x}<br>Monto promedio/cuenta: L %{y:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Evolucion diaria del monto fondeado promedio por cuenta (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=70, l=40, r=20),
        xaxis=dict(title="Fecha", tickangle=-45),
        yaxis=dict(title="Monto promedio por cuenta (L)"),
    )
    return fig


def grafico_top_promedio(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    top = df.sort_values(["monto_promedio_por_cuenta", "fecha"], ascending=[False, True]).head(TOP_N)
    top = top.sort_values(["monto_promedio_por_cuenta", "fecha"], ascending=[True, False])

    fig = go.Figure(
        data=[
            go.Bar(
                y=top["fecha"].dt.strftime("%Y-%m-%d").tolist(),
                x=top["monto_promedio_por_cuenta"].tolist(),
                orientation="h",
                marker_color=COLORES["amarillo_opt"],
                text=[f"L {v:,.2f}" for v in top["monto_promedio_por_cuenta"].tolist()],
                textposition="outside",
                hovertemplate="Fecha: %{y}<br>Monto promedio/cuenta: L %{x:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {TOP_N} dias por monto promedio fondeado por cuenta",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=120, r=20),
        xaxis=dict(title="Monto promedio por cuenta (L)"),
        yaxis=dict(title="Fecha"),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    dias_con_fondeo = len(df)
    promedio_q1 = float(df["monto_promedio_por_cuenta"].mean()) if not df.empty else 0.0
    pico = (
        df.sort_values(["monto_promedio_por_cuenta", "fecha"], ascending=[False, True]).head(1)
        if not df.empty
        else pd.DataFrame()
    )
    pico_fecha = pico.iloc[0]["fecha"].strftime("%Y-%m-%d") if not pico.empty else "-"
    pico_valor = float(pico.iloc[0]["monto_promedio_por_cuenta"]) if not pico.empty else 0.0

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "QBR1 - Monto fondeado promedio por cuenta",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Universo: cuentas de Cuenta Digital abiertas en Q1 2026 con fondeo diario (saldo > 0).",
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
                            html.P("Dias con fondeo", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{dias_con_fondeo:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                            html.P("Promedio diario Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"L {promedio_q1:,.2f}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                    html.Div(
                        style={
                            "backgroundColor": COLORES["blanco"],
                            "borderRadius": "10px",
                            "padding": "12px 14px",
                            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                            "borderTop": f"4px solid {COLORES['amarillo_opt']}",
                        },
                        children=[
                            html.P("Pico promedio diario", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{pico_fecha}: L {pico_valor:,.2f}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                ],
            ),
            dcc.Graph(figure=grafico_evolucion_promedio(df), style={"width": "100%"}),
            dcc.Graph(figure=grafico_top_promedio(df), style={"width": "100%"}),
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

    print(f"Dias cargados: {len(df)}")
    app = Dash(__name__)
    app.layout = construir_layout(df)
    print("Dashboard corriendo en http://127.0.0.1:8072")
    app.run(debug=True, use_reloader=False, port=8072)


if __name__ == "__main__":
    main()
