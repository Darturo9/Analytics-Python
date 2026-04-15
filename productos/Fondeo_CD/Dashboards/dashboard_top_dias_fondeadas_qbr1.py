"""
dashboard_top_dias_fondeadas_qbr1.py
------------------------------------
Dashboard QBR1 con los dias de Q1 2026 que tuvieron mas cuentas fondeadas.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_top_dias_fondeadas_qbr1.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH_DIAS = "productos/Fondeo_CD/Queries/TopDiasCuentasFondeadasQ1.sql"
QUERY_PATH_DEPTOS = "productos/Fondeo_CD/Queries/TopDeptosFondeoQ1.sql"
TOP_N = 15
TOP_DEPTOS = 3


def cargar_top_dias() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH_DIAS)
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce")
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df["ranking_dia"] = pd.to_numeric(df.get("ranking_dia"), errors="coerce").fillna(0).astype(int)

    df = df[df["fecha"].notna()].copy()
    df = df.sort_values(["cuentas_fondeadas", "fecha"], ascending=[False, True]).reset_index(drop=True)
    return df


def cargar_top_deptos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH_DEPTOS)
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["depto"] = df.get("depto", pd.Series(dtype="string")).astype(str).str.strip()
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df["ranking_depto"] = pd.to_numeric(df.get("ranking_depto"), errors="coerce").fillna(0).astype(int)
    df = df[df["depto"] != ""].copy()
    df = df.sort_values(["cuentas_fondeadas", "depto"], ascending=[False, True]).reset_index(drop=True)
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


def grafico_top_dias(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    top = df.head(TOP_N).sort_values(["cuentas_fondeadas", "fecha"], ascending=[True, False])
    etiquetas = top["fecha"].dt.strftime("%Y-%m-%d").tolist()
    valores = top["cuentas_fondeadas"].tolist()

    fig = go.Figure(
        data=[
            go.Bar(
                y=etiquetas,
                x=valores,
                orientation="h",
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in valores],
                textposition="outside",
                hovertemplate="Fecha: %{y}<br>Cuentas fondeadas: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {TOP_N} dias con mas cuentas fondeadas (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=120, r=20),
        xaxis=dict(title="Cuentas fondeadas"),
        yaxis=dict(title="Fecha"),
    )
    return fig


def grafico_top_deptos(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos de departamentos disponibles")

    top = df.head(TOP_DEPTOS).sort_values(["cuentas_fondeadas", "depto"], ascending=[True, False])
    etiquetas = top["depto"].tolist()
    valores = top["cuentas_fondeadas"].tolist()

    fig = go.Figure(
        data=[
            go.Bar(
                y=etiquetas,
                x=valores,
                orientation="h",
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in valores],
                textposition="outside",
                hovertemplate="Departamento: %{y}<br>Cuentas fondeadas: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {TOP_DEPTOS} departamentos con mas cuentas fondeadas (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=140, r=20),
        xaxis=dict(title="Cuentas fondeadas"),
        yaxis=dict(title="Departamento"),
    )
    return fig


def construir_layout(df_dias: pd.DataFrame, df_deptos: pd.DataFrame) -> html.Div:
    dias_con_fondeo = len(df_dias)
    promedio_diario = float(df_dias["cuentas_fondeadas"].mean()) if not df_dias.empty else 0.0
    mejor = df_dias.iloc[0] if not df_dias.empty else None
    mejor_fecha = mejor["fecha"].strftime("%Y-%m-%d") if mejor is not None else "-"
    mejor_valor = int(mejor["cuentas_fondeadas"]) if mejor is not None else 0
    depto_lider = df_deptos.iloc[0]["depto"] if not df_deptos.empty else "-"

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "QBR1 - Dias con mas cuentas fondeadas",
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
                            html.P("Dias con fondeo en Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
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
                            html.P("Pico de cuentas fondeadas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{mejor_fecha}: {mejor_valor:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                            html.P("Promedio diario", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(f"{promedio_diario:,.1f}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                            html.P("Depto lider de fondeo", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                            html.H2(str(depto_lider), style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                        ],
                    ),
                ],
            ),
            dcc.Graph(figure=grafico_top_dias(df_dias), style={"width": "100%"}),
            dcc.Graph(figure=grafico_top_deptos(df_deptos), style={"width": "100%"}),
        ],
    )


def main() -> None:
    print(f"Cargando datos de dias desde: {QUERY_PATH_DIAS}")
    print(f"Cargando datos de deptos desde: {QUERY_PATH_DEPTOS}")
    try:
        df_dias = cargar_top_dias()
        df_deptos = cargar_top_deptos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    print(f"Dias cargados: {len(df_dias)}")
    print(f"Deptos cargados: {len(df_deptos)}")
    app = Dash(__name__)
    app.layout = construir_layout(df_dias, df_deptos)
    print("Dashboard corriendo en http://127.0.0.1:8071")
    app.run(debug=True, use_reloader=False, port=8071)


if __name__ == "__main__":
    main()
