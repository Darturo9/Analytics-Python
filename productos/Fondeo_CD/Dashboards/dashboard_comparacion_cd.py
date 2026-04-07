"""
dashboard_comparacion_cd.py
----------------------------
Dashboard de comparacion mensual de Fondeo Cuenta Digital.
Muestra enero, febrero y marzo 2026: cuentas abiertas vs fondeadas en el mismo mes.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_comparacion_cd.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/ComparacionMensual.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c) for c in df.columns]
    df["cuentas_abiertas"] = pd.to_numeric(df.get("cuentas_abiertas"), errors="coerce").fillna(0).astype(int)
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df["tasa_activacion"] = (df["cuentas_fondeadas"] / df["cuentas_abiertas"] * 100).round(1).fillna(0.0)
    df["cuentas_sin_fondear"] = df["cuentas_abiertas"] - df["cuentas_fondeadas"]
    df = df.sort_values("orden").reset_index(drop=True)
    return df


def kpi_card(titulo: str, valor: str, color: str) -> html.Div:
    return html.Div(
        style={
            "backgroundColor": COLORES["blanco"],
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
            "borderTop": f"4px solid {color}",
        },
        children=[
            html.P(titulo, style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
            html.H2(valor, style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
        ],
    )


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
                x=0.5, y=0.5,
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(color=COLORES["gris_texto"], size=14),
            )
        ],
    )
    return fig


def grafico_barras_comparacion(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Bar(
                name="Cuentas abiertas",
                x=df["mes"].tolist(),
                y=df["cuentas_abiertas"].tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in df["cuentas_abiertas"].tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Cuentas abiertas: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="Cuentas fondeadas",
                x=df["mes"].tolist(),
                y=df["cuentas_fondeadas"].tolist(),
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in df["cuentas_fondeadas"].tolist()],
                textposition="outside",
                hovertemplate="Mes: %{x}<br>Cuentas fondeadas: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Cuentas abiertas vs fondeadas por mes",
        barmode="group",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Cantidad de cuentas"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def grafico_tasa_activacion(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["mes"].tolist(),
                y=df["tasa_activacion"].tolist(),
                mode="lines+markers+text",
                line=dict(color=COLORES["azul_experto"], width=3),
                marker=dict(size=10, color=COLORES["azul_experto"]),
                text=[f"{v:.1f}%" for v in df["tasa_activacion"].tolist()],
                textposition="top center",
                hovertemplate="Mes: %{x}<br>Tasa de activacion: %{y:.1f}%<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Tasa de activacion mensual (% cuentas fondeadas en el mismo mes de apertura)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=50, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Tasa de activacion (%)", range=[0, 110]),
    )
    return fig


def grafico_sin_fondear(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    fig = go.Figure(
        data=[
            go.Bar(
                name="Fondeadas",
                x=df["mes"].tolist(),
                y=df["cuentas_fondeadas"].tolist(),
                marker_color=COLORES["amarillo_opt"],
                hovertemplate="Mes: %{x}<br>Fondeadas: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="Sin fondear",
                x=df["mes"].tolist(),
                y=df["cuentas_sin_fondear"].tolist(),
                marker_color=COLORES["gris_texto"],
                hovertemplate="Mes: %{x}<br>Sin fondear: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Composicion de cuentas abiertas: fondeadas vs sin fondear",
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Cantidad de cuentas"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    total_abiertas = int(df["cuentas_abiertas"].sum())
    total_fondeadas = int(df["cuentas_fondeadas"].sum())
    tasa_global = round(total_fondeadas / total_abiertas * 100, 1) if total_abiertas > 0 else 0.0
    mejor_mes = df.loc[df["tasa_activacion"].idxmax(), "mes"] if not df.empty else "-"
    mejor_tasa = float(df["tasa_activacion"].max()) if not df.empty else 0.0

    kpis = [
        kpi_card("Total cuentas abiertas (Q1)", f"{total_abiertas:,}", COLORES["azul_experto"]),
        kpi_card("Total cuentas fondeadas (Q1)", f"{total_fondeadas:,}", COLORES["amarillo_opt"]),
        kpi_card("Tasa de activacion global", f"{tasa_global:.1f}%", COLORES["aqua_digital"]),
        kpi_card("Mejor mes", f"{mejor_mes} ({mejor_tasa:.1f}%)", COLORES["azul_financiero"]),
    ]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Comparacion Mensual - Fondeo Cuenta Digital 2026",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Cohorte mensual: cuentas abiertas vs fondeadas en el mismo mes de apertura | Enero - Febrero - Marzo 2026",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "24px"},
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "16px", "marginBottom": "28px"},
                children=kpis,
            ),
            html.Div(
                style={"display": "flex", "flexDirection": "column", "gap": "24px"},
                children=[
                    dcc.Graph(figure=grafico_barras_comparacion(df), style={"width": "100%"}),
                    dcc.Graph(figure=grafico_tasa_activacion(df), style={"width": "100%"}),
                    dcc.Graph(figure=grafico_sin_fondear(df), style={"width": "100%"}),
                ],
            ),
        ],
    )


def main():
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
    print(df[["mes", "cuentas_abiertas", "cuentas_fondeadas", "tasa_activacion"]].to_string(index=False))

    app = Dash(__name__)
    app.layout = construir_layout(df)
    print("Dashboard corriendo en http://127.0.0.1:8058")
    app.run(debug=True, use_reloader=False, port=8058)


if __name__ == "__main__":
    main()
