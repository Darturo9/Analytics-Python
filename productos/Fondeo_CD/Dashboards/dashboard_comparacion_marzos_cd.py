"""
dashboard_comparacion_marzos_cd.py
------------------------------------
Dashboard de comparacion interanual de marzo: 2024, 2025 y 2026.
Cohorte: cuentas abiertas en marzo de cada año vs fondeadas ese mismo mes.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_comparacion_marzos_cd.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/ComparacionMarzos.sql"


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
                hovertemplate="Año: %{x}<br>Cuentas abiertas: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="Cuentas fondeadas",
                x=df["mes"].tolist(),
                y=df["cuentas_fondeadas"].tolist(),
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in df["cuentas_fondeadas"].tolist()],
                textposition="outside",
                hovertemplate="Año: %{x}<br>Cuentas fondeadas: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Cuentas abiertas vs fondeadas en marzo — comparacion interanual",
        barmode="group",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=40, r=20),
        xaxis=dict(title="Año"),
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
                hovertemplate="Año: %{x}<br>Tasa de activacion: %{y:.1f}%<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Tasa de activacion en marzo — % de cuentas fondeadas en el mismo mes de apertura",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=50, r=20),
        xaxis=dict(title="Año"),
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
                text=[f"{v:,}" for v in df["cuentas_fondeadas"].tolist()],
                textposition="inside",
                hovertemplate="Año: %{x}<br>Fondeadas: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="Sin fondear",
                x=df["mes"].tolist(),
                y=df["cuentas_sin_fondear"].tolist(),
                marker_color=COLORES["gris_texto"],
                text=[f"{v:,}" for v in df["cuentas_sin_fondear"].tolist()],
                textposition="inside",
                hovertemplate="Año: %{x}<br>Sin fondear: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Composicion de cuentas abiertas en marzo: fondeadas vs sin fondear",
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=40, l=40, r=20),
        xaxis=dict(title="Año"),
        yaxis=dict(title="Cantidad de cuentas"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    mejor_idx = df["tasa_activacion"].idxmax() if not df.empty else 0
    mejor_mes = df.loc[mejor_idx, "mes"] if not df.empty else "-"
    mejor_tasa = float(df["tasa_activacion"].max()) if not df.empty else 0.0

    variacion_abiertas = ""
    variacion_fondeadas = ""
    if len(df) >= 2:
        ult = df.iloc[-1]
        ant = df.iloc[-2]
        delta_ab = ult["cuentas_abiertas"] - ant["cuentas_abiertas"]
        delta_fo = ult["cuentas_fondeadas"] - ant["cuentas_fondeadas"]
        variacion_abiertas = f"{'+' if delta_ab >= 0 else ''}{delta_ab:,} vs año anterior"
        variacion_fondeadas = f"{'+' if delta_fo >= 0 else ''}{delta_fo:,} vs año anterior"

    ultimo = df.iloc[-1] if not df.empty else None

    kpis = [
        kpi_card("Cuentas abiertas (Marzo 2026)", f"{int(ultimo['cuentas_abiertas']):,}" if ultimo is not None else "-", COLORES["azul_experto"]),
        kpi_card("Cuentas fondeadas (Marzo 2026)", f"{int(ultimo['cuentas_fondeadas']):,}" if ultimo is not None else "-", COLORES["amarillo_opt"]),
        kpi_card("Tasa activacion (Marzo 2026)", f"{float(ultimo['tasa_activacion']):.1f}%" if ultimo is not None else "-", COLORES["aqua_digital"]),
        kpi_card("Mejor tasa historica", f"{mejor_mes} ({mejor_tasa:.1f}%)", COLORES["azul_financiero"]),
        kpi_card("Variacion aperturas", variacion_abiertas or "-", COLORES["azul_experto"]),
        kpi_card("Variacion fondeadas", variacion_fondeadas or "-", COLORES["aqua_digital"]),
    ]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Comparacion Interanual Marzo — Fondeo Cuenta Digital",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Cohorte: cuentas abiertas en marzo de cada año vs fondeadas ese mismo mes | 2024 — 2025 — 2026",
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

    print(f"Años cargados: {len(df)}")
    print(df[["mes", "cuentas_abiertas", "cuentas_fondeadas", "tasa_activacion"]].to_string(index=False))

    app = Dash(__name__)
    app.layout = construir_layout(df)
    print("Dashboard corriendo en http://127.0.0.1:8059")
    app.run(debug=True, use_reloader=False, port=8059)


if __name__ == "__main__":
    main()
