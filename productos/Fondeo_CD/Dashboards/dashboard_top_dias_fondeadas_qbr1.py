"""
dashboard_top_dias_fondeadas_qbr1.py
------------------------------------
Dashboard QBR1 con cuentas fondeadas unicas por semana en Q1 2026.

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


QUERY_PATH_SEMANAS = "productos/Fondeo_CD/Queries/TopDiasCuentasFondeadasQ1.sql"
QUERY_PATH_DEPTOS = "productos/Fondeo_CD/Queries/TopDeptosFondeoQ1.sql"
QUERY_PATH_PROMEDIO = "productos/Fondeo_CD/Queries/MontoPromedioFondeadoPorCuentaQ1.sql"
TOP_DEPTOS = 3


def cargar_fondeo_semanal() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH_SEMANAS)
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["semana_inicio"] = pd.to_datetime(df.get("semana_inicio"), errors="coerce")
    df["semana_fin"] = pd.to_datetime(df.get("semana_fin"), errors="coerce")
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    df["orden_semana"] = pd.to_numeric(df.get("orden_semana"), errors="coerce").fillna(0).astype(int)

    df = df[df["semana_inicio"].notna()].copy()
    df = df.sort_values("semana_inicio").reset_index(drop=True)
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


def cargar_monto_promedio() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH_PROMEDIO)
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


def grafico_fondeo_semanal(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos disponibles")

    etiquetas = [
        f"Sem {i + 1}\n{s.strftime('%d-%b')} a {f.strftime('%d-%b')}"
        for i, (s, f) in enumerate(zip(df["semana_inicio"], df["semana_fin"]))
    ]
    valores = df["cuentas_fondeadas"].tolist()

    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=valores,
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in valores],
                textposition="outside",
                hovertemplate="Semana: %{x}<br>Cuentas fondeadas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas fondeadas unicas por semana (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=80, l=40, r=20),
        xaxis=dict(title="Semana", tickangle=-20),
        yaxis=dict(title="Cuentas fondeadas"),
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


def grafico_monto_promedio_q1(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos de monto promedio disponibles")

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["fecha"].dt.strftime("%Y-%m-%d").tolist(),
                y=df["monto_promedio_por_cuenta"].tolist(),
                mode="lines+markers",
                line=dict(color=COLORES["azul_financiero"], width=3),
                marker=dict(size=6, color=COLORES["aqua_digital"]),
                hovertemplate="Fecha: %{x}<br>Monto promedio por cuenta: L %{y:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Evolucion diaria del monto promedio por cuenta (Q1 2026)",
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
        return figura_vacia("Sin datos de monto promedio disponibles")

    top = df.sort_values(["monto_promedio_por_cuenta", "fecha"], ascending=[False, True]).head(10)
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
                hovertemplate="Fecha: %{y}<br>Monto promedio por cuenta: L %{x:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Top 10 dias por monto promedio por cuenta (Q1 2026)",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=120, r=20),
        xaxis=dict(title="Monto promedio por cuenta (L)"),
        yaxis=dict(title="Fecha"),
    )
    return fig


def construir_layout(df_semanas: pd.DataFrame, df_deptos: pd.DataFrame, df_promedio: pd.DataFrame) -> html.Div:
    semanas_con_fondeo = len(df_semanas)
    promedio_semanal = float(df_semanas["cuentas_fondeadas"].mean()) if not df_semanas.empty else 0.0
    mejor = (
        df_semanas.sort_values(["cuentas_fondeadas", "semana_inicio"], ascending=[False, True]).head(1)
        if not df_semanas.empty
        else pd.DataFrame()
    )
    mejor_rango = (
        f"{mejor.iloc[0]['semana_inicio'].strftime('%Y-%m-%d')} a {mejor.iloc[0]['semana_fin'].strftime('%Y-%m-%d')}"
        if not mejor.empty
        else "-"
    )
    mejor_valor = int(mejor.iloc[0]["cuentas_fondeadas"]) if not mejor.empty else 0
    depto_lider = df_deptos.iloc[0]["depto"] if not df_deptos.empty else "-"

    promedio_q1_monto = float(df_promedio["monto_promedio_por_cuenta"].mean()) if not df_promedio.empty else 0.0
    pico_monto = (
        df_promedio.sort_values(["monto_promedio_por_cuenta", "fecha"], ascending=[False, True]).head(1)
        if not df_promedio.empty
        else pd.DataFrame()
    )
    pico_monto_fecha = pico_monto.iloc[0]["fecha"].strftime("%Y-%m-%d") if not pico_monto.empty else "-"
    pico_monto_valor = float(pico_monto.iloc[0]["monto_promedio_por_cuenta"]) if not pico_monto.empty else 0.0

    kpis_semana = html.Div(
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
                    html.P("Semanas con fondeo en Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                    html.H2(f"{semanas_con_fondeo:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                    html.P("Pico semanal de fondeo", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                    html.H2(f"{mejor_rango}: {mejor_valor:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                    html.P("Promedio semanal", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                    html.H2(f"{promedio_semanal:,.1f}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
    )

    kpis_monto = html.Div(
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
                    html.P("Promedio Q1", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                    html.H2(f"L {promedio_q1_monto:,.2f}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
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
                    html.P("Pico diario de promedio", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
                    html.H2(
                        f"{pico_monto_fecha}: L {pico_monto_valor:,.2f}",
                        style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]},
                    ),
                ],
            ),
        ],
    )

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "QBR1 - Fondeo Cuenta Digital",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Universo: cuentas de Cuenta Digital abiertas en Q1 2026.",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "18px"},
            ),
            dcc.Tabs(
                value="tab-semanal",
                children=[
                    dcc.Tab(
                        label="Cuentas fondeadas por semana",
                        value="tab-semanal",
                        children=[
                            html.Div(style={"paddingTop": "16px"}, children=[
                                kpis_semana,
                                dcc.Graph(figure=grafico_fondeo_semanal(df_semanas), style={"width": "100%"}),
                                dcc.Graph(figure=grafico_top_deptos(df_deptos), style={"width": "100%"}),
                            ])
                        ],
                    ),
                    dcc.Tab(
                        label="Monto promedio por cuenta (Q1)",
                        value="tab-monto",
                        children=[
                            html.Div(style={"paddingTop": "16px"}, children=[
                                kpis_monto,
                                dcc.Graph(figure=grafico_monto_promedio_q1(df_promedio), style={"width": "100%"}),
                                dcc.Graph(figure=grafico_top_promedio(df_promedio), style={"width": "100%"}),
                            ])
                        ],
                    ),
                ],
            ),
        ],
    )


def main() -> None:
    print(f"Cargando datos semanales desde: {QUERY_PATH_SEMANAS}")
    print(f"Cargando datos de deptos desde: {QUERY_PATH_DEPTOS}")
    print(f"Cargando datos de monto promedio desde: {QUERY_PATH_PROMEDIO}")
    try:
        df_semanas = cargar_fondeo_semanal()
        df_deptos = cargar_top_deptos()
        df_promedio = cargar_monto_promedio()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    print(f"Semanas cargadas: {len(df_semanas)}")
    print(f"Deptos cargados: {len(df_deptos)}")
    print(f"Dias con promedio cargados: {len(df_promedio)}")
    app = Dash(__name__)
    app.layout = construir_layout(df_semanas, df_deptos, df_promedio)
    print("Dashboard corriendo en http://127.0.0.1:8071")
    app.run(debug=True, use_reloader=False, port=8071)


if __name__ == "__main__":
    main()
