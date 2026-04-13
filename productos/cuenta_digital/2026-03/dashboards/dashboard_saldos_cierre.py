"""
dashboard_saldos_cierre.py
--------------------------
Dashboard ejecutivo de saldos de Cuenta Digital (corte 31-Mar-2026).

Muestra dos indicadores clave:
1) Saldo al cierre (saldo_ayer)
2) Saldo promedio (saldo_promedio)

Ejecucion:
    python productos/cuenta_digital/2026-03/dashboards/dashboard_saldos_cierre.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/cuenta_digital/2026-03/queries/saldo_cierre_marzo_2026.sql"


def cargar_datos() -> pd.DataFrame:
    """Carga y normaliza la base para el dashboard."""
    df = run_query_file(QUERY_PATH)
    df.columns = [str(col) for col in df.columns]

    df["cuenta"] = df.get("cuenta", "").astype(str).str.strip()
    df["moneda"] = df.get("moneda", "Sin dato").astype(str).str.strip().replace("", "Sin dato")
    df["estatus_cuenta"] = (
        df.get("estatus_cuenta", "Sin dato").astype(str).str.strip().replace("", "Sin dato")
    )

    df["transacciones"] = pd.to_numeric(df.get("transacciones"), errors="coerce").fillna(0.0)
    df["saldo_ayer"] = pd.to_numeric(df.get("saldo_ayer"), errors="coerce").fillna(0.0)
    df["saldo_promedio"] = pd.to_numeric(df.get("saldo_promedio"), errors="coerce").fillna(0.0)

    df["fecha_mov"] = pd.to_datetime(df.get("fecha_mov"), errors="coerce")
    df["fecha_informacion"] = pd.to_datetime(df.get("fecha_informacion"), errors="coerce")
    df["fecha_apertura"] = pd.to_datetime(df.get("fecha_apertura"), errors="coerce")

    # Dedupe defensivo por cuenta para evitar distorsion en KPIs y graficos.
    registros_antes = len(df)
    df = (
        df.sort_values(["cuenta", "fecha_informacion", "fecha_mov"])
        .drop_duplicates(subset=["cuenta"], keep="last")
        .reset_index(drop=True)
    )
    registros_despues = len(df)
    if registros_despues < registros_antes:
        print(f"Dedupe por cuenta aplicado: {registros_antes - registros_despues:,} registros removidos")

    return df


def filtrar_por_moneda(df: pd.DataFrame, moneda: str) -> pd.DataFrame:
    """Aplica filtro global de moneda."""
    if moneda == "Todos":
        return df.copy()
    return df[df["moneda"] == moneda].copy()


def figura_vacia(mensaje: str) -> go.Figure:
    """Crea una figura vacia con mensaje informativo."""
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


def kpi_card(titulo: str, valor: str, color_borde: str) -> html.Div:
    """Renderiza una tarjeta KPI."""
    return html.Div(
        style={
            "backgroundColor": COLORES["blanco"],
            "borderRadius": "10px",
            "padding": "12px 14px",
            "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
            "borderTop": f"4px solid {color_borde}",
        },
        children=[
            html.P(titulo, style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
            html.H2(valor, style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
        ],
    )


def construir_kpis(df: pd.DataFrame) -> list[html.Div]:
    """Construye las 5 tarjetas KPI principales."""
    cuentas = int(df["cuenta"].nunique()) if not df.empty else 0
    saldo_cierre_total = float(df["saldo_ayer"].sum()) if not df.empty else 0.0
    saldo_prom_total = float(df["saldo_promedio"].sum()) if not df.empty else 0.0
    saldo_cierre_prom = float(df["saldo_ayer"].mean()) if not df.empty else 0.0
    saldo_prom_prom = float(df["saldo_promedio"].mean()) if not df.empty else 0.0

    return [
        kpi_card("Cuentas analizadas", f"{cuentas:,}", COLORES["azul_experto"]),
        kpi_card("Saldo al cierre total", f"L {saldo_cierre_total:,.2f}", COLORES["aqua_digital"]),
        kpi_card("Saldo promedio total", f"L {saldo_prom_total:,.2f}", COLORES["amarillo_opt"]),
        kpi_card("Saldo al cierre promedio por cuenta", f"L {saldo_cierre_prom:,.2f}", COLORES["azul_financiero"]),
        kpi_card("Saldo promedio por cuenta", f"L {saldo_prom_prom:,.2f}", COLORES["azul_experto"]),
    ]


def grafico_comparativo_estatus(df: pd.DataFrame) -> go.Figure:
    """Barras agrupadas: promedio de saldos por estatus de cuenta."""
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    resumen = (
        df.groupby("estatus_cuenta", dropna=False)
        .agg(
            avg_saldo_ayer=("saldo_ayer", "mean"),
            avg_saldo_promedio=("saldo_promedio", "mean"),
            cuentas=("cuenta", "nunique"),
        )
        .reset_index()
        .sort_values("avg_saldo_ayer", ascending=False)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                name="Saldo al cierre promedio",
                x=resumen["estatus_cuenta"].tolist(),
                y=resumen["avg_saldo_ayer"].tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"L {v:,.2f}" for v in resumen["avg_saldo_ayer"].tolist()],
                textposition="outside",
                hovertemplate="Estatus: %{x}<br>Saldo al cierre prom.: L %{y:,.2f}<extra></extra>",
            ),
            go.Bar(
                name="Saldo promedio por cuenta",
                x=resumen["estatus_cuenta"].tolist(),
                y=resumen["avg_saldo_promedio"].tolist(),
                marker_color=COLORES["amarillo_opt"],
                text=[f"L {v:,.2f}" for v in resumen["avg_saldo_promedio"].tolist()],
                textposition="outside",
                hovertemplate="Estatus: %{x}<br>Saldo promedio: L %{y:,.2f}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Comparativo de saldos promedio por estatus",
        barmode="group",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Estatus de cuenta"),
        yaxis=dict(title="Monto (L)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def grafico_relacion_saldos(df: pd.DataFrame) -> go.Figure:
    """Dispersión saldo_promedio vs saldo_ayer con linea de referencia y=x."""
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    colores = [
        COLORES["aqua_digital"],
        COLORES["amarillo_opt"],
        COLORES["azul_experto"],
        COLORES["azul_financiero"],
        COLORES["amarillo_emp"],
    ]
    estatus_ordenados = sorted(df["estatus_cuenta"].dropna().unique().tolist())
    color_map = {estatus: colores[idx % len(colores)] for idx, estatus in enumerate(estatus_ordenados)}

    fig = go.Figure()
    for estatus in estatus_ordenados:
        sub = df[df["estatus_cuenta"] == estatus]
        fig.add_trace(
            go.Scatter(
                x=sub["saldo_promedio"],
                y=sub["saldo_ayer"],
                mode="markers",
                name=estatus,
                marker=dict(color=color_map[estatus], size=8, opacity=0.6),
                customdata=sub[["cuenta", "transacciones"]],
                hovertemplate=(
                    "Estatus: " + estatus
                    + "<br>Cuenta: %{customdata[0]}"
                    + "<br>Saldo promedio: L %{x:,.2f}"
                    + "<br>Saldo al cierre: L %{y:,.2f}"
                    + "<br>Transacciones: %{customdata[1]:,.0f}<extra></extra>"
                ),
            )
        )

    max_valor = float(max(df["saldo_promedio"].max(), df["saldo_ayer"].max(), 1.0))
    fig.add_shape(
        type="line",
        x0=0,
        y0=0,
        x1=max_valor,
        y1=max_valor,
        line=dict(color=COLORES["gris_texto"], width=2, dash="dash"),
    )
    fig.add_annotation(
        x=max_valor,
        y=max_valor,
        text="y = x",
        showarrow=False,
        xanchor="left",
        yanchor="bottom",
        font=dict(color=COLORES["gris_texto"], size=11),
    )

    fig.update_layout(
        title="Relacion entre saldo promedio y saldo al cierre",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=50, l=50, r=20),
        xaxis=dict(title="Saldo promedio (L)"),
        yaxis=dict(title="Saldo al cierre (L)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def tabla_top_diferencias(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """Tabla resumen de cuentas con mayor diferencia absoluta entre saldos."""
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    tabla = df.copy()
    tabla["diferencia_abs"] = (tabla["saldo_ayer"] - tabla["saldo_promedio"]).abs()
    top = tabla.sort_values("diferencia_abs", ascending=False).head(top_n)

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[
                        "Cuenta",
                        "Estatus",
                        "Moneda",
                        "Saldo al cierre",
                        "Saldo promedio",
                        "Dif. abs",
                        "Transacciones",
                    ],
                    fill_color=COLORES["azul_experto"],
                    font=dict(color=COLORES["blanco"], size=12),
                    align="left",
                ),
                cells=dict(
                    values=[
                        top["cuenta"].astype(str).tolist(),
                        top["estatus_cuenta"].astype(str).tolist(),
                        top["moneda"].astype(str).tolist(),
                        [f"L {v:,.2f}" for v in top["saldo_ayer"].tolist()],
                        [f"L {v:,.2f}" for v in top["saldo_promedio"].tolist()],
                        [f"L {v:,.2f}" for v in top["diferencia_abs"].tolist()],
                        [f"{int(v):,}" for v in top["transacciones"].tolist()],
                    ],
                    fill_color=COLORES["blanco"],
                    font=dict(color=COLORES["azul_experto"], size=11),
                    align="left",
                ),
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} cuentas por diferencia absoluta (saldo cierre vs saldo promedio)",
        margin=dict(t=48, b=10, l=10, r=10),
        paper_bgcolor=COLORES["blanco"],
    )
    return fig


def construir_layout(df_base: pd.DataFrame) -> html.Div:
    """Layout principal del dashboard."""
    monedas = sorted(df_base["moneda"].dropna().unique().tolist())
    opciones_moneda = [{"label": "Todos", "value": "Todos"}] + [
        {"label": moneda, "value": moneda} for moneda in monedas
    ]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2(
                "Cuenta Digital - Saldo Promedio vs Saldo al Cierre (31-Mar-2026)",
                style={"color": COLORES["azul_experto"], "marginBottom": "6px"},
            ),
            html.P(
                "Enfoque mixto: totales y promedios por cuenta para saldos de cierre y saldos promedio.",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "22px"},
            ),
            html.Div(
                style={"maxWidth": "280px", "marginBottom": "22px"},
                children=[
                    html.Label("Filtro global por moneda", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="filtro-moneda",
                        options=opciones_moneda,
                        value="Todos",
                        clearable=False,
                    ),
                ],
            ),
            html.Div(
                id="kpis-contenedor",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(230px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(450px, 1fr))",
                    "gap": "18px",
                    "marginBottom": "20px",
                },
                children=[
                    dcc.Graph(id="g-comparativo-estatus"),
                    dcc.Graph(id="g-relacion-saldos"),
                ],
            ),
            dcc.Graph(id="g-tabla-top-diferencias"),
        ],
    )


def construir_app(df_base: pd.DataFrame) -> Dash:
    """Crea app Dash con callbacks."""
    app = Dash(__name__)
    app.layout = construir_layout(df_base)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-comparativo-estatus", "figure"),
        Output("g-relacion-saldos", "figure"),
        Output("g-tabla-top-diferencias", "figure"),
        Input("filtro-moneda", "value"),
    )
    def actualizar_vista(moneda: str):
        df = filtrar_por_moneda(df_base, moneda)
        return (
            construir_kpis(df),
            grafico_comparativo_estatus(df),
            grafico_relacion_saldos(df),
            tabla_top_diferencias(df),
        )

    return app


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")
    try:
        df = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos del dashboard: {exc}")
        raise SystemExit(1) from exc

    print(f"Registros cargados (dedupe aplicado): {len(df):,}")

    app = construir_app(df)
    print("Dashboard corriendo en http://127.0.0.1:8063")
    app.run(debug=True, use_reloader=False, port=8063)


if __name__ == "__main__":
    main()
