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


def filtrar_cuentas_activas(df: pd.DataFrame) -> pd.DataFrame:
    """Limita el universo a cuentas activas."""
    estados_activos = {"A", "ACTIVA", "ACTIVO"}
    estado_norm = df["estatus_cuenta"].astype(str).str.strip().str.upper()
    return df[estado_norm.isin(estados_activos)].copy()


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


def _agrupar_metrica_por_estatus(df: pd.DataFrame, metrica: str, tipo: str) -> pd.DataFrame:
    """Agrupa una metrica por estatus de cuenta."""
    if df.empty:
        return pd.DataFrame(columns=["estatus", "valor"])

    base = df.copy()
    base["estatus"] = base["estatus_cuenta"].astype(str).str.strip().str.upper()
    base.loc[base["estatus"].eq(""), "estatus"] = "SIN DATO"

    if tipo == "count":
        agg = base.groupby("estatus")["cuenta"].nunique()
    elif tipo == "sum":
        agg = base.groupby("estatus")[metrica].sum()
    else:
        agg = base.groupby("estatus")[metrica].mean()

    resultado = (
        agg.reset_index(name="valor")
        .sort_values("valor", ascending=False)
        .reset_index(drop=True)
    )
    return resultado


def grafico_metrica_por_estatus(
    df: pd.DataFrame,
    titulo: str,
    metrica: str,
    tipo: str,
    color: str,
    formato_moneda: bool,
    etiqueta_hover: str,
) -> go.Figure:
    """Genera grafico de barras por estatus para una metrica."""
    resumen = _agrupar_metrica_por_estatus(df, metrica, tipo)
    if resumen.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    textos = []
    for valor in resumen["valor"].tolist():
        if formato_moneda:
            textos.append(f"L {valor:,.2f}")
        else:
            textos.append(f"{int(round(valor)):,}")

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen["estatus"].tolist(),
                y=resumen["valor"].tolist(),
                marker_color=color,
                text=textos,
                textposition="outside",
                hovertemplate=f"Estatus: %{{x}}<br>{etiqueta_hover}: %{{text}}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=titulo,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Cuentas activas"),
        yaxis=dict(title="Monto (L)" if formato_moneda else "Cuentas"),
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
                "Enfoque mixto: totales y promedios por cuenta para saldos de cierre y saldos promedio (solo cuentas activas).",
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
                    "gridTemplateColumns": "repeat(auto-fit, minmax(420px, 1fr))",
                    "gap": "18px",
                    "marginBottom": "20px",
                },
                children=[
                    dcc.Graph(id="g-suma-saldo-promedio"),
                    dcc.Graph(id="g-suma-saldo-cierre"),
                    dcc.Graph(id="g-cuentas-analizadas"),
                    dcc.Graph(id="g-promedio-saldo-cierre"),
                    dcc.Graph(id="g-promedio-saldo-promedio"),
                ],
            ),
        ],
    )


def construir_app(df_base: pd.DataFrame) -> Dash:
    """Crea app Dash con callbacks."""
    app = Dash(__name__)
    app.layout = construir_layout(df_base)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-suma-saldo-promedio", "figure"),
        Output("g-suma-saldo-cierre", "figure"),
        Output("g-cuentas-analizadas", "figure"),
        Output("g-promedio-saldo-cierre", "figure"),
        Output("g-promedio-saldo-promedio", "figure"),
        Input("filtro-moneda", "value"),
    )
    def actualizar_vista(moneda: str):
        df = filtrar_por_moneda(df_base, moneda)
        df = filtrar_cuentas_activas(df)
        return (
            construir_kpis(df),
            grafico_metrica_por_estatus(
                df,
                "Suma de saldos promedios",
                "saldo_promedio",
                "sum",
                COLORES["amarillo_opt"],
                True,
                "Suma saldo promedio",
            ),
            grafico_metrica_por_estatus(
                df,
                "Suma de saldos a fin de periodo",
                "saldo_ayer",
                "sum",
                COLORES["aqua_digital"],
                True,
                "Suma saldo al cierre",
            ),
            grafico_metrica_por_estatus(
                df,
                "Cuentas analizadas",
                "cuenta",
                "count",
                COLORES["azul_experto"],
                False,
                "Cuentas",
            ),
            grafico_metrica_por_estatus(
                df,
                "Saldo al cierre promedio por cuenta",
                "saldo_ayer",
                "mean",
                COLORES["azul_financiero"],
                True,
                "Promedio saldo al cierre",
            ),
            grafico_metrica_por_estatus(
                df,
                "Saldo promedio por cuenta",
                "saldo_promedio",
                "mean",
                COLORES["amarillo_emp"],
                True,
                "Promedio saldo promedio",
            ),
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
