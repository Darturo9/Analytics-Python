"""
dashboard_fondeo_cd.py
----------------------
Dashboard ejecutivo de Fondeo Cuenta Digital.
Usa FondeoDiaro.sql (datos por cuenta) y TopDiasFondeo.sql (actividad diaria).

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_fondeo_cd.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/FondeoDiaro.sql"
QUERY_TOP_DIAS = "productos/Fondeo_CD/Queries/TopDiasFondeo.sql"
QUERY_APERTURAS = "productos/Fondeo_CD/Queries/AperturasDiarias.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c) for c in df.columns]

    df["fecha_apertura"] = pd.to_datetime(df.get("fecha_apertura"), errors="coerce")
    df["primer_dia_fondeo"] = pd.to_datetime(df.get("primer_dia_fondeo"), errors="coerce")
    df["saldo_maximo_mes"] = pd.to_numeric(df.get("saldo_maximo_mes"), errors="coerce").fillna(0.0)
    df["saldo_promedio_dias_fondeados"] = pd.to_numeric(df.get("saldo_promedio_dias_fondeados"), errors="coerce").fillna(0.0)
    df["dias_con_fondos"] = (
        pd.to_numeric(df.get("dias_con_fondos"), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["cuenta"] = df.get("cuenta", "").astype(str).str.strip()
    df["padded_codigo_cliente"] = df.get("padded_codigo_cliente", "").astype(str).str.strip()
    df["moneda"] = df.get("moneda", "Sin dato").astype(str).str.strip().replace("", "Sin dato")

    df["dias_hasta_fondeo"] = (df["primer_dia_fondeo"] - df["fecha_apertura"]).dt.days.clip(lower=0)

    bins = [-1, 5, 10, 20, 1000]
    labels = ["1-5", "6-10", "11-20", "21+"]
    df["rango_dias_fondeo"] = pd.cut(df["dias_con_fondos"], bins=bins, labels=labels)
    return df


def cargar_top_dias() -> pd.DataFrame:
    df = run_query_file(QUERY_TOP_DIAS)
    df.columns = [str(c) for c in df.columns]
    df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce")
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    return df


def cargar_aperturas() -> pd.DataFrame:
    df = run_query_file(QUERY_APERTURAS)
    df.columns = [str(c) for c in df.columns]
    df["fecha_apertura"] = pd.to_datetime(df.get("fecha_apertura"), errors="coerce")
    df["cuentas_abiertas"] = pd.to_numeric(df.get("cuentas_abiertas"), errors="coerce").fillna(0).astype(int)
    return df


def filtrar_df(df: pd.DataFrame, moneda: str) -> pd.DataFrame:
    if moneda == "Todos":
        return df.copy()
    return df[df["moneda"] == moneda].copy()


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
        font=dict(color=COLORES["azul_experto"]),
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


def grafico_hist_dias(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")
    fig = go.Figure(
        data=[
            go.Histogram(
                x=df["dias_con_fondos"],
                marker_color=COLORES["amarillo_opt"],
                nbinsx=15,
                hovertemplate="Dias con fondos: %{x}<br>Cuentas: %{y}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Distribucion de dias con fondos",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=45, b=30, l=40, r=10),
        xaxis=dict(title="Dias con fondos"),
        yaxis=dict(title="Cantidad de cuentas"),
    )
    return fig


def grafico_rangos_dias(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")
    orden = ["1-5", "6-10", "11-20", "21+"]
    conteo = (
        df.groupby("rango_dias_fondeo")["cuenta"]
        .nunique()
        .reindex(orden, fill_value=0)
    )
    fig = go.Figure(
        data=[
            go.Bar(
                x=conteo.index.tolist(),
                y=conteo.values.tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in conteo.values.tolist()],
                textposition="outside",
                hovertemplate="Rango: %{x}<br>Cuentas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas por rango de dias con fondos",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=45, b=30, l=40, r=10),
        xaxis=dict(title="Rango de dias"),
        yaxis=dict(title="Cantidad de cuentas"),
    )
    return fig


def grafico_moneda(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")
    conteo = df.groupby("moneda")["cuenta"].nunique().sort_values(ascending=False)
    fig = go.Figure(
        data=[
            go.Bar(
                x=conteo.index.tolist(),
                y=conteo.values.tolist(),
                marker_color=COLORES["azul_financiero"],
                text=[f"{v:,}" for v in conteo.values.tolist()],
                textposition="outside",
                hovertemplate="Moneda: %{x}<br>Cuentas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Participacion por moneda",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=45, b=30, l=40, r=10),
        xaxis=dict(title="Moneda"),
        yaxis=dict(title="Cantidad de cuentas"),
    )
    return fig


def grafico_actividad_diaria(df_dias: pd.DataFrame) -> go.Figure:
    if df_dias.empty:
        return figura_vacia("Sin datos de actividad diaria")
    solo_marzo = df_dias[df_dias["fecha"].dt.month == 3].sort_values("fecha")
    if solo_marzo.empty:
        return figura_vacia("Sin datos de marzo")
    etiquetas = solo_marzo["fecha"].dt.strftime("%d %b").tolist()
    fig = go.Figure(
        data=[
            go.Bar(
                x=etiquetas,
                y=solo_marzo["cuentas_fondeadas"].tolist(),
                marker_color=COLORES["azul_experto"],
                text=[f"{v:,}" for v in solo_marzo["cuentas_fondeadas"].tolist()],
                textposition="outside",
                hovertemplate="Fecha: %{x}<br>Cuentas fondeadas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas fondeadas por dia - Marzo 2026",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=45, b=60, l=40, r=20),
        xaxis=dict(title="Dia", tickangle=-45),
        yaxis=dict(title="Cuentas fondeadas"),
    )
    return fig


def grafico_apertura_vs_fondeo(df: pd.DataFrame, df_aperturas: pd.DataFrame) -> go.Figure:
    if df.empty or df_aperturas.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    aperturas = (
        df_aperturas.dropna(subset=["fecha_apertura"])
        .set_index(df_aperturas["fecha_apertura"].dt.date)["cuentas_abiertas"]
        .rename("aperturas")
    )
    primeros_fondeos = (
        df.dropna(subset=["primer_dia_fondeo"])
        .groupby(df["primer_dia_fondeo"].dt.date)["cuenta"]
        .nunique()
        .rename("primer_fondeo")
    )

    combined = pd.concat([aperturas, primeros_fondeos], axis=1).fillna(0).astype(int)
    combined = combined[combined.index <= pd.Timestamp("2026-03-31").date()].sort_index()

    if combined.empty:
        return figura_vacia("Sin datos suficientes")

    etiquetas = [d.strftime("%d %b") for d in combined.index]

    fig = go.Figure(
        data=[
            go.Bar(
                name="Cuentas abiertas",
                x=etiquetas,
                y=combined["aperturas"].tolist(),
                marker_color=COLORES["aqua_digital"],
                hovertemplate="Fecha: %{x}<br>Aperturas: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="Primer fondeo",
                x=etiquetas,
                y=combined["primer_fondeo"].tolist(),
                marker_color=COLORES["amarillo_opt"],
                hovertemplate="Fecha: %{x}<br>Primer fondeo: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        title="Apertura de cuenta vs primer fondeo por dia",
        barmode="group",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=45, b=60, l=40, r=20),
        xaxis=dict(title="Dia", tickangle=-45),
        yaxis=dict(title="Cantidad de cuentas"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def construir_layout(df: pd.DataFrame) -> html.Div:
    monedas = sorted(df["moneda"].dropna().unique().tolist())
    opciones_moneda = [{"label": "Todos", "value": "Todos"}] + [
        {"label": m, "value": m} for m in monedas
    ]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("Fondeo Cuenta Digital - Marzo 2026", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                "Base: cuentas con al menos 1 dia de saldo > 0 segun FondeoDiaro.sql",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "24px"},
            ),
            html.Div(
                style={"maxWidth": "300px", "marginBottom": "24px"},
                children=[
                    html.Label("Filtro moneda", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                    dcc.Dropdown(id="filtro-moneda", options=opciones_moneda, value="Todos", clearable=False),
                ],
            ),
            html.Div(
                id="kpis-contenedor",
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "16px", "marginBottom": "28px"},
            ),
            html.Div(
                style={"display": "flex", "flexDirection": "column", "gap": "24px"},
                children=[
                    dcc.Graph(id="g-hist-dias", style={"width": "100%"}),
                    dcc.Graph(id="g-rangos-dias", style={"width": "100%"}),
                    dcc.Graph(id="g-moneda", style={"width": "100%"}),
                    dcc.Graph(id="g-actividad-diaria", style={"width": "100%"}),
                    dcc.Graph(id="g-apertura-vs-fondeo", style={"width": "100%"}),
                ],
            ),
        ],
    )


def construir_app(df_base: pd.DataFrame, df_dias: pd.DataFrame, df_aperturas: pd.DataFrame) -> Dash:
    app = Dash(__name__)
    app.layout = construir_layout(df_base)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-hist-dias", "figure"),
        Output("g-rangos-dias", "figure"),
        Output("g-moneda", "figure"),
        Output("g-actividad-diaria", "figure"),
        Output("g-apertura-vs-fondeo", "figure"),
        Input("filtro-moneda", "value"),
    )
    def actualizar_vista(moneda: str):
        df = filtrar_df(df_base, moneda)

        total_cuentas = int(df["cuenta"].nunique())
        saldo_prom_max = float(df["saldo_maximo_mes"].mean()) if total_cuentas > 0 else 0.0
        saldo_prom_real = float(df["saldo_promedio_dias_fondeados"].mean()) if total_cuentas > 0 else 0.0
        promedio_dias = float(df["dias_con_fondos"].mean()) if total_cuentas > 0 else 0.0
        mediana_dias = float(df["dias_con_fondos"].median()) if total_cuentas > 0 else 0.0
        pct_10 = float((df["dias_con_fondos"] >= 10).mean() * 100) if total_cuentas > 0 else 0.0
        dias_activacion = float(df["dias_hasta_fondeo"].mean()) if total_cuentas > 0 else 0.0

        kpis = [
            kpi_card("Cuentas fondeadas", f"{total_cuentas:,}", COLORES["azul_experto"]),
            kpi_card("Saldo promedio (dias fondeados)", f"{saldo_prom_real:,.2f}", COLORES["aqua_digital"]),
            kpi_card("Saldo maximo promedio", f"{saldo_prom_max:,.2f}", COLORES["azul_financiero"]),
            kpi_card("Promedio dias con fondos", f"{promedio_dias:,.1f}", COLORES["amarillo_opt"]),
            kpi_card("Mediana dias con fondos", f"{mediana_dias:,.1f}", COLORES["aqua_digital"]),
            kpi_card("Dias promedio hasta primer fondeo", f"{dias_activacion:,.1f}", COLORES["azul_experto"]),
            kpi_card("% cuentas >=10 dias", f"{pct_10:,.1f}%", COLORES["azul_financiero"]),
        ]

        return (
            kpis,
            grafico_hist_dias(df),
            grafico_rangos_dias(df),
            grafico_moneda(df),
            grafico_actividad_diaria(df_dias),
            grafico_apertura_vs_fondeo(df, df_aperturas),
        )

    return app


def main():
    try:
        print(f"Cargando datos desde: {QUERY_PATH}")
        df = cargar_datos()
        print(f"Registros cargados: {len(df):,}")

        print(f"Cargando top dias desde: {QUERY_TOP_DIAS}")
        df_dias = cargar_top_dias()
        print(f"Dias cargados: {len(df_dias):,}")

        print(f"Cargando aperturas desde: {QUERY_APERTURAS}")
        df_aperturas = cargar_aperturas()
        print(f"Aperturas cargadas: {len(df_aperturas):,}")
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos para el dashboard: {exc}")
        raise SystemExit(1) from exc

    app = construir_app(df, df_dias, df_aperturas)
    print("Dashboard corriendo en http://127.0.0.1:8057")
    app.run(debug=True, use_reloader=False, port=8057)


if __name__ == "__main__":
    main()
