"""
dashboard_fondeo_cd.py
----------------------
Dashboard ejecutivo de fondeo para Cuenta Digital.

Ejecucion:
python3 productos/Fondeo_CD/Dashboards/dashboard_fondeo_cd.py --fecha-inicio 2026-03-01 --fecha-fin 2026-03-31
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


BASE_DIR = Path(__file__).resolve().parents[1]
QUERY_RESUMEN = BASE_DIR / "Queries" / "FondeoResumenCuentas.sql"
QUERY_DIARIO = BASE_DIR / "Queries" / "FondeoDiaro.sql"
DEFAULT_FECHA_INICIO = date(2026, 3, 1)
DEFAULT_FECHA_FIN = date(2026, 3, 31)


def parsear_fecha(valor: str, nombre: str) -> date:
    try:
        return date.fromisoformat(valor)
    except ValueError as exc:
        raise ValueError(f"{nombre} debe tener formato YYYY-MM-DD. Valor recibido: {valor}") from exc


def resolver_rango(args: argparse.Namespace) -> tuple[date, date]:
    if args.fecha_inicio and args.fecha_fin:
        inicio = parsear_fecha(args.fecha_inicio, "--fecha-inicio")
        fin = parsear_fecha(args.fecha_fin, "--fecha-fin")
    elif args.fecha_inicio or args.fecha_fin:
        raise ValueError("Debes enviar ambos parametros: --fecha-inicio y --fecha-fin.")
    else:
        inicio, fin = DEFAULT_FECHA_INICIO, DEFAULT_FECHA_FIN
    if inicio > fin:
        raise ValueError("fecha_inicio no puede ser mayor que fecha_fin.")
    return inicio, fin


def cargar_datos(inicio: date, fin: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    params = {"fecha_inicio": inicio.isoformat(), "fecha_fin": fin.isoformat()}
    df_resumen = run_query_file(str(QUERY_RESUMEN), params=params)
    df_diario = run_query_file(str(QUERY_DIARIO), params=params)

    df_resumen.columns = [str(c) for c in df_resumen.columns]
    df_diario.columns = [str(c) for c in df_diario.columns]

    if "fecha_apertura" in df_resumen.columns:
        df_resumen["fecha_apertura"] = pd.to_datetime(df_resumen["fecha_apertura"], errors="coerce")
    if "fecha_primer_fondeo" in df_resumen.columns:
        df_resumen["fecha_primer_fondeo"] = pd.to_datetime(df_resumen["fecha_primer_fondeo"], errors="coerce")
    if "fecha_informacion" in df_diario.columns:
        df_diario["fecha_informacion"] = pd.to_datetime(df_diario["fecha_informacion"], errors="coerce")

    for col in ["tuvo_fondos_mes", "dias_a_primer_fondeo", "dias_con_fondos"]:
        if col in df_resumen.columns:
            df_resumen[col] = pd.to_numeric(df_resumen[col], errors="coerce")

    for col in [
        "cuentas_creadas_periodo",
        "cuentas_reportadas_dia",
        "cuentas_con_fondos_dia",
        "cuentas_con_primer_fondeo_dia",
        "cuentas_acumuladas_con_fondos",
    ]:
        if col in df_diario.columns:
            df_diario[col] = pd.to_numeric(df_diario[col], errors="coerce").fillna(0)

    df_diario = df_diario.sort_values("fecha_informacion").reset_index(drop=True)
    return df_resumen, df_diario


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


def figura_acumulado(df_diario: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df_diario["fecha_informacion"],
            y=df_diario["cuentas_con_fondos_dia"],
            name="Con fondos por dia",
            marker_color=COLORES["amarillo_opt"],
            opacity=0.7,
            hovertemplate="%{x|%Y-%m-%d}<br>Con fondos: %{y:,}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_diario["fecha_informacion"],
            y=df_diario["cuentas_acumuladas_con_fondos"],
            mode="lines+markers",
            name="Acumulado con fondos",
            line=dict(color=COLORES["aqua_digital"], width=3),
            hovertemplate="%{x|%Y-%m-%d}<br>Acumulado: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=35, l=40, r=10),
        xaxis=dict(title="Fecha"),
        yaxis=dict(title="Cuentas"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return fig


def figura_semana(df_resumen: pd.DataFrame) -> go.Figure:
    orden = ["Semana 1", "Semana 2", "Semana 3", "Semana 4"]
    tabla = (
        df_resumen.groupby(["semana_apertura", "tuvo_fondos_mes"])["cuenta"]
        .nunique()
        .reset_index(name="cuentas")
    )
    piv = tabla.pivot(index="semana_apertura", columns="tuvo_fondos_mes", values="cuentas").fillna(0)
    piv = piv.reindex(orden, fill_value=0)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=piv.index.tolist(),
            y=piv.get(1, pd.Series(0, index=piv.index)).tolist(),
            name="Con fondeo",
            marker_color=COLORES["aqua_digital"],
            hovertemplate="%{x}<br>Con fondeo: %{y:,}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=piv.index.tolist(),
            y=piv.get(0, pd.Series(0, index=piv.index)).tolist(),
            name="Sin fondeo",
            marker_color=COLORES["azul_financiero"],
            hovertemplate="%{x}<br>Sin fondeo: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=40, r=10),
        xaxis=dict(title="Semana de apertura"),
        yaxis=dict(title="Cuentas"),
    )
    return fig


def figura_hist_dias(df_resumen: pd.DataFrame) -> go.Figure:
    serie = pd.to_numeric(
        df_resumen.loc[df_resumen["tuvo_fondos_mes"] == 1, "dias_a_primer_fondeo"],
        errors="coerce",
    ).dropna()

    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=serie,
            nbinsx=15,
            marker_color=COLORES["amarillo_opt"],
            hovertemplate="Dias: %{x}<br>Cuentas: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=35, l=40, r=10),
        xaxis=dict(title="Dias a primer fondeo"),
        yaxis=dict(title="Cuentas"),
    )
    return fig


def figura_moneda(df_resumen: pd.DataFrame) -> go.Figure:
    tabla = (
        df_resumen[df_resumen["tuvo_fondos_mes"] == 1]
        .groupby("moneda")["cuenta"]
        .nunique()
        .sort_values(ascending=False)
    )
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=tabla.index.tolist(),
            y=tabla.values.tolist(),
            marker_color=COLORES["azul_experto"],
            hovertemplate="Moneda: %{x}<br>Cuentas con fondeo: %{y:,}<extra></extra>",
        )
    )
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=40, r=10),
        xaxis=dict(title="Moneda"),
        yaxis=dict(title="Cuentas"),
    )
    return fig


def construir_app(df_resumen: pd.DataFrame, df_diario: pd.DataFrame, inicio: date, fin: date) -> Dash:
    total = int(df_resumen["cuenta"].nunique()) if "cuenta" in df_resumen.columns else len(df_resumen)
    fondeadas = int(df_resumen.loc[df_resumen["tuvo_fondos_mes"] == 1, "cuenta"].nunique())
    sin_fondeo = total - fondeadas
    tasa = (fondeadas / total * 100) if total > 0 else 0.0
    promedio_dias = pd.to_numeric(
        df_resumen.loc[df_resumen["tuvo_fondos_mes"] == 1, "dias_a_primer_fondeo"],
        errors="coerce",
    ).dropna().mean()
    promedio_dias_txt = f"{promedio_dias:.1f}" if pd.notna(promedio_dias) else "N/A"

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "24px", "fontFamily": "Arial, sans-serif", "backgroundColor": COLORES["gris_fondo"]},
        children=[
            html.H2(
                f"Fondeo Cuenta Digital | Cohorte apertura {inicio} a {fin}",
                style={"color": COLORES["azul_experto"], "marginBottom": "14px"},
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "12px"},
                children=[
                    kpi_card("Cuentas creadas", f"{total:,}", COLORES["azul_experto"]),
                    kpi_card("Cuentas con fondeo", f"{fondeadas:,}", COLORES["aqua_digital"]),
                    kpi_card("Cuentas sin fondeo", f"{sin_fondeo:,}", COLORES["azul_financiero"]),
                    kpi_card("Tasa de fondeo", f"{tasa:.1f}%", COLORES["amarillo_opt"]),
                    kpi_card("Promedio dias a primer fondeo", promedio_dias_txt, COLORES["amarillo_emp"]),
                ],
            ),
            html.Div(
                style={
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px",
                    "padding": "12px",
                    "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                    "marginTop": "16px",
                },
                children=[
                    html.H4("Evolucion diaria y acumulado de fondeo", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(figure=figura_acumulado(df_diario)),
                ],
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))", "gap": "12px", "marginTop": "12px"},
                children=[
                    html.Div(
                        style={"backgroundColor": COLORES["blanco"], "borderRadius": "10px", "padding": "12px", "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)"},
                        children=[
                            html.H4("Fondeo por semana de apertura", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                            dcc.Graph(figure=figura_semana(df_resumen)),
                        ],
                    ),
                    html.Div(
                        style={"backgroundColor": COLORES["blanco"], "borderRadius": "10px", "padding": "12px", "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)"},
                        children=[
                            html.H4("Distribucion dias a primer fondeo", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                            dcc.Graph(figure=figura_hist_dias(df_resumen)),
                        ],
                    ),
                    html.Div(
                        style={"backgroundColor": COLORES["blanco"], "borderRadius": "10px", "padding": "12px", "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)"},
                        children=[
                            html.H4("Cuentas con fondeo por moneda", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                            dcc.Graph(figure=figura_moneda(df_resumen)),
                        ],
                    ),
                ],
            ),
        ],
    )
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Dashboard ejecutivo de fondeo Cuenta Digital.")
    parser.add_argument("--fecha-inicio", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin", help="Fecha fin (YYYY-MM-DD).")
    parser.add_argument("--port", type=int, default=8056, help="Puerto del dashboard (default 8056).")
    args = parser.parse_args()

    inicio, fin = resolver_rango(args)
    print(f"Cargando datos para rango: {inicio} a {fin}")
    df_resumen, df_diario = cargar_datos(inicio, fin)
    app = construir_app(df_resumen, df_diario, inicio, fin)

    print(f"Dashboard corriendo en http://127.0.0.1:{args.port}")
    app.run(debug=True, use_reloader=False, port=args.port)


if __name__ == "__main__":
    main()
