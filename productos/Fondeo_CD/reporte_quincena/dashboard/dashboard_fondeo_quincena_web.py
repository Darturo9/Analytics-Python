"""
dashboard_fondeo_quincena_web.py
--------------------------------
Dashboard web para analizar fondeo de cuentas creadas en una quincena.

Incluye:
- KPIs principales
- Distribucion de cuentas fondeadas por genero
- Distribucion de cuentas fondeadas por generacion
- Top 3 departamentos por monto maximo fondeado
- Boton para descargar captura PNG del dashboard

Ejecucion:
    python3 productos/Fondeo_CD/reporte_quincena/dashboard/dashboard_fondeo_quincena_web.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.colors import COLORES
from core.db import run_query_file


RUTA_QUERY = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "reporte_quincena"
    / "queries"
    / "detalle_fondeo_quincena_dashboard.sql"
)

# Configuracion editable (sin argumentos en terminal).
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15

CONFIG_HOST = "127.0.0.1"
CONFIG_PORT = 8062


def validar_rango(anio: int, mes: int, dia_inicio: int, dia_fin: int) -> tuple[date, date]:
    if mes < 1 or mes > 12:
        raise ValueError("CONFIG_MES debe estar entre 1 y 12.")
    if dia_inicio < 1:
        raise ValueError("CONFIG_DIA_INICIO debe ser mayor o igual a 1.")
    if dia_fin < dia_inicio:
        raise ValueError("CONFIG_DIA_FIN debe ser mayor o igual a CONFIG_DIA_INICIO.")

    fecha_inicio = date(anio, mes, dia_inicio)
    if mes == 12:
        primer_dia_mes_siguiente = date(anio + 1, 1, 1)
    else:
        primer_dia_mes_siguiente = date(anio, mes + 1, 1)
    ultimo_dia_mes = primer_dia_mes_siguiente - timedelta(days=1)

    if dia_fin > ultimo_dia_mes.day:
        raise ValueError(
            f"CONFIG_DIA_FIN ({dia_fin}) excede el ultimo dia del mes ({ultimo_dia_mes.day}) para {anio}-{mes:02d}."
        )

    fecha_fin_exclusiva = date(anio, mes, dia_fin) + timedelta(days=1)
    return fecha_inicio, fecha_fin_exclusiva


def clasificar_generacion(fecha_nac: pd.Series) -> pd.Series:
    year = pd.to_datetime(fecha_nac, errors="coerce").dt.year
    return pd.Series(
        pd.NA,
        index=fecha_nac.index,
        dtype="object",
    ).mask(year.between(1965, 1980), "Generation X (1965-1980)") \
     .mask(year.between(1981, 1996), "Gen Y - Millennials (1981-1996)") \
     .mask(year.between(1997, 2012), "Generacion Z (1997-2012)") \
     .fillna("Otra Generacion / Sin dato")


def cargar_datos(params: dict) -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY), params=params)
    df.columns = [str(c) for c in df.columns]
    if df.empty:
        return df

    df["saldo_max_periodo"] = pd.to_numeric(df.get("saldo_max_periodo"), errors="coerce").fillna(0.0)
    df["fondeada"] = pd.to_numeric(df.get("fondeada"), errors="coerce").fillna(0).astype(int)
    df["genero"] = df.get("genero", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    df["departamento"] = df.get("departamento", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    df["generacion"] = clasificar_generacion(df.get("fecha_nac"))
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


def grafico_genero(df_fondeadas: pd.DataFrame) -> go.Figure:
    if df_fondeadas.empty:
        return figura_vacia("Sin cuentas fondeadas en el rango")

    resumen = (
        df_fondeadas.groupby("genero", as_index=False)["numero_cuenta"]
        .nunique()
        .rename(columns={"numero_cuenta": "cuentas"})
        .sort_values("cuentas", ascending=False)
    )

    fig = go.Figure(
        data=[
            go.Pie(
                labels=resumen["genero"],
                values=resumen["cuentas"],
                hole=0.45,
                marker=dict(colors=[COLORES["azul_financiero"], COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["gris_texto"]]),
                textinfo="label+percent",
                hovertemplate="%{label}<br>Cuentas: %{value:,}<br>Participacion: %{percent}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas fondeadas por genero",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig


def grafico_generacion(df_fondeadas: pd.DataFrame) -> go.Figure:
    if df_fondeadas.empty:
        return figura_vacia("Sin cuentas fondeadas en el rango")

    resumen = (
        df_fondeadas.groupby("generacion", as_index=False)["numero_cuenta"]
        .nunique()
        .rename(columns={"numero_cuenta": "cuentas"})
        .sort_values("cuentas", ascending=False)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen["generacion"],
                y=resumen["cuentas"],
                marker_color=COLORES["aqua_digital"],
                text=[f"{v:,}" for v in resumen["cuentas"]],
                textposition="outside",
                hovertemplate="Generacion: %{x}<br>Cuentas: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Cuentas fondeadas por generacion",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=60, l=40, r=10),
        xaxis=dict(title="Generacion", tickangle=-12),
        yaxis=dict(title="Cuentas"),
    )
    return fig


def top3_departamentos(df_fondeadas: pd.DataFrame) -> pd.DataFrame:
    if df_fondeadas.empty:
        return pd.DataFrame(columns=["departamento", "cuentas_fondeadas", "monto_max_fondeado"])

    top = (
        df_fondeadas.groupby("departamento", as_index=False)
        .agg(
            cuentas_fondeadas=("numero_cuenta", "nunique"),
            monto_max_fondeado=("saldo_max_periodo", "sum"),
        )
        .sort_values(["monto_max_fondeado", "cuentas_fondeadas"], ascending=[False, False])
        .head(3)
        .reset_index(drop=True)
    )
    return top


def grafico_top_deptos(top3: pd.DataFrame) -> go.Figure:
    if top3.empty:
        return figura_vacia("Sin cuentas fondeadas en el rango")

    fig = go.Figure(
        data=[
            go.Bar(
                x=top3["departamento"],
                y=top3["monto_max_fondeado"],
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,.0f}" for v in top3["monto_max_fondeado"]],
                textposition="outside",
                hovertemplate=(
                    "Departamento: %{x}"
                    "<br>Monto maximo fondeado (suma): %{y:,.2f}"
                    "<extra></extra>"
                ),
            )
        ]
    )
    fig.update_layout(
        title="Top 3 departamentos por monto maximo fondeado",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=50, l=40, r=10),
        xaxis=dict(title="Departamento"),
        yaxis=dict(title="Monto maximo fondeado (suma)"),
    )
    return fig


def construir_layout(df: pd.DataFrame, periodo: str) -> html.Div:
    total_cuentas = int(df["numero_cuenta"].nunique()) if not df.empty else 0
    df_fondeadas = df[df["fondeada"] == 1].copy() if not df.empty else df
    total_fondeadas = int(df_fondeadas["numero_cuenta"].nunique()) if not df_fondeadas.empty else 0
    sin_fondear = total_cuentas - total_fondeadas
    tasa = (total_fondeadas / total_cuentas * 100.0) if total_cuentas > 0 else 0.0
    top3 = top3_departamentos(df_fondeadas)

    table_rows = []
    if not top3.empty:
        for _, r in top3.iterrows():
            table_rows.append(
                html.Tr(
                    [
                        html.Td(r["departamento"]),
                        html.Td(f"{int(r['cuentas_fondeadas']):,}", style={"textAlign": "right"}),
                        html.Td(f"{float(r['monto_max_fondeado']):,.2f}", style={"textAlign": "right"}),
                    ]
                )
            )
    else:
        table_rows.append(html.Tr([html.Td("Sin datos", colSpan=3, style={"textAlign": "center"})]))

    return html.Div(
        id="dashboard-root",
        style={"padding": "30px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("Fondeo CD - Dashboard Quincenal", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                f"Periodo evaluado (creacion y fondeo): {periodo}",
                style={"color": COLORES["gris_texto"], "marginTop": 0},
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(210px, 1fr))",
                    "gap": "14px",
                    "marginBottom": "18px",
                },
                children=[
                    kpi_card("Cuentas creadas", f"{total_cuentas:,}", COLORES["azul_financiero"]),
                    kpi_card("Cuentas fondeadas", f"{total_fondeadas:,}", COLORES["aqua_digital"]),
                    kpi_card("Cuentas sin fondear", f"{sin_fondear:,}", COLORES["gris_texto"]),
                    kpi_card("Tasa de fondeo", f"{tasa:.2f}%", COLORES["amarillo_opt"]),
                ],
            ),
            html.Button(
                "Descargar captura (PNG)",
                id="btn-captura",
                n_clicks=0,
                style={
                    "marginBottom": "14px",
                    "backgroundColor": COLORES["azul_experto"],
                    "color": COLORES["blanco"],
                    "border": "none",
                    "padding": "10px 14px",
                    "borderRadius": "8px",
                    "cursor": "pointer",
                    "fontWeight": "bold",
                },
            ),
            html.Div(id="captura-status", style={"display": "none"}),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                children=[
                    dcc.Graph(figure=grafico_genero(df_fondeadas)),
                    dcc.Graph(figure=grafico_generacion(df_fondeadas)),
                ],
            ),
            dcc.Graph(figure=grafico_top_deptos(top3)),
            html.H4("Top 3 departamentos (resumen)", style={"color": COLORES["azul_experto"], "marginBottom": "8px"}),
            html.Table(
                style={
                    "width": "100%",
                    "backgroundColor": COLORES["blanco"],
                    "borderCollapse": "collapse",
                    "boxShadow": "0 1px 6px rgba(0, 56, 101, 0.12)",
                },
                children=[
                    html.Thead(
                        html.Tr(
                            [
                                html.Th("Departamento", style={"textAlign": "left", "padding": "8px", "borderBottom": "1px solid #d9e2ec"}),
                                html.Th("Cuentas fondeadas", style={"textAlign": "right", "padding": "8px", "borderBottom": "1px solid #d9e2ec"}),
                                html.Th("Monto maximo fondeado", style={"textAlign": "right", "padding": "8px", "borderBottom": "1px solid #d9e2ec"}),
                            ]
                        )
                    ),
                    html.Tbody(table_rows),
                ],
            ),
        ],
    )


def main() -> None:
    fecha_inicio, fecha_fin_exclusiva = validar_rango(CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN)
    periodo = f"{fecha_inicio.isoformat()} a {(fecha_fin_exclusiva - timedelta(days=1)).isoformat()}"
    params = {
        "fecha_inicio_quincena": fecha_inicio.isoformat(),
        "fecha_fin_quincena_exclusiva": fecha_fin_exclusiva.isoformat(),
        "fecha_inicio_fondeo": fecha_inicio.isoformat(),
        "fecha_fin_fondeo_exclusiva": fecha_fin_exclusiva.isoformat(),
    }

    print(f"Cargando query: {RUTA_QUERY}")
    print(
        "Configuracion -> "
        f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, dia_fin={CONFIG_DIA_FIN}"
    )

    try:
        df = cargar_datos(params)
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    app = Dash(
        __name__,
        external_scripts=[
            "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js",
        ],
        title="Fondeo CD - Quincena",
    )
    app.layout = construir_layout(df, periodo)

    app.clientside_callback(
        """
        function(n_clicks){
            if(!n_clicks){ return window.dash_clientside.no_update; }
            var node = document.getElementById('dashboard-root');
            if(!node || typeof html2canvas === 'undefined'){ return 'no-captura'; }
            html2canvas(node, {scale: 2, backgroundColor: '#f4f7fb'}).then(function(canvas){
                var link = document.createElement('a');
                link.download = 'fondeo_cd_quincena.png';
                link.href = canvas.toDataURL('image/png');
                link.click();
            });
            return 'captura-' + n_clicks;
        }
        """,
        Output("captura-status", "children"),
        Input("btn-captura", "n_clicks"),
    )

    print(f"Dashboard disponible en http://{CONFIG_HOST}:{CONFIG_PORT}")
    app.run(debug=False, host=CONFIG_HOST, port=CONFIG_PORT)


if __name__ == "__main__":
    main()
