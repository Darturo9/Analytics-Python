"""
dashboard_uso_dinero_q1.py
--------------------------
Dashboard para visualizar en que usan el dinero los clientes de Cuenta Digital.

Universo:
- Cuentas abiertas en Q1 2026 (enero, febrero, marzo).
- Uso del dinero detectado por pagos/transferencias en Q1 2026.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/dashboard_uso_dinero_q1.py
"""

import sys

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.colors import COLORES
from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/uso_dinero_q1_2026.sql"


def cargar_datos() -> pd.DataFrame:
    """Carga y normaliza los datos base del dashboard."""
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c) for c in df.columns]

    df["padded_codigo_cliente"] = df.get("padded_codigo_cliente", "").astype(str).str.strip()
    df["tipo_uso"] = df.get("tipo_uso", "Sin clasificar").astype(str).str.strip().replace("", "Sin clasificar")
    df["origen_pago"] = df.get("origen_pago", "Sin origen").astype(str).str.strip().replace("", "Sin origen")
    df["periodo_mes"] = df.get("periodo_mes", "").astype(str).str.strip()
    df["genero"] = df.get("genero", "SIN DATO").astype(str).str.strip().str.upper().replace("", "SIN DATO")
    df["generacion"] = df.get("generacion", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    df["rango_edad"] = df.get("rango_edad", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    df["estado_civil"] = df.get("estado_civil", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    df["direccion_lvl_1"] = df.get("direccion_lvl_1", "SIN DATO").astype(str).str.strip().replace("", "SIN DATO")

    df["fecha_transaccion"] = pd.to_datetime(df.get("fecha_transaccion"), errors="coerce")
    df["valor"] = pd.to_numeric(df.get("valor"), errors="coerce").fillna(0.0)
    df["edad"] = pd.to_numeric(df.get("edad"), errors="coerce")

    return df


def filtrar_df(df: pd.DataFrame, mes: str, origen: str) -> pd.DataFrame:
    """Aplica filtros de mes y origen."""
    out = df.copy()
    if mes != "Todos":
        out = out[out["periodo_mes"] == mes]
    if origen != "Todos":
        out = out[out["origen_pago"] == origen]
    return out


def figura_vacia(mensaje: str) -> go.Figure:
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


def obtener_clientes_unicos(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna base unica por cliente para analisis demografico."""
    if df.empty:
        return df.copy()
    return (
        df.sort_values(["padded_codigo_cliente", "fecha_transaccion"])
        .drop_duplicates(subset=["padded_codigo_cliente"], keep="last")
        .reset_index(drop=True)
    )


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


def construir_kpis(df: pd.DataFrame) -> list[html.Div]:
    clientes = int(df["padded_codigo_cliente"].nunique()) if not df.empty else 0
    transacciones = int(len(df))
    monto_total = float(df["valor"].sum()) if not df.empty else 0.0
    ticket_promedio = float(df["valor"].mean()) if not df.empty else 0.0
    tipos_uso = int(df["tipo_uso"].nunique()) if not df.empty else 0

    return [
        kpi_card("Monto total transado", f"L {monto_total:,.2f}", COLORES["aqua_digital"]),
        kpi_card("Total transacciones", f"{transacciones:,}", COLORES["amarillo_opt"]),
        kpi_card("Clientes unicos", f"{clientes:,}", COLORES["azul_experto"]),
        kpi_card("Ticket promedio", f"L {ticket_promedio:,.2f}", COLORES["azul_financiero"]),
        kpi_card("Tipos de uso", f"{tipos_uso:,}", COLORES["amarillo_emp"]),
    ]


def grafico_top_por_monto(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    resumen = (
        df.groupby("tipo_uso", as_index=False)["valor"]
        .sum()
        .sort_values("valor", ascending=False)
        .head(top_n)
    )
    resumen = resumen.sort_values("valor", ascending=True)

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen["tipo_uso"].tolist(),
                x=resumen["valor"].tolist(),
                orientation="h",
                marker_color=COLORES["aqua_digital"],
                text=[f"L {v:,.2f}" for v in resumen["valor"].tolist()],
                textposition="outside",
                hovertemplate="Tipo uso: %{y}<br>Monto: L %{x:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} usos por monto",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=180, r=20),
        xaxis=dict(title="Monto total (L)"),
        yaxis=dict(title=""),
    )
    return fig


def grafico_top_por_transacciones(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    resumen = (
        df.groupby("tipo_uso", as_index=False)
        .size()
        .rename(columns={"size": "transacciones"})
        .sort_values("transacciones", ascending=False)
        .head(top_n)
    )
    resumen = resumen.sort_values("transacciones", ascending=True)

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen["tipo_uso"].tolist(),
                x=resumen["transacciones"].tolist(),
                orientation="h",
                marker_color=COLORES["amarillo_opt"],
                text=[f"{v:,}" for v in resumen["transacciones"].tolist()],
                textposition="outside",
                hovertemplate="Tipo uso: %{y}<br>Transacciones: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} usos por frecuencia",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=180, r=20),
        xaxis=dict(title="Transacciones"),
        yaxis=dict(title=""),
    )
    return fig


def grafico_clientes_por_tipo(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    resumen = (
        df.groupby("tipo_uso")["padded_codigo_cliente"]
        .nunique()
        .reset_index(name="clientes")
        .sort_values("clientes", ascending=False)
        .head(top_n)
    )
    resumen = resumen.sort_values("clientes", ascending=True)

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen["tipo_uso"].tolist(),
                x=resumen["clientes"].tolist(),
                orientation="h",
                marker_color=COLORES["azul_experto"],
                text=[f"{v:,}" for v in resumen["clientes"].tolist()],
                textposition="outside",
                hovertemplate="Tipo uso: %{y}<br>Clientes unicos: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} usos por clientes unicos",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=180, r=20),
        xaxis=dict(title="Clientes unicos"),
        yaxis=dict(title=""),
    )
    return fig


def grafico_mensual(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    top_tipos = (
        df.groupby("tipo_uso")["valor"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .index
        .tolist()
    )
    base = df[df["tipo_uso"].isin(top_tipos)].copy()
    resumen = (
        base.groupby(["periodo_mes", "tipo_uso"], as_index=False)["valor"]
        .sum()
        .sort_values(["periodo_mes", "valor"], ascending=[True, False])
    )

    fig = go.Figure()
    colores = [COLORES["aqua_digital"], COLORES["amarillo_opt"], COLORES["azul_experto"], COLORES["azul_financiero"], COLORES["amarillo_emp"]]
    max_valor = float(resumen["valor"].max()) if not resumen.empty else 0.0
    umbral_texto = max_valor * 0.08 if max_valor > 0 else 0.0
    for idx, tipo in enumerate(top_tipos):
        sub = resumen[resumen["tipo_uso"] == tipo]
        valores = sub["valor"].tolist()
        textos = [f"L {v:,.0f}" if v >= umbral_texto else "" for v in valores]
        fig.add_trace(
            go.Bar(
                name=tipo,
                x=sub["periodo_mes"].tolist(),
                y=valores,
                marker_color=colores[idx % len(colores)],
                text=textos,
                textposition="inside",
                textfont=dict(color=COLORES["blanco"], size=11),
                hovertemplate="Mes: %{x}<br>Tipo: " + tipo + "<br>Monto: L %{y:,.2f}<extra></extra>",
            )
        )

    totales = (
        df.groupby("periodo_mes", as_index=False)["valor"]
        .sum()
        .sort_values("periodo_mes")
    )
    margen_top = max(float(totales["valor"].max()) * 0.10, 1.0) if not totales.empty else 1.0
    fig.add_trace(
        go.Scatter(
            x=totales["periodo_mes"].tolist(),
            y=(totales["valor"] + margen_top).tolist(),
            mode="text",
            text=[f"L {v:,.0f}" for v in totales["valor"].tolist()],
            textposition="top center",
            textfont=dict(size=12, color=COLORES["azul_experto"]),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        title="Evolucion mensual del monto por tipo de uso (Top 5)",
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Mes"),
        yaxis=dict(
            title="Monto total (L)",
            range=[0, float(totales["valor"].max()) + (margen_top * 3)] if not totales.empty else None,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        uniformtext_minsize=10,
        uniformtext_mode="hide",
    )
    return fig


def grafico_monto_por_genero(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    mapa_genero = {"F": "Mujer", "M": "Hombre"}
    base = df.copy()
    base["genero_norm"] = base["genero"].map(mapa_genero).fillna("Sin dato")
    resumen = base.groupby("genero_norm", as_index=False)["valor"].sum().sort_values("valor", ascending=False)

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen["genero_norm"].tolist(),
                y=resumen["valor"].tolist(),
                marker_color=COLORES["azul_financiero"],
                text=[f"L {v:,.2f}" for v in resumen["valor"].tolist()],
                textposition="outside",
                hovertemplate="Genero: %{x}<br>Monto: L %{y:,.2f}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Monto total por genero",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Genero"),
        yaxis=dict(title="Monto total (L)"),
    )
    return fig


def grafico_clientes_por_rango_edad(df: pd.DataFrame) -> go.Figure:
    clientes = obtener_clientes_unicos(df)
    if clientes.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    orden = ["18-25", "26-35", "36-45", "46-55", "56+", "SIN DATO"]
    resumen = clientes["rango_edad"].value_counts().reindex(orden, fill_value=0)

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen.index.tolist(),
                y=resumen.values.tolist(),
                marker_color=COLORES["amarillo_opt"],
                text=[f"{int(v):,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Rango edad: %{x}<br>Clientes: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes unicos por rango de edad",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=40, r=20),
        xaxis=dict(title="Rango de edad"),
        yaxis=dict(title="Clientes unicos"),
    )
    return fig


def grafico_clientes_por_generacion(df: pd.DataFrame) -> go.Figure:
    clientes = obtener_clientes_unicos(df)
    if clientes.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    orden = [
        "Generation X (1965-1980)",
        "Gen Y - Millennials (1981-1996)",
        "Generacion Z (1997-2012)",
        "OTRA GENERACION",
        "SIN DATO",
    ]
    resumen = clientes["generacion"].value_counts().reindex(orden, fill_value=0)
    resumen = resumen[resumen > 0]

    fig = go.Figure(
        data=[
            go.Bar(
                x=resumen.index.tolist(),
                y=resumen.values.tolist(),
                marker_color=COLORES["aqua_digital"],
                text=[f"{int(v):,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Generacion: %{x}<br>Clientes: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Clientes unicos por generacion",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=50, l=40, r=20),
        xaxis=dict(title="Generacion", tickangle=-20),
        yaxis=dict(title="Clientes unicos"),
    )
    return fig


def grafico_clientes_por_estado_civil(df: pd.DataFrame, top_n: int = 8) -> go.Figure:
    clientes = obtener_clientes_unicos(df)
    if clientes.empty:
        return figura_vacia("Sin datos para el filtro seleccionado")

    resumen = (
        clientes["estado_civil"]
        .fillna("SIN DATO")
        .astype(str)
        .str.strip()
        .replace("", "SIN DATO")
        .value_counts()
        .head(top_n)
        .sort_values(ascending=True)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                y=resumen.index.tolist(),
                x=resumen.values.tolist(),
                orientation="h",
                marker_color=COLORES["azul_experto"],
                text=[f"{int(v):,}" for v in resumen.values.tolist()],
                textposition="outside",
                hovertemplate="Estado civil: %{y}<br>Clientes: %{x:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} estados civiles por clientes unicos",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=55, b=40, l=180, r=20),
        xaxis=dict(title="Clientes unicos"),
        yaxis=dict(title=""),
    )
    return fig


def construir_layout(df_base: pd.DataFrame) -> html.Div:
    meses = sorted([m for m in df_base["periodo_mes"].dropna().unique().tolist() if m])
    opciones_mes = [{"label": "Todos", "value": "Todos"}] + [{"label": m, "value": m} for m in meses]

    origenes = sorted([o for o in df_base["origen_pago"].dropna().unique().tolist() if o])
    opciones_origen = [{"label": "Todos", "value": "Todos"}] + [{"label": o, "value": o} for o in origenes]

    return html.Div(
        style={"padding": "32px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("Cuenta Digital Q1 2026 - Uso del Dinero", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                "Analisis de pagos y transacciones para clientes con cuentas abiertas en enero-marzo 2026.",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "22px"},
            ),
            html.Div(
                style={"display": "flex", "gap": "16px", "marginBottom": "22px", "flexWrap": "wrap"},
                children=[
                    html.Div(
                        style={"maxWidth": "240px", "minWidth": "240px"},
                        children=[
                            html.Label("Mes", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                            dcc.Dropdown(id="filtro-mes", options=opciones_mes, value="Todos", clearable=False),
                        ],
                    ),
                    html.Div(
                        style={"maxWidth": "240px", "minWidth": "240px"},
                        children=[
                            html.Label("Origen", style={"color": COLORES["azul_experto"], "fontWeight": "bold"}),
                            dcc.Dropdown(id="filtro-origen", options=opciones_origen, value="Todos", clearable=False),
                        ],
                    ),
                ],
            ),
            html.Div(
                id="kpis-contenedor",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr",
                    "gap": "30px",
                },
                children=[
                    dcc.Graph(id="g-top-monto", style={"width": "100%"}),
                    dcc.Graph(id="g-top-frecuencia", style={"width": "100%"}),
                    dcc.Graph(id="g-top-clientes", style={"width": "100%"}),
                    dcc.Graph(id="g-mensual", style={"width": "100%"}),
                    dcc.Graph(id="g-demografico-genero", style={"width": "100%"}),
                    dcc.Graph(id="g-demografico-edad", style={"width": "100%"}),
                    dcc.Graph(id="g-demografico-generacion", style={"width": "100%"}),
                    dcc.Graph(id="g-demografico-estado-civil", style={"width": "100%"}),
                ],
            ),
        ],
    )


def construir_app(df_base: pd.DataFrame) -> Dash:
    app = Dash(__name__)
    app.layout = construir_layout(df_base)

    @app.callback(
        Output("kpis-contenedor", "children"),
        Output("g-top-monto", "figure"),
        Output("g-top-frecuencia", "figure"),
        Output("g-top-clientes", "figure"),
        Output("g-mensual", "figure"),
        Output("g-demografico-genero", "figure"),
        Output("g-demografico-edad", "figure"),
        Output("g-demografico-generacion", "figure"),
        Output("g-demografico-estado-civil", "figure"),
        Input("filtro-mes", "value"),
        Input("filtro-origen", "value"),
    )
    def actualizar_vista(mes: str, origen: str):
        df = filtrar_df(df_base, mes, origen)
        return (
            construir_kpis(df),
            grafico_top_por_monto(df),
            grafico_top_por_transacciones(df),
            grafico_clientes_por_tipo(df),
            grafico_mensual(df),
            grafico_monto_por_genero(df),
            grafico_clientes_por_rango_edad(df),
            grafico_clientes_por_generacion(df),
            grafico_clientes_por_estado_civil(df),
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

    print(f"Registros de uso cargados: {len(df):,}")
    app = construir_app(df)
    print("Dashboard corriendo en http://127.0.0.1:8064")
    app.run(debug=True, use_reloader=False, port=8064)


if __name__ == "__main__":
    main()
