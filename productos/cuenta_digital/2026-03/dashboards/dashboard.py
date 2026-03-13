"""
dashboard.py
------------
Dashboard Cuenta Digital - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from dash import Dash, html, dcc
import plotly.graph_objects as go
from core.db import run_query_file
from core.colors import COLORES

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")

# ── Clasificar nuevos vs existentes ──────────────────────────────────────────
df["tipo_cliente"] = df["dif"].apply(lambda x: "Nuevo" if x == 0 else "Existente")
df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"])
df["dia"] = df["fecha_apertura"].dt.day

# ── Métricas ──────────────────────────────────────────────────────────────────
total_cuentas    = len(df)
total_nuevos     = (df["tipo_cliente"] == "Nuevo").sum()
total_existentes = (df["tipo_cliente"] == "Existente").sum()

# Pivot por día y tipo
pivot = df.groupby(["dia", "tipo_cliente"]).size().unstack(fill_value=0)
dias  = sorted(pivot.index.tolist())

existentes = [pivot.loc[d, "Existente"] if "Existente" in pivot.columns else 0 for d in dias]
nuevos     = [pivot.loc[d, "Nuevo"]     if "Nuevo"     in pivot.columns else 0 for d in dias]
totales    = [e + n for e, n in zip(existentes, nuevos)]

# ── Gráfico ───────────────────────────────────────────────────────────────────
fig = go.Figure()

fig.add_trace(go.Bar(
    name="Existente",
    x=dias,
    y=existentes,
    marker_color=COLORES["amarillo_opt"],
))

fig.add_trace(go.Bar(
    name="Nuevo",
    x=dias,
    y=nuevos,
    marker_color=COLORES["aqua_digital"],
    text=totales,
    textposition="outside",
    textfont=dict(size=11, color=COLORES["azul_experto"]),
))

fig.update_layout(
    barmode="stack",
    plot_bgcolor=COLORES["blanco"],
    paper_bgcolor=COLORES["blanco"],
    font=dict(color=COLORES["azul_experto"]),
    margin=dict(t=30, b=40, l=40, r=10),
    xaxis=dict(title="", tickmode="linear", dtick=1),
    yaxis=dict(title="Cuentas"),
    legend_title_text="",
)

# ── Estilos ───────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius":    "8px",
    "padding":         "24px 32px",
    "boxShadow":       "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop":       f"4px solid {COLORES['aqua_digital']}",
    "flex":            "1",
}

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[

        html.H2("Cuenta Digital — Marzo 2026",
                style={"color": COLORES["azul_experto"], "marginBottom": "24px"}),

        # KPIs
        html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style, children=[
                html.P("Total cuentas", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(f"{total_cuentas:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                html.P("Clientes nuevos", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(f"{total_nuevos:,}", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
            ]),
            html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                html.P("Clientes existentes", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(f"{total_existentes:,}", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
            ]),
        ]),

        # Gráfico
        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "flex": "none"}, children=[
            html.H4("Aperturas por día", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(figure=fig)
        ]),
    ]
)

if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8050")
    app.run(debug=True)
