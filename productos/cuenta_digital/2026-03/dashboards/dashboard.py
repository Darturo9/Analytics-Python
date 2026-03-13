"""
dashboard.py
------------
Dashboard Cuenta Digital - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python productos/cuenta_digital/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

from dash import Dash, html, dcc
import plotly.express as px
from core.db import run_query_file

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")

# ── Métricas base ─────────────────────────────────────────────────────────────
total_cuentas    = len(df)
cuentas_por_dia  = df.groupby("fecha_apertura").size().reset_index(name="cuentas")

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

app.layout = html.Div(style={"fontFamily": "Arial", "backgroundColor": "#f4f6f9", "minHeight": "100vh", "padding": "24px"}, children=[

    # Título
    html.H2("Cuenta Digital — Marzo 2026",
            style={"color": "#06172c", "marginBottom": "24px"}),

    # KPI: total cuentas
    html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
        html.Div(style={
            "backgroundColor": "white",
            "borderRadius": "8px",
            "padding": "24px 32px",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.1)",
            "borderTop": "4px solid #05c0cf"
        }, children=[
            html.P("Cuentas creadas en marzo", style={"margin": 0, "color": "#666", "fontSize": "14px"}),
            html.H1(f"{total_cuentas:,}", style={"margin": "8px 0 0 0", "color": "#06172c"}),
        ]),
    ]),

    # Gráfico: cuentas por día
    html.Div(style={
        "backgroundColor": "white",
        "borderRadius": "8px",
        "padding": "24px",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.1)"
    }, children=[
        html.H4("Aperturas por día", style={"color": "#06172c", "marginTop": 0}),
        dcc.Graph(
            figure=px.bar(
                cuentas_por_dia,
                x="fecha_apertura",
                y="cuentas",
                color_discrete_sequence=["#05c0cf"]
            ).update_layout(
                plot_bgcolor="white",
                paper_bgcolor="white",
                xaxis_title="Fecha",
                yaxis_title="Cuentas",
                margin=dict(t=10, b=40, l=40, r=10)
            )
        )
    ])
])

if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8050")
    app.run(debug=True)
