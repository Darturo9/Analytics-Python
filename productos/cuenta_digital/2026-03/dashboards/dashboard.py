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
from core.colors import COLORES, PALETA

# ── Cargar datos ─────────────────────────────────────────────────────────────
print("Cargando datos...")
df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")
print(f"  {len(df)} cuentas cargadas")

# ── Métricas base ─────────────────────────────────────────────────────────────
total_cuentas   = len(df)
cuentas_por_dia = df.groupby("fecha_apertura").size().reset_index(name="cuentas")

# ── Estilos reutilizables ─────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius":    "8px",
    "padding":         "24px 32px",
    "boxShadow":       "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop":       f"4px solid {COLORES['aqua_digital']}",
}

# ── App ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[

        # Título
        html.H2("Cuenta Digital — Marzo 2026",
                style={"color": COLORES["azul_experto"], "marginBottom": "24px"}),

        # KPIs
        html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style, children=[
                html.P("Cuentas creadas en marzo", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                html.H1(f"{total_cuentas:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
            ]),
        ]),

        # Gráfico: aperturas por día
        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px"}, children=[
            html.H4("Aperturas por día", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(
                figure=px.bar(
                    cuentas_por_dia,
                    x="fecha_apertura",
                    y="cuentas",
                    color_discrete_sequence=[COLORES["aqua_digital"]]
                ).update_layout(
                    plot_bgcolor=COLORES["blanco"],
                    paper_bgcolor=COLORES["blanco"],
                    xaxis_title="Fecha",
                    yaxis_title="Cuentas",
                    font=dict(color=COLORES["azul_experto"]),
                    margin=dict(t=10, b=40, l=40, r=10)
                )
            )
        ]),
    ]
)

if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8050")
    app.run(debug=True)
