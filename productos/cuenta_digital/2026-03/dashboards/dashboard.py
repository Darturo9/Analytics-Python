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

# ── Clasificar nuevos vs existentes ──────────────────────────────────────────
df["tipo_cliente"] = df["dif"].apply(lambda x: "Nuevo" if x == 0 else "Existente")

# ── Métricas ──────────────────────────────────────────────────────────────────
total_cuentas   = len(df)
total_nuevos    = (df["tipo_cliente"] == "Nuevo").sum()
total_existentes = (df["tipo_cliente"] == "Existente").sum()

# Aperturas por día con etiqueta solo del número de día
cuentas_por_dia = df.groupby("fecha_apertura").size().reset_index(name="cuentas")
cuentas_por_dia["dia"] = cuentas_por_dia["fecha_apertura"].apply(lambda x: str(x.day) if hasattr(x, 'day') else str(x)[-2:].lstrip("0") or "0")

# Aperturas por día separado por tipo
por_dia_tipo = df.groupby(["fecha_apertura", "tipo_cliente"]).size().reset_index(name="cuentas")
por_dia_tipo["dia"] = por_dia_tipo["fecha_apertura"].apply(lambda x: str(x.day) if hasattr(x, 'day') else str(x)[-2:].lstrip("0") or "0")

# ── Estilos ───────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius":    "8px",
    "padding":         "24px 32px",
    "boxShadow":       "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop":       f"4px solid {COLORES['aqua_digital']}",
    "flex":            "1",
}

layout_grafico = dict(
    plot_bgcolor=COLORES["blanco"],
    paper_bgcolor=COLORES["blanco"],
    font=dict(color=COLORES["azul_experto"]),
    margin=dict(t=10, b=40, l=40, r=10),
    xaxis_title="",
    yaxis_title="Cuentas",
    xaxis=dict(tickmode="linear"),
    legend_title_text="",
)

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

        # Gráfico: aperturas por día (nuevo vs existente)
        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}", "padding": "24px", "flex": "none"}, children=[
            html.H4("Aperturas por día", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(
                figure=px.bar(
                    por_dia_tipo,
                    x="dia",
                    y="cuentas",
                    color="tipo_cliente",
                    barmode="stack",
                    color_discrete_map={
                        "Nuevo":     COLORES["aqua_digital"],
                        "Existente": COLORES["amarillo_opt"],
                    },
                    category_orders={"dia": [str(d) for d in range(1, 32)]}
                ).update_layout(**layout_grafico)
            )
        ]),
    ]
)

if __name__ == "__main__":
    print("Dashboard corriendo en http://127.0.0.1:8050")
    app.run(debug=True)
