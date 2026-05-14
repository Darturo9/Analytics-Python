"""
dashboard_qbr1_sinlogin_abril_2026.py
--------------------------------------
Dashboard árbol Sin Login — Abril 2026.
Muestra por campaña: contactados, logins (app vs web) y cambio de contraseña.

Ejecución:
    python3 productos/LoginUsuarios/QBR_1_2026/dashboards/dashboard_qbr1_sinlogin_abril_2026.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.colors import COLORES, PALETA
from core.db import run_query

PORT = 8069

CAMPAIGN_IDS = (47516, 47619, 47723, 47955, 48101, 48311, 48514, 48739)

SQL_POR_CAMPANA = """
WITH campanas_abril AS (
    SELECT
        c.CampaignID,
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN (47516, 47619, 47723, 47955, 48101, 48311, 48514, 48739)
      AND CAST(h.FechaAplica AS DATE) >= '2026-04-01'
      AND CAST(h.FechaAplica AS DATE) <  '2026-05-01'
),
logins_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(t.clccli)), 8)                              AS padded_codigo_cliente,
        COUNT(*)                                                                     AS total_logins,
        SUM(CASE WHEN t.secode = 'app-login'             THEN 1 ELSE 0 END)        AS logins_app,
        SUM(CASE WHEN t.secode IN ('web-login', 'login') THEN 1 ELSE 0 END)        AS logins_web
    FROM dw_bel_IBSTTRA_VIEW t
    WHERE t.dw_fecha_trx >= '2026-04-01'
      AND t.dw_fecha_trx <  '2026-05-01'
      AND t.secode IN ('app-login', 'web-login', 'login')
      AND t.clccli IS NOT NULL
    GROUP BY RTRIM(LTRIM(t.clccli))
),
cambios_pass_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente,
        COUNT(*)                                        AS total_cambios
    FROM DW_BEL_IBUSER u
    WHERE u.dw_fecha_cambio_pass >= '2026-04-01'
      AND u.dw_fecha_cambio_pass <  '2026-05-01'
      AND u.CLCCLI IS NOT NULL
    GROUP BY RTRIM(LTRIM(u.CLCCLI))
)
SELECT
    cam.CampaignID,
    COUNT(DISTINCT cam.padded_codigo_cliente)
        AS clientes_contactados,
    COUNT(DISTINCT CASE WHEN l.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_login,
    COALESCE(SUM(l.total_logins), 0)   AS total_logins,
    COALESCE(SUM(l.logins_app), 0)     AS total_logins_app,
    COALESCE(SUM(l.logins_web), 0)     AS total_logins_web,
    COUNT(DISTINCT CASE WHEN cp.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_cambio_pass,
    COALESCE(SUM(cp.total_cambios), 0) AS total_cambios_pass
FROM campanas_abril cam
LEFT JOIN logins_abril l
    ON cam.padded_codigo_cliente = l.padded_codigo_cliente
LEFT JOIN cambios_pass_abril cp
    ON cam.padded_codigo_cliente = cp.padded_codigo_cliente
GROUP BY cam.CampaignID
ORDER BY cam.CampaignID
"""

SQL_TOTAL = """
WITH campanas_abril AS (
    SELECT DISTINCT
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN (47516, 47619, 47723, 47955, 48101, 48311, 48514, 48739)
      AND CAST(h.FechaAplica AS DATE) >= '2026-04-01'
      AND CAST(h.FechaAplica AS DATE) <  '2026-05-01'
),
logins_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(t.clccli)), 8)                              AS padded_codigo_cliente,
        COUNT(*)                                                                     AS total_logins,
        SUM(CASE WHEN t.secode = 'app-login'             THEN 1 ELSE 0 END)        AS logins_app,
        SUM(CASE WHEN t.secode IN ('web-login', 'login') THEN 1 ELSE 0 END)        AS logins_web
    FROM dw_bel_IBSTTRA_VIEW t
    WHERE t.dw_fecha_trx >= '2026-04-01'
      AND t.dw_fecha_trx <  '2026-05-01'
      AND t.secode IN ('app-login', 'web-login', 'login')
      AND t.clccli IS NOT NULL
    GROUP BY RTRIM(LTRIM(t.clccli))
),
cambios_pass_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente,
        COUNT(*)                                        AS total_cambios
    FROM DW_BEL_IBUSER u
    WHERE u.dw_fecha_cambio_pass >= '2026-04-01'
      AND u.dw_fecha_cambio_pass <  '2026-05-01'
      AND u.CLCCLI IS NOT NULL
    GROUP BY RTRIM(LTRIM(u.CLCCLI))
)
SELECT
    COUNT(DISTINCT cam.padded_codigo_cliente)
        AS clientes_contactados,
    COUNT(DISTINCT CASE WHEN l.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_login,
    COALESCE(SUM(l.total_logins), 0)   AS total_logins,
    COALESCE(SUM(l.logins_app), 0)     AS total_logins_app,
    COALESCE(SUM(l.logins_web), 0)     AS total_logins_web,
    COUNT(DISTINCT CASE WHEN cp.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_cambio_pass,
    COALESCE(SUM(cp.total_cambios), 0) AS total_cambios_pass
FROM campanas_abril cam
LEFT JOIN logins_abril l
    ON cam.padded_codigo_cliente = l.padded_codigo_cliente
LEFT JOIN cambios_pass_abril cp
    ON cam.padded_codigo_cliente = cp.padded_codigo_cliente
"""


def pct(parte: float, total: float) -> float:
    return round(parte / total * 100, 2) if total > 0 else 0.0


def kpi_card(titulo: str, valor: str, color_borde: str, subtitulo: str = "") -> html.Div:
    children = [
        html.P(titulo, style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "13px"}),
        html.H2(valor, style={"margin": "6px 0 0 0", "color": COLORES["azul_experto"], "fontSize": "24px"}),
    ]
    if subtitulo:
        children.append(
            html.P(subtitulo, style={"margin": "2px 0 0 0", "color": COLORES["gris_texto"], "fontSize": "12px"})
        )
    return html.Div(
        style={
            "backgroundColor": COLORES["blanco"],
            "borderRadius": "10px",
            "padding": "14px 18px",
            "boxShadow": "0 1px 6px rgba(0,56,101,0.12)",
            "borderTop": f"4px solid {color_borde}",
            "minWidth": "180px",
            "flex": "1",
        },
        children=children,
    )


def figura_logins_app_web(df: pd.DataFrame) -> go.Figure:
    campanas = [str(c) for c in df["CampaignID"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="App",
        x=campanas,
        y=df["total_logins_app"],
        marker_color=COLORES["aqua_digital"],
        text=[f"{v:,}" for v in df["total_logins_app"]],
        textposition="outside",
        hovertemplate="Campaña %{x}<br>App logins: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Web",
        x=campanas,
        y=df["total_logins_web"],
        marker_color=COLORES["amarillo_opt"],
        text=[f"{v:,}" for v in df["total_logins_web"]],
        textposition="outside",
        hovertemplate="Campaña %{x}<br>Web logins: %{y:,}<extra></extra>",
    ))
    max_val = max(
        df["total_logins_app"].max() if not df.empty else 0,
        df["total_logins_web"].max() if not df.empty else 0,
    )
    fig.update_layout(
        barmode="group",
        height=380,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=60, l=50, r=20),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
        xaxis=dict(title="Campaign ID", tickangle=-30),
        yaxis=dict(title="Total eventos", range=[0, max_val * 1.18] if max_val > 0 else [0, 10]),
        bargap=0.2,
        bargroupgap=0.08,
    )
    return fig


def figura_conversion(df: pd.DataFrame) -> go.Figure:
    campanas = [str(c) for c in df["CampaignID"]]
    pct_login = [pct(r["clientes_unicos_login"], r["clientes_contactados"]) for _, r in df.iterrows()]
    pct_pass  = [pct(r["clientes_unicos_cambio_pass"], r["clientes_contactados"]) for _, r in df.iterrows()]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="% con login",
        y=campanas,
        x=pct_login,
        orientation="h",
        marker_color=COLORES["azul_financiero"],
        text=[f"{v:.1f}%" for v in pct_login],
        textposition="outside",
        hovertemplate="Campaña %{y}<br>% login: %{x:.2f}%<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="% cambio contraseña",
        y=campanas,
        x=pct_pass,
        orientation="h",
        marker_color=COLORES["amarillo_emp"],
        text=[f"{v:.1f}%" for v in pct_pass],
        textposition="outside",
        hovertemplate="Campaña %{y}<br>% cambio pass: %{x:.2f}%<extra></extra>",
    ))
    max_pct = max(max(pct_login, default=0), max(pct_pass, default=0))
    fig.update_layout(
        barmode="group",
        height=380,
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=30, l=80, r=60),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
        xaxis=dict(title="% de clientes contactados", range=[0, max_pct * 1.25] if max_pct > 0 else [0, 10]),
        yaxis=dict(title="Campaign ID"),
        bargap=0.2,
        bargroupgap=0.08,
    )
    return fig


def construir_dashboard() -> Dash:
    df_camp  = run_query(SQL_POR_CAMPANA)
    df_total = run_query(SQL_TOTAL)

    t = df_total.iloc[0] if not df_total.empty else None

    contactados_total  = int(t["clientes_contactados"])       if t is not None else 0
    unicos_login_total = int(t["clientes_unicos_login"])      if t is not None else 0
    tot_logins_total   = int(t["total_logins"])               if t is not None else 0
    tot_app_total      = int(t["total_logins_app"])           if t is not None else 0
    tot_web_total      = int(t["total_logins_web"])           if t is not None else 0
    unicos_pass_total  = int(t["clientes_unicos_cambio_pass"])if t is not None else 0

    fig_app_web    = figura_logins_app_web(df_camp) if not df_camp.empty else go.Figure()
    fig_conversion = figura_conversion(df_camp)     if not df_camp.empty else go.Figure()

    app = Dash(__name__)
    app.title = "Sin Login Abril 2026 — Por Campaña"

    seccion_graficos = html.Div(
        style={"display": "flex", "gap": "16px", "flexWrap": "wrap"},
        children=[
            html.Div(
                style={
                    "flex": "1", "minWidth": "420px",
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px", "padding": "12px",
                    "boxShadow": "0 1px 6px rgba(0,56,101,0.12)",
                },
                children=[
                    html.H4("Logins App vs Web por campaña",
                            style={"color": COLORES["azul_experto"], "margin": "0 0 4px 0", "fontSize": "15px"}),
                    dcc.Graph(id="graf-app-web", figure=fig_app_web, config={"displayModeBar": False}),
                ],
            ),
            html.Div(
                style={
                    "flex": "1", "minWidth": "420px",
                    "backgroundColor": COLORES["blanco"],
                    "borderRadius": "10px", "padding": "12px",
                    "boxShadow": "0 1px 6px rgba(0,56,101,0.12)",
                },
                children=[
                    html.H4("% Conversión por campaña (vs contactados)",
                            style={"color": COLORES["azul_experto"], "margin": "0 0 4px 0", "fontSize": "15px"}),
                    dcc.Graph(
                        id="graf-conversion",
                        figure=fig_conversion,
                        config={
                            "displayModeBar": True,
                            "modeBarButtonsToRemove": [
                                "zoom2d", "pan2d", "select2d", "lasso2d",
                                "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d",
                            ],
                            "toImageButtonOptions": {
                                "format":   "png",
                                "filename": "conversion_por_campana_sinlogin_abril2026",
                                "height":   520,
                                "width":    900,
                                "scale":    3,
                            },
                        },
                    ),
                ],
            ),
        ],
    )

    app.layout = html.Div(
        style={
            "backgroundColor": COLORES["gris_fondo"],
            "minHeight": "100vh",
            "padding": "20px 24px",
            "fontFamily": "Arial, sans-serif",
        },
        children=[
            html.H2(
                "Árbol Sin Login — Contactados, Login y Cambio de Contraseña",
                style={"color": COLORES["azul_experto"], "margin": "0 0 4px 0"},
            ),
            html.P(
                "Periodo de contacto: abril 2026 (FechaAplica) | "
                f"Campañas: {', '.join(str(i) for i in CAMPAIGN_IDS)}",
                style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "18px", "fontSize": "13px"},
            ),

            # KPI cards — totales deduplicados
            html.Div(
                style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "20px"},
                children=[
                    kpi_card(
                        "Clientes contactados",
                        f"{contactados_total:,}",
                        COLORES["azul_experto"],
                        "únicos entre campañas",
                    ),
                    kpi_card(
                        "Clientes con login",
                        f"{unicos_login_total:,}",
                        COLORES["aqua_digital"],
                        f"{pct(unicos_login_total, contactados_total):.1f}% de contactados",
                    ),
                    kpi_card(
                        "Total eventos login",
                        f"{tot_logins_total:,}",
                        COLORES["azul_financiero"],
                        f"App {tot_app_total:,} | Web {tot_web_total:,}",
                    ),
                    kpi_card(
                        "Clientes cambio contraseña",
                        f"{unicos_pass_total:,}",
                        COLORES["amarillo_opt"],
                        f"{pct(unicos_pass_total, contactados_total):.1f}% de contactados",
                    ),
                ],
            ),

            seccion_graficos,
        ],
    )

    return app


def main() -> None:
    try:
        app = construir_dashboard()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar consulta SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc

    print(f"Dashboard corriendo en http://127.0.0.1:{PORT}")
    app.run(debug=False, use_reloader=False, host="127.0.0.1", port=PORT)


if __name__ == "__main__":
    main()
