"""
Dashboard web: Creacion Usuario SV - Quincena configurable.

Incluye:
- KPIs de usuarios creados, activos, logins y trx
- Distribucion por genero
- Distribucion por generacion
- Creacion diaria y comparativo diario de logins vs trx
- Boton para descargar captura del dashboard en alta calidad (PNG)
- Boton nativo de cada grafico para exportar PNG de alta calidad

Ejecucion:
    python3 productos/creacion_usuario_sv/reporte_quincena/dashboard/dashboard_creacion_quincena_web.py
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


RUTA_CONVERSION = (
    PROJECT_ROOT
    / "productos"
    / "creacion_usuario_sv"
    / "reporte_quincena"
    / "queries"
    / "conversion_quincena.sql"
)
RUTA_RTM = (
    PROJECT_ROOT
    / "productos"
    / "creacion_usuario_sv"
    / "reporte_quincena"
    / "queries"
    / "comunicacionesRTM_quincena.sql"
)
RUTA_LOGINS = (
    PROJECT_ROOT
    / "productos"
    / "creacion_usuario_sv"
    / "reporte_quincena"
    / "queries"
    / "logins_quincena.sql"
)
RUTA_TRX = (
    PROJECT_ROOT
    / "productos"
    / "creacion_usuario_sv"
    / "reporte_quincena"
    / "queries"
    / "trx_quincena.sql"
)

# Configuracion editable (sin argumentos en terminal).
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15
CONFIG_RTM_FECHA_INICIO = "2024-09-01"
CONFIG_VENTANA_CAMPANIA_MESES = 3

CONFIG_HOST = "127.0.0.1"
CONFIG_PORT = 8066


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


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def clasificar_generacion(fecha_nac) -> str:
    if pd.isna(fecha_nac):
        return "OTRA GENERACION"
    anio = int(fecha_nac.year)
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generacion Z (1997-2012)"
    return "OTRA GENERACION"


def normalizar_genero(valor) -> str:
    if pd.isna(valor):
        return "Sin dato"
    texto = str(valor).strip().upper()
    if texto in {"F", "FEMENINO", "MUJER"}:
        return "Mujer"
    if texto in {"M", "MASCULINO", "HOMBRE"}:
        return "Hombre"
    return "Sin dato"


def es_usuario_activo(valor) -> bool:
    if pd.isna(valor):
        return False

    estado = str(valor).strip().upper()
    if estado.endswith(".0") and estado[:-2].isdigit():
        estado = estado[:-2]

    if "INACT" in estado:
        return False

    activos = {"A", "ACTIVO", "ACTIVE", "1", "TRUE", "T", "HABILITADO", "VIGENTE"}
    return estado in activos or ("ACT" in estado and "INACT" not in estado)


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


def cargar_bases(fecha_inicio: date, fecha_fin_exclusiva: date) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fecha_rtm_inicio = pd.to_datetime(CONFIG_RTM_FECHA_INICIO)
    fecha_rtm_fin_exclusiva = pd.to_datetime(fecha_fin_exclusiva)

    conversion = run_query_file(
        str(RUTA_CONVERSION),
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )
    rtm = run_query_file(
        str(RUTA_RTM),
        params={
            "fecha_rtm_inicio": fecha_rtm_inicio.isoformat(),
            "fecha_rtm_fin_exclusiva": fecha_rtm_fin_exclusiva.isoformat(),
        },
    )
    logins = run_query_file(
        str(RUTA_LOGINS),
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )
    trx = run_query_file(
        str(RUTA_TRX),
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )

    return conversion, rtm, logins, trx


def preparar_cohorte(conversion: pd.DataFrame, rtm: pd.DataFrame) -> pd.DataFrame:
    conv = conversion.copy()
    camp = rtm.copy()

    conv["fecha_creacion_usuario"] = pd.to_datetime(conv["fecha_creacion_usuario"], errors="coerce")
    conv["fecha_nacimiento_usuario"] = pd.to_datetime(conv["fecha_nacimiento_usuario"], errors="coerce")
    conv = conv[conv["fecha_creacion_usuario"].notna()].copy()

    conv["generacion"] = conv["fecha_nacimiento_usuario"].apply(clasificar_generacion)
    conv["genero"] = conv["genero_cliente"].apply(normalizar_genero)
    conv["es_usuario_activo"] = conv["estado_usuario"].apply(es_usuario_activo)

    conv["id_usuario"] = conv["nombre_usuario"].astype("string").str.strip()
    conv.loc[conv["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    conv.loc[conv["id_usuario"] == "", "id_usuario"] = pd.NA

    conv["fecha"] = conv["fecha_creacion_usuario"].dt.date
    conv["codigo_cliente_usuario_creado"] = conv["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)

    camp["codigo_cliente_usuario_campania"] = camp["codigo_cliente_usuario_campania"].apply(normalizar_codigo_cliente)
    camp["fecha_campania"] = pd.to_datetime(camp["fecha_campania"], errors="coerce")

    camp_match = (
        camp[["codigo_cliente_usuario_campania", "fecha_campania"]]
        .dropna(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .drop_duplicates(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .copy()
    )

    df_merge = conv.merge(
        camp_match,
        how="left",
        left_on="codigo_cliente_usuario_creado",
        right_on="codigo_cliente_usuario_campania",
    )

    fecha_creacion = df_merge["fecha_creacion_usuario"].dt.normalize()
    fecha_campania = df_merge["fecha_campania"].dt.normalize()
    fecha_campania_mas_ventana = fecha_campania + pd.DateOffset(months=CONFIG_VENTANA_CAMPANIA_MESES)

    df_merge["match_campania"] = (
        fecha_campania.notna()
        & (fecha_creacion >= fecha_campania)
        & (fecha_creacion <= fecha_campania_mas_ventana)
    )

    cohorte = (
        df_merge.groupby("id_usuario", as_index=False)
        .agg(
            fecha=("fecha", "min"),
            codigo_cliente_usuario_creado=("codigo_cliente_usuario_creado", "first"),
            genero=("genero", "first"),
            generacion=("generacion", "first"),
            es_usuario_activo=("es_usuario_activo", lambda s: bool(s.fillna(False).any())),
            medio=("match_campania", lambda s: "Medios propios" if s.fillna(False).any() else "Producto"),
        )
        .copy()
    )

    cohorte = cohorte[cohorte["id_usuario"].notna()].copy()
    return cohorte


def preparar_logins(logins_df: pd.DataFrame) -> pd.DataFrame:
    logins = logins_df.copy()
    if logins.empty:
        return logins

    logins["codigo_cliente_login"] = logins["codigo_cliente_login"].apply(normalizar_codigo_cliente)
    logins = logins[logins["codigo_cliente_login"] != ""].copy()
    logins["fecha_login"] = pd.to_datetime(logins["fecha_login"], errors="coerce")
    logins = logins[logins["fecha_login"].notna()].copy()
    return logins


def preparar_trx(trx_df: pd.DataFrame) -> pd.DataFrame:
    trx = trx_df.copy()
    if trx.empty:
        return trx

    trx["codigo_cliente_transaccion"] = trx["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    trx = trx[trx["codigo_cliente_transaccion"] != ""].copy()
    trx["fecha_transaccion"] = pd.to_datetime(trx["fecha_transaccion"], errors="coerce")
    trx = trx[trx["fecha_transaccion"].notna()].copy()
    return trx


def cruzar_eventos(cohorte: pd.DataFrame, logins: pd.DataFrame, trx: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mapa_cod_usuario = (
        cohorte[["codigo_cliente_usuario_creado", "id_usuario"]]
        .dropna(subset=["codigo_cliente_usuario_creado", "id_usuario"])
        .drop_duplicates()
    )

    logins_match = logins.merge(
        mapa_cod_usuario,
        how="inner",
        left_on="codigo_cliente_login",
        right_on="codigo_cliente_usuario_creado",
    )

    trx_match = trx.merge(
        mapa_cod_usuario,
        how="inner",
        left_on="codigo_cliente_transaccion",
        right_on="codigo_cliente_usuario_creado",
    )

    return logins_match, trx_match


def grafico_genero(cohorte: pd.DataFrame) -> go.Figure:
    if cohorte.empty:
        return figura_vacia("Sin datos en el rango")

    resumen = (
        cohorte.groupby("genero", as_index=False)["id_usuario"]
        .nunique()
        .rename(columns={"id_usuario": "usuarios"})
        .sort_values("usuarios", ascending=False)
    )

    colores_map = {
        "Mujer": COLORES["aqua_digital"],
        "Hombre": COLORES["amarillo_opt"],
        "Sin dato": COLORES["azul_financiero"],
    }
    colores = [colores_map.get(lbl, COLORES["gris_texto"]) for lbl in resumen["genero"].tolist()]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=resumen["genero"],
                values=resumen["usuarios"],
                hole=0.45,
                marker=dict(colors=colores),
                textinfo="label+percent",
                hovertemplate="%{label}<br>Usuarios: %{value:,}<br>Participacion: %{percent}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Usuarios creados por genero",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig


def grafico_generacion(cohorte: pd.DataFrame) -> go.Figure:
    if cohorte.empty:
        return figura_vacia("Sin datos en el rango")

    orden = [
        "Generation X (1965-1980)",
        "Gen Y - Millennials (1981-1996)",
        "Generacion Z (1997-2012)",
        "OTRA GENERACION",
    ]

    resumen = (
        cohorte.groupby("generacion", as_index=False)["id_usuario"]
        .nunique()
        .rename(columns={"id_usuario": "usuarios"})
        .set_index("generacion")
        .reindex(orden, fill_value=0)
        .reset_index()
    )

    colores = [
        COLORES["azul_experto"],
        COLORES["aqua_digital"],
        COLORES["amarillo_opt"],
        COLORES["azul_financiero"],
    ]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=resumen["generacion"],
                values=resumen["usuarios"],
                hole=0.45,
                marker=dict(colors=colores),
                textinfo="label+percent",
                hovertemplate="%{label}<br>Usuarios: %{value:,}<br>Participacion: %{percent}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Usuarios creados por generacion",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig


def grafico_creacion_diaria(cohorte: pd.DataFrame, fecha_inicio: date, fecha_fin_exclusiva: date) -> go.Figure:
    if cohorte.empty:
        return figura_vacia("Sin datos en el rango")

    dias = pd.date_range(fecha_inicio, fecha_fin_exclusiva - timedelta(days=1), freq="D")
    diario = cohorte.groupby("fecha")["id_usuario"].nunique()
    valores = [int(diario.get(d.date(), 0)) for d in dias]

    fig = go.Figure(
        data=[
            go.Bar(
                x=[d.date().isoformat() for d in dias],
                y=valores,
                marker_color=COLORES["azul_experto"],
                text=[f"{v:,}" if v > 0 else "" for v in valores],
                textposition="outside",
                hovertemplate="Fecha: %{x}<br>Usuarios creados: %{y:,}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Creacion diaria de usuarios",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=60, l=40, r=10),
        xaxis=dict(title="Fecha", tickangle=-30),
        yaxis=dict(title="Usuarios"),
    )
    return fig


def grafico_logins_vs_trx(logins_match: pd.DataFrame, trx_match: pd.DataFrame, fecha_inicio: date, fecha_fin_exclusiva: date) -> go.Figure:
    dias = pd.date_range(fecha_inicio, fecha_fin_exclusiva - timedelta(days=1), freq="D")

    if logins_match.empty:
        logins_diario = pd.Series(dtype="int64")
    else:
        logins_diario = logins_match.groupby(logins_match["fecha_login"].dt.date)["id_usuario"].size()

    if trx_match.empty:
        trx_diario = pd.Series(dtype="int64")
    else:
        trx_diario = trx_match.groupby(trx_match["fecha_transaccion"].dt.date)["id_usuario"].size()

    valores_logins = [int(logins_diario.get(d.date(), 0)) for d in dias]
    valores_trx = [int(trx_diario.get(d.date(), 0)) for d in dias]

    if sum(valores_logins) == 0 and sum(valores_trx) == 0:
        return figura_vacia("Sin logins ni trx en el rango")

    fig = go.Figure(
        data=[
            go.Bar(
                name="Logins",
                x=[d.date().isoformat() for d in dias],
                y=valores_logins,
                marker_color=COLORES["aqua_digital"],
                hovertemplate="Fecha: %{x}<br>Logins: %{y:,}<extra></extra>",
            ),
            go.Bar(
                name="TRX",
                x=[d.date().isoformat() for d in dias],
                y=valores_trx,
                marker_color=COLORES["amarillo_opt"],
                hovertemplate="Fecha: %{x}<br>TRX: %{y:,}<extra></extra>",
            ),
        ]
    )
    fig.update_layout(
        barmode="group",
        title="Logins vs TRX por dia",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=50, b=60, l=40, r=10),
        xaxis=dict(title="Fecha", tickangle=-30),
        yaxis=dict(title="Eventos"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


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


def graph_export_config(filename: str) -> dict:
    return {
        "displaylogo": False,
        "toImageButtonOptions": {
            "format": "png",
            "filename": filename,
            "scale": 6,
        },
        "modeBarButtonsToAdd": ["toImage"],
    }


def construir_layout(
    cohorte: pd.DataFrame,
    logins_match: pd.DataFrame,
    trx_match: pd.DataFrame,
    periodo: str,
    fecha_inicio: date,
    fecha_fin_exclusiva: date,
) -> html.Div:
    total_usuarios = int(cohorte["id_usuario"].nunique()) if not cohorte.empty else 0
    usuarios_activos = int(cohorte.loc[cohorte["es_usuario_activo"] == True, "id_usuario"].nunique()) if not cohorte.empty else 0

    usuarios_con_login = int(logins_match["id_usuario"].nunique()) if not logins_match.empty else 0
    total_logins = int(len(logins_match))

    usuarios_con_trx = int(trx_match["id_usuario"].nunique()) if not trx_match.empty else 0
    total_trx = int(len(trx_match))

    return html.Div(
        id="dashboard-root",
        style={"padding": "30px", "backgroundColor": COLORES["gris_fondo"], "fontFamily": "Arial, sans-serif"},
        children=[
            html.H2("Creacion Usuario SV - Dashboard Quincenal", style={"color": COLORES["azul_experto"], "marginBottom": "6px"}),
            html.P(
                f"Periodo evaluado (creacion, logins y trx): {periodo}",
                style={"color": COLORES["gris_texto"], "marginTop": 0},
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "14px",
                    "marginBottom": "18px",
                },
                children=[
                    kpi_card("Usuarios creados", f"{total_usuarios:,}", COLORES["azul_financiero"]),
                    kpi_card("Usuarios activos", f"{usuarios_activos:,}", COLORES["azul_experto"]),
                    kpi_card("Usuarios con login", f"{usuarios_con_login:,}", COLORES["aqua_digital"]),
                    kpi_card("Total logins", f"{total_logins:,}", COLORES["aqua_digital"]),
                    kpi_card("Usuarios con trx", f"{usuarios_con_trx:,}", COLORES["amarillo_opt"]),
                    kpi_card("Total trx", f"{total_trx:,}", COLORES["amarillo_opt"]),
                ],
            ),
            html.Button(
                "Descargar captura completa (PNG alta calidad)",
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
            dcc.Graph(
                figure=grafico_creacion_diaria(cohorte, fecha_inicio, fecha_fin_exclusiva),
                config=graph_export_config("creacion_usuarios_diaria_quincena"),
            ),
            dcc.Graph(
                figure=grafico_logins_vs_trx(logins_match, trx_match, fecha_inicio, fecha_fin_exclusiva),
                config=graph_export_config("logins_vs_trx_diario_quincena"),
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                children=[
                    dcc.Graph(
                        figure=grafico_genero(cohorte),
                        config=graph_export_config("usuarios_por_genero_quincena"),
                    ),
                    dcc.Graph(
                        figure=grafico_generacion(cohorte),
                        config=graph_export_config("usuarios_por_generacion_quincena"),
                    ),
                ],
            ),
        ],
    )


def main() -> None:
    fecha_inicio, fecha_fin_exclusiva = validar_rango(CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN)
    periodo = f"{fecha_inicio.isoformat()} a {(fecha_fin_exclusiva - timedelta(days=1)).isoformat()}"

    print("Cargando dashboard...")
    print(
        "Configuracion -> "
        f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, dia_fin={CONFIG_DIA_FIN}, "
        f"rtm_inicio={CONFIG_RTM_FECHA_INICIO}, ventana_meses={CONFIG_VENTANA_CAMPANIA_MESES}"
    )

    try:
        conversion, rtm, logins_raw, trx_raw = cargar_bases(fecha_inicio, fecha_fin_exclusiva)
        cohorte = preparar_cohorte(conversion, rtm)
        logins = preparar_logins(logins_raw)
        trx = preparar_trx(trx_raw)
        logins_match, trx_match = cruzar_eventos(cohorte, logins, trx)
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo preparando datos: {exc}")
        raise SystemExit(1) from exc

    app = Dash(
        __name__,
        external_scripts=[
            "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js",
        ],
        title="Creacion Usuario SV - Quincena",
    )

    app.layout = construir_layout(cohorte, logins_match, trx_match, periodo, fecha_inicio, fecha_fin_exclusiva)

    app.clientside_callback(
        """
        function(n_clicks){
            if(!n_clicks){ return window.dash_clientside.no_update; }
            var node = document.getElementById('dashboard-root');
            if(!node || typeof html2canvas === 'undefined'){ return 'no-captura'; }
            var maxScale = 6;
            var deviceScale = window.devicePixelRatio || 2;
            var exportScale = Math.min(maxScale, Math.max(3, deviceScale * 2));
            html2canvas(node, {
                scale: exportScale,
                backgroundColor: '#ffffff',
                useCORS: true,
                allowTaint: true,
                imageTimeout: 0,
                windowWidth: document.body.scrollWidth,
                windowHeight: document.body.scrollHeight
            }).then(function(canvas){
                var link = document.createElement('a');
                link.download = 'creacion_usuario_sv_quincena_dashboard.png';
                link.href = canvas.toDataURL('image/png', 1.0);
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
