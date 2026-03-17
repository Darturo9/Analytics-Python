"""
dashboard.py
------------
Dashboard LoginUsuarios - Marzo 2026
Ejecutar desde la raíz del proyecto:

    python3 productos/LoginUsuarios/2026-03/dashboards/dashboard.py
"""

import sys
sys.path.insert(0, ".")

from pathlib import Path

import pandas as pd
from dash import Dash, html, dcc, dash_table
import plotly.graph_objects as go

from core.db import run_query_file
from core.colors import COLORES


RUTA_QUERY_LOGINS = "productos/LoginUsuarios/2026-03/queries/Logins.sql"
RUTA_DATA_CONTACTADOS = Path("productos/LoginUsuarios/2026-03/data")


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_canal(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip().lower()


def normalizar_nombre_columna(nombre: str) -> str:
    return (
        str(nombre)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace(" ", "_")
    )


def detectar_columna_codigo(columnas: list[str]) -> str | None:
    columnas_norm = {c: normalizar_nombre_columna(c) for c in columnas}
    for original, normalizada in columnas_norm.items():
        if "codigo" in normalizada and ("cliente" in normalizada or "usuario" in normalizada):
            return original
    for original, normalizada in columnas_norm.items():
        if normalizada in {"cldoc", "codigo", "cliente"}:
            return original
    return None


def detectar_columna_canal(columnas: list[str]) -> str | None:
    columnas_norm = {c: normalizar_nombre_columna(c) for c in columnas}
    for original, normalizada in columnas_norm.items():
        if "canal" in normalizada:
            return original
    return None


def cargar_contactados() -> tuple[pd.DataFrame, str | None]:
    archivos_preferidos = [
        RUTA_DATA_CONTACTADOS / "Contactados.xlsx",
        RUTA_DATA_CONTACTADOS / "contactados.xlsx",
        RUTA_DATA_CONTACTADOS / "Contactados.csv",
        RUTA_DATA_CONTACTADOS / "contactados.csv",
    ]

    archivo = None
    for candidato in archivos_preferidos:
        if candidato.exists():
            archivo = candidato
            break

    if archivo is None and RUTA_DATA_CONTACTADOS.exists():
        encontrados = sorted(
            p for p in RUTA_DATA_CONTACTADOS.iterdir()
            if p.is_file() and "contact" in p.name.lower() and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
        )
        if encontrados:
            archivo = encontrados[0]

    if archivo is None:
        return pd.DataFrame(columns=["padded_codigo_usuario", "canal_contacto"]), None

    if archivo.suffix.lower() in {".xlsx", ".xls"}:
        df_contactados = pd.read_excel(archivo)
    else:
        df_contactados = pd.read_csv(archivo)

    col_codigo = detectar_columna_codigo(df_contactados.columns.tolist())
    col_canal = detectar_columna_canal(df_contactados.columns.tolist())

    if col_codigo is None:
        print(f"[ADVERTENCIA] No se encontró columna de código en {archivo}.")
        return pd.DataFrame(columns=["padded_codigo_usuario", "canal_contacto"]), str(archivo)

    df_contactados["padded_codigo_usuario"] = df_contactados[col_codigo].apply(normalizar_codigo)
    df_contactados["canal_contacto"] = (
        df_contactados[col_canal].apply(normalizar_canal) if col_canal else ""
    )
    df_contactados = df_contactados[df_contactados["padded_codigo_usuario"].notna()].copy()
    df_contactados = df_contactados[["padded_codigo_usuario", "canal_contacto"]].drop_duplicates()

    return df_contactados, str(archivo)


def construir_figura_vacia(mensaje: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=30, b=40, l=40, r=10),
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
                font=dict(size=14, color=COLORES["gris_texto"]),
            )
        ],
    )
    return fig


def construir_figura_logins_dia(df_logins: pd.DataFrame) -> go.Figure:
    if df_logins.empty:
        return construir_figura_vacia("No hay datos de logins para el período")

    pivot = (
        df_logins.groupby(["dia", "tipo_contacto"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )
    dias = pivot.index.tolist()
    no_contactados = pivot["No contactado"].tolist() if "No contactado" in pivot.columns else [0] * len(dias)
    contactados = pivot["Contactado"].tolist() if "Contactado" in pivot.columns else [0] * len(dias)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="No contactado",
        x=dias,
        y=no_contactados,
        marker_color=COLORES["amarillo_opt"],
        hovertemplate="Día %{x}<br>No contactado: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Contactado",
        x=dias,
        y=contactados,
        marker_color=COLORES["aqua_digital"],
        hovertemplate="Día %{x}<br>Contactado: %{y:,}<extra></extra>",
    ))
    fig.update_layout(
        barmode="stack",
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Día"),
        yaxis=dict(title="Eventos de login"),
        legend_title_text="",
    )
    return fig


def construir_figura_canales(df_logins: pd.DataFrame) -> go.Figure:
    conteos = df_logins["canal_login"].fillna("sin-canal").value_counts()
    if conteos.empty:
        return construir_figura_vacia("No hay datos de canal para mostrar")

    fig = go.Figure(data=[
        go.Bar(
            x=conteos.index.tolist(),
            y=conteos.values.tolist(),
            marker_color=COLORES["azul_experto"],
            text=[f"{int(v):,}" for v in conteos.values.tolist()],
            textposition="outside",
            hovertemplate="Canal %{x}<br>Eventos: %{y:,}<extra></extra>",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=20, b=40, l=40, r=10),
        xaxis=dict(title="Canal login"),
        yaxis=dict(title="Eventos"),
        showlegend=False,
    )
    return fig


def construir_figura_match_canal(df_logins: pd.DataFrame) -> go.Figure:
    df_contactados = df_logins[df_logins["cliente_contactado"]].copy()
    if df_contactados.empty:
        return construir_figura_vacia("No hay clientes contactados en los logins")

    conteos = (
        df_contactados["canal_match_contacto"]
        .map({True: "Canal coincide", False: "Canal no coincide"})
        .value_counts()
    )

    fig = go.Figure(data=[
        go.Pie(
            labels=conteos.index.tolist(),
            values=conteos.values.tolist(),
            hole=0.45,
            marker=dict(colors=[COLORES["aqua_digital"], COLORES["amarillo_opt"]]),
            texttemplate="%{label}<br>%{value:,} (%{percent})",
            textposition="outside",
        )
    ])
    fig.update_layout(
        plot_bgcolor=COLORES["blanco"],
        paper_bgcolor=COLORES["blanco"],
        font=dict(color=COLORES["azul_experto"]),
        margin=dict(t=10, b=10, l=10, r=10),
        legend_title_text="",
    )
    return fig


# ── Carga y preparación de datos ─────────────────────────────────────────────
print("Cargando logins...")
df = run_query_file(RUTA_QUERY_LOGINS)
print(f"  {len(df)} eventos de login cargados")

df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
df = df[df["fecha_inicio"].notna()].copy()
df["dia"] = df["fecha_inicio"].dt.day
df["canal_login_norm"] = df["canal_login"].apply(normalizar_canal)
df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
df["id_usuario"] = df["nombre_usuario"].fillna("").astype(str).str.strip()
df.loc[df["id_usuario"] == "", "id_usuario"] = df["padded_codigo_usuario"]

contactados, ruta_contactados = cargar_contactados()
if ruta_contactados:
    print(f"Cargando contactados desde: {ruta_contactados}")
    print(f"  {contactados['padded_codigo_usuario'].nunique():,} clientes contactados cargados")
else:
    print("No se encontró archivo Contactados. Se mostrará dashboard sin clasificación de contactados.")

set_contactados = set(contactados["padded_codigo_usuario"].dropna().tolist())
set_par_codigo_canal = set(
    (codigo, canal)
    for codigo, canal in contactados[["padded_codigo_usuario", "canal_contacto"]].dropna().itertuples(index=False, name=None)
)

df["cliente_contactado"] = df["padded_codigo_usuario"].isin(set_contactados)
df["tipo_contacto"] = df["cliente_contactado"].map({True: "Contactado", False: "No contactado"})
df["canal_match_contacto"] = df.apply(
    lambda r: (r["padded_codigo_usuario"], r["canal_login_norm"]) in set_par_codigo_canal,
    axis=1,
)

total_eventos = int(df.shape[0])
usuarios_unicos = int(df["id_usuario"].nunique())
eventos_contactados = int(df[df["cliente_contactado"]].shape[0])
usuarios_contactados_con_login = int(df.loc[df["cliente_contactado"], "id_usuario"].nunique())

df_contactados_tabla = (
    df[df["cliente_contactado"]]
    .copy()
    .sort_values("fecha_inicio", ascending=False)
    .head(200)
)
df_contactados_tabla["fecha_inicio"] = df_contactados_tabla["fecha_inicio"].dt.strftime("%Y-%m-%d %H:%M:%S")


# ── App ───────────────────────────────────────────────────────────────────────
card_style = {
    "backgroundColor": COLORES["blanco"],
    "borderRadius": "8px",
    "padding": "20px 24px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "borderTop": f"4px solid {COLORES['aqua_digital']}",
}

app = Dash(__name__)

app.layout = html.Div(
    style={"fontFamily": "Arial", "backgroundColor": COLORES["gris_fondo"], "minHeight": "100vh", "padding": "24px"},
    children=[
        html.H2("LoginUsuarios — Marzo 2026", style={"color": COLORES["azul_experto"], "marginBottom": "10px"}),
        html.P(
            f"Archivo de contactados: {ruta_contactados if ruta_contactados else 'No encontrado'}",
            style={"color": COLORES["gris_texto"], "marginTop": 0, "marginBottom": "20px"},
        ),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "16px", "marginBottom": "24px"},
            children=[
                html.Div(style=card_style, children=[
                    html.P("Eventos de login", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{total_eventos:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_experto"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
                    html.P("Usuarios únicos con login", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{usuarios_unicos:,}", style={"margin": "8px 0 0 0", "color": COLORES["azul_financiero"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.P("Eventos de clientes contactados", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{eventos_contactados:,}", style={"margin": "8px 0 0 0", "color": COLORES["aqua_digital"]}),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['amarillo_opt']}"}, children=[
                    html.P("Usuarios contactados con login", style={"margin": 0, "color": COLORES["gris_texto"], "fontSize": "14px"}),
                    html.H2(f"{usuarios_contactados_con_login:,}", style={"margin": "8px 0 0 0", "color": COLORES["amarillo_opt"]}),
                ]),
            ],
        ),

        html.Div(style={**card_style, "marginBottom": "20px"}, children=[
            html.H4("Eventos de login por día", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dcc.Graph(figure=construir_figura_logins_dia(df)),
        ]),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))", "gap": "16px", "marginBottom": "20px"},
            children=[
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_experto']}"}, children=[
                    html.H4("Eventos por canal de login", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(figure=construir_figura_canales(df)),
                ]),
                html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['aqua_digital']}"}, children=[
                    html.H4("Clientes contactados: match de canal", style={"color": COLORES["azul_experto"], "marginTop": 0}),
                    dcc.Graph(figure=construir_figura_match_canal(df)),
                ]),
            ],
        ),

        html.Div(style={**card_style, "borderTop": f"4px solid {COLORES['azul_financiero']}"}, children=[
            html.H4("Últimos logins de clientes contactados (top 200)", style={"color": COLORES["azul_experto"], "marginTop": 0}),
            dash_table.DataTable(
                columns=[
                    {"name": "Fecha login", "id": "fecha_inicio"},
                    {"name": "Código cliente", "id": "padded_codigo_usuario"},
                    {"name": "Usuario", "id": "nombre_usuario"},
                    {"name": "Canal login", "id": "canal_login"},
                    {"name": "Canal match", "id": "canal_match_contacto"},
                ],
                data=df_contactados_tabla[
                    ["fecha_inicio", "padded_codigo_usuario", "nombre_usuario", "canal_login", "canal_match_contacto"]
                ].to_dict("records"),
                page_size=12,
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "fontFamily": "Arial", "fontSize": "13px"},
                style_header={"backgroundColor": COLORES["azul_experto"], "color": COLORES["blanco"], "fontWeight": "bold"},
                style_data={"backgroundColor": COLORES["blanco"], "color": COLORES["azul_experto"]},
            ),
        ]),
    ],
)


if __name__ == "__main__":
    print("Dashboard LoginUsuarios corriendo en http://127.0.0.1:8054")
    app.run(debug=False, use_reloader=False, port=8054)
