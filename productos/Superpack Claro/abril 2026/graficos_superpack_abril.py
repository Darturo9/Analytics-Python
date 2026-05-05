"""
graficos_superpack_abril.py
-----------------------------
Genera grafico web interactivo para Superpack Claro - Abril 2026:
  1. Transacciones por dia

Nota: el grafico por hora no esta disponible porque DW_FECHA_OPERACION_SP
es tipo DATE en la base de datos (no almacena la hora).

Requisito: pip install plotly

Ejecucion:
    python3 "productos/Superpack Claro/abril 2026/graficos_superpack_abril.py"
"""

import sys
from pathlib import Path

import plotly.graph_objects as go
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from core.db import run_query_file

BASE_DIR   = Path(__file__).resolve().parent
QUERY_DIA  = BASE_DIR / "queries" / "grafico_trx_por_dia_abril_2026.sql"
QUERY_HORA = BASE_DIR / "queries" / "grafico_trx_por_hora_abril_2026.sql"

COLOR_BARRA = "#1f6fbf"


def grafico_por_dia(df) -> None:
    if df.empty:
        print("Sin datos para grafico por dia.")
        return

    fechas = [str(f) for f in df["fecha"]]
    trx    = df["total_transacciones"].astype(int).tolist()
    clientes = df["clientes_unicos"].astype(int).tolist()

    fig = go.Figure(go.Bar(
        x=fechas,
        y=trx,
        marker_color=COLOR_BARRA,
        text=[f"{v:,}" for v in trx],
        textposition="outside",
        customdata=clientes,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Transacciones: %{y:,}<br>"
            "Clientes unicos: %{customdata:,}<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(text="Superpack Claro — Transacciones por Día | Abril 2026", font=dict(size=18)),
        xaxis_title="Fecha",
        yaxis_title="Transacciones",
        xaxis=dict(tickangle=-45),
        yaxis=dict(tickformat=","),
        plot_bgcolor="white",
        bargap=0.25,
        margin=dict(t=80, b=80),
    )
    fig.update_yaxes(gridcolor="#e0e0e0")

    print("  Abriendo grafico por dia en el navegador...")
    fig.show()


def grafico_por_hora(df) -> None:
    if df.empty:
        print("Sin datos para grafico por hora.")
        return

    import pandas as pd
    horas_completas = list(range(24))
    df_hora = df.set_index("hora").reindex(horas_completas, fill_value=0).reset_index()
    df_hora.columns = ["hora", "total_transacciones", "clientes_unicos"]

    horas    = [f"{int(h):02d}:00" for h in df_hora["hora"]]
    trx      = df_hora["total_transacciones"].astype(int).tolist()
    clientes = df_hora["clientes_unicos"].astype(int).tolist()

    fig = go.Figure(go.Bar(
        x=horas,
        y=trx,
        marker_color=COLOR_BARRA,
        text=[f"{v:,}" if v > 0 else "" for v in trx],
        textposition="outside",
        customdata=clientes,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Transacciones: %{y:,}<br>"
            "Clientes unicos: %{customdata:,}<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=dict(text="Superpack Claro — Transacciones por Hora del Día | Abril 2026", font=dict(size=18)),
        xaxis_title="Hora",
        yaxis_title="Transacciones",
        xaxis=dict(tickangle=-45),
        yaxis=dict(tickformat=","),
        plot_bgcolor="white",
        bargap=0.25,
        margin=dict(t=80, b=80),
    )
    fig.update_yaxes(gridcolor="#e0e0e0")

    print("  Abriendo grafico por hora en el navegador...")
    fig.show()


def main() -> None:
    try:
        print("Cargando datos por dia...")
        df_dia = run_query_file(str(QUERY_DIA))
        df_dia.columns = [c.lower() for c in df_dia.columns]

        print("Generando grafico por dia...")
        grafico_por_dia(df_dia)
        print("Listo.")
        print("\nNota: grafico por hora no disponible — DW_FECHA_OPERACION_SP es tipo DATE (sin hora).")

    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        lower = msg.lower()
        if "permission was denied" in lower:
            print("[ERROR] Permiso denegado. Solicita permiso SELECT al DBA.")
        elif "login timeout" in lower or "could not open a connection" in lower:
            print("[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales.")
        else:
            print(f"[ERROR] {msg}")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
