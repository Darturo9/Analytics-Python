"""
graficos_superpack_abril.py
-----------------------------
Genera dos graficos PNG para Superpack Claro - Abril 2026:
  1. Transacciones por dia
  2. Transacciones por hora del dia

Salida: abril 2026/exports/

Ejecucion:
    python3 "productos/Superpack Claro/abril 2026/graficos_superpack_abril.py"
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from core.db import run_query_file

BASE_DIR   = Path(__file__).resolve().parent
QUERY_DIA  = BASE_DIR / "queries" / "grafico_trx_por_dia_abril_2026.sql"
QUERY_HORA = BASE_DIR / "queries" / "grafico_trx_por_hora_abril_2026.sql"
EXPORTS    = BASE_DIR / "exports"

COLOR_BARS  = "#1f6fbf"
COLOR_LINE  = "#e85d04"


def grafico_por_dia(df) -> None:
    if df.empty:
        print("Sin datos para grafico por dia.")
        return

    fechas = [str(f) for f in df["fecha"]]
    trx    = df["total_transacciones"].astype(int).tolist()

    fig, ax = plt.subplots(figsize=(14, 5))
    bars = ax.bar(fechas, trx, color=COLOR_BARS, width=0.6)

    ax.set_title("Superpack Claro — Transacciones por Día (Abril 2026)", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Fecha", fontsize=11)
    ax.set_ylabel("Transacciones", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.tick_params(axis="x", rotation=45)

    for bar, val in zip(bars, trx):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(trx) * 0.01,
            f"{val:,}",
            ha="center", va="bottom", fontsize=7.5
        )

    ax.set_ylim(0, max(trx) * 1.15)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()

    EXPORTS.mkdir(parents=True, exist_ok=True)
    salida = EXPORTS / "trx_por_dia_abril_2026.png"
    fig.savefig(salida, dpi=150)
    plt.close(fig)
    print(f"  Grafico guardado: {salida}")


def grafico_por_hora(df) -> None:
    if df.empty:
        print("Sin datos para grafico por hora.")
        return

    # Rellenar horas sin transacciones con 0
    horas_completas = list(range(24))
    df_hora = df.set_index("hora").reindex(horas_completas, fill_value=0).reset_index()
    df_hora.columns = ["hora", "total_transacciones", "clientes_unicos"]

    horas = [f"{int(h):02d}:00" for h in df_hora["hora"]]
    trx   = df_hora["total_transacciones"].astype(int).tolist()

    fig, ax = plt.subplots(figsize=(14, 5))
    bars = ax.bar(horas, trx, color=COLOR_BARS, width=0.6)

    ax.set_title("Superpack Claro — Transacciones por Hora del Día (Abril 2026)", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Hora", fontsize=11)
    ax.set_ylabel("Transacciones", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.tick_params(axis="x", rotation=45)

    max_trx = max(trx) if max(trx) > 0 else 1
    for bar, val in zip(bars, trx):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max_trx * 0.01,
                f"{val:,}",
                ha="center", va="bottom", fontsize=7.5
            )

    ax.set_ylim(0, max_trx * 1.15)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()

    EXPORTS.mkdir(parents=True, exist_ok=True)
    salida = EXPORTS / "trx_por_hora_abril_2026.png"
    fig.savefig(salida, dpi=150)
    plt.close(fig)
    print(f"  Grafico guardado: {salida}")


def main() -> None:
    try:
        print("Cargando datos por dia...")
        df_dia = run_query_file(str(QUERY_DIA))
        df_dia.columns = [c.lower() for c in df_dia.columns]

        print("Cargando datos por hora...")
        df_hora = run_query_file(str(QUERY_HORA))
        df_hora.columns = [c.lower() for c in df_hora.columns]

        print("Generando graficos...")
        grafico_por_dia(df_dia)
        grafico_por_hora(df_hora)
        print("Listo.")

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
