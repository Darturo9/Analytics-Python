"""
resultados_quincenales_superpack.py
====================================
Mide como va la compra de Superpack Claro en una quincena.
1 sola consulta a BD — demografia incluida en la query principal.

Imprime en consola:
  - Compras totales, clientes unicos, monto total
  - Monto mas comprado del periodo
  - Clientes unicos con compras > 120 L
  - Top 5 dias y top 5 horas de mas compras
  - Distribucion por genero
  - Top 3 generaciones
  - Top 5 departamentos

Fechas configuradas en FECHA_INICIO / FECHA_FIN_EXCLUSIVA al inicio del archivo.
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query_file

BASE_DIR   = Path(__file__).resolve().parents[1]
RUTA_QUERY = BASE_DIR / "queries" / "compras_superpack_quincenal.sql"

# ── Quincena a analizar ───────────────────────────────────────────────────────
FECHA_INICIO        = "2026-05-01"   # inclusivo
FECHA_FIN_EXCLUSIVA = "2026-05-16"   # exclusivo
# ─────────────────────────────────────────────────────────────────────────────

CANAL_MAP = {1: "APP", 7: "WEB"}


def normalizar_canal(codigo) -> str:
    try:
        return CANAL_MAP.get(int(codigo), "OTRO")
    except (ValueError, TypeError):
        return "SIN_DATO"


def normalizar_genero(value) -> str:
    if pd.isna(value):
        return "SIN DATO"
    t = str(value).strip().upper()
    if t in ("F", "FEMENINO", "MUJER"):
        return "MUJER"
    if t in ("M", "H", "MASCULINO", "HOMBRE"):
        return "HOMBRE"
    return "SIN DATO"


def clasificar_generacion(fecha_nac) -> str:
    if pd.isna(fecha_nac):
        return "SIN DATO"
    anio = int(fecha_nac.year)
    if 1965 <= anio <= 1980:
        return "Gen X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Gen Z (1997-2012)"
    return "OTRA"


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["fecha_operacion"]  = pd.to_datetime(out["fecha_operacion"], errors="coerce").dt.date
    out["monto_operacion"]  = pd.to_numeric(out["monto_operacion"], errors="coerce").fillna(0.0)
    out["canal"]            = out["canal_operacion_codigo"].apply(normalizar_canal)
    out["fecha_nacimiento"] = pd.to_datetime(out["fecha_nacimiento"], errors="coerce")
    out["genero"]           = out["genero_raw"].apply(normalizar_genero)
    out["generacion"]       = out["fecha_nacimiento"].apply(clasificar_generacion)
    out["departamento"]     = out["departamento"].fillna("SIN DATO").str.strip().str.upper()
    return out


def tabla_conteo(df: pd.DataFrame, col: str, total: int, top_n: int | None = None) -> pd.DataFrame:
    t = (
        df.groupby(col, as_index=False)["padded_codigo_cliente"]
        .nunique()
        .rename(columns={"padded_codigo_cliente": "cantidad", col: "categoria"})
        .sort_values("cantidad", ascending=False)
    )
    if top_n:
        t = t.head(top_n)
    t["pct"] = (t["cantidad"] / total * 100).round(1).astype(str) + "%"
    return t.reset_index(drop=True)


def imprimir_consola(trx: pd.DataFrame) -> None:
    compras = trx.copy()

    total_trx      = len(compras)
    total_clientes = compras["padded_codigo_cliente"].nunique()
    total_monto    = compras["monto_operacion"].sum()

    monto_top = (
        compras.groupby("monto_operacion").size()
        .sort_values(ascending=False)
        .reset_index(name="frecuencia")
        .iloc[0] if not compras.empty else None
    )

    clientes_mayores_120 = compras[compras["monto_operacion"] > 120]["padded_codigo_cliente"].nunique()

    sep = "=" * 56
    print(f"\n{sep}")
    print("  SUPERPACK CLARO — RESULTADOS QUINCENALES")
    print(f"  Periodo: {FECHA_INICIO} a {FECHA_FIN_EXCLUSIVA} (excl.)")
    print(sep)
    print(f"  Compras totales          : {total_trx:,}")
    print(f"  Clientes unicos          : {total_clientes:,}")
    print(f"  Monto total              : L {total_monto:,.2f}")
    if monto_top is not None:
        print(f"  Monto mas comprado       : L {monto_top['monto_operacion']:,.2f} ({int(monto_top['frecuencia']):,} veces)")
    print(f"  Clientes con compras >120: {clientes_mayores_120:,}")
    print(f"{'-' * 56}")

    # Top 5 dias
    top_dias = (
        compras.groupby("fecha_operacion")
        .agg(trx=("padded_codigo_cliente", "count"))
        .reset_index().sort_values("trx", ascending=False).head(5)
    )
    print("\n  Top 5 dias de mas compras:")
    for _, r in top_dias.iterrows():
        print(f"    {str(r['fecha_operacion']):<14} {int(r['trx']):>7,} trx")

    # Top 5 horas
    if "hora_operacion" in compras.columns:
        top_horas = (
            compras.dropna(subset=["hora_operacion"])
            .groupby("hora_operacion")
            .agg(trx=("padded_codigo_cliente", "count"))
            .reset_index().sort_values("trx", ascending=False).head(5)
        )
        print("\n  Top 5 horas de mas compras:")
        for _, r in top_horas.iterrows():
            print(f"    {int(r['hora_operacion']):02d}:00        {int(r['trx']):>7,} trx")

    # Genero
    t_gen = tabla_conteo(compras, "genero", total_clientes)
    print("\n  Genero:")
    for _, r in t_gen.iterrows():
        print(f"    {r['categoria']:<12} {int(r['cantidad']):>7,}  {r['pct']}")

    # Generaciones
    t_generacion = tabla_conteo(compras, "generacion", total_clientes, top_n=3)
    print("\n  Top 3 generaciones:")
    for _, r in t_generacion.iterrows():
        print(f"    {r['categoria']:<30} {int(r['cantidad']):>7,}  {r['pct']}")

    # Deptos
    t_depto = tabla_conteo(compras, "departamento", total_clientes, top_n=5)
    print("\n  Top 5 departamentos:")
    for _, r in t_depto.iterrows():
        print(f"    {r['categoria']:<30} {int(r['cantidad']):>7,}  {r['pct']}")

    print(f"{sep}\n")


def main() -> None:
    try:
        df = run_query_file(
            str(RUTA_QUERY),
            params={"fecha_inicio": FECHA_INICIO, "fecha_fin_exclusiva": FECHA_FIN_EXCLUSIVA},
        )
    except SQLAlchemyError as exc:
        msg = str(exc).lower()
        if "permission was denied" in msg:
            print("[ERROR] Permiso denegado. Solicita acceso al DBA.")
        elif "login timeout expired" in msg or "server not found" in msg:
            print("[ERROR] No se pudo conectar. Verifica red/VPN.")
        else:
            print(f"[ERROR] {exc}")
        sys.exit(1)

    if df.empty:
        print("[AVISO] No hay compras en el rango indicado.")
        sys.exit(0)

    trx = preparar(df)
    imprimir_consola(trx)


if __name__ == "__main__":
    main()
