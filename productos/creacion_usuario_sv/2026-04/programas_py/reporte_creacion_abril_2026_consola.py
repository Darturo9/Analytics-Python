"""
Reporte en consola: Creacion de Usuario SV - Abril 2026.

Uso:
    python3 productos/creacion_usuario_sv/2026-04/programas_py/reporte_creacion_abril_2026_consola.py
"""

from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.db import run_query_file

BASE_DIR = Path(__file__).resolve().parents[1]


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame]:
    conv = run_query_file(str(BASE_DIR / "queries" / "conversion_abril_2026.sql"))
    rtm = run_query_file(str(BASE_DIR / "queries" / "comunicacionesRTM_abril_2026.sql"))
    return conv, rtm


def preparar_datos(conv: pd.DataFrame, rtm: pd.DataFrame) -> pd.DataFrame:
    conv = conv.copy()
    rtm = rtm.copy()

    conv["fecha_creacion_usuario"] = pd.to_datetime(conv["fecha_creacion_usuario"], errors="coerce")
    conv = conv[conv["fecha_creacion_usuario"].notna()].copy()

    conv["id_usuario"] = conv["nombre_usuario"].astype("string").str.strip()
    conv.loc[conv["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    conv.loc[conv["id_usuario"] == "", "id_usuario"] = pd.NA

    conv["anio"] = conv["fecha_creacion_usuario"].dt.year
    conv["mes"] = conv["fecha_creacion_usuario"].dt.month
    conv["dia"] = conv["fecha_creacion_usuario"].dt.day

    conv["codigo_cliente_usuario_creado"] = conv["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    rtm["codigo_cliente_usuario_campania"] = rtm["codigo_cliente_usuario_campania"].apply(normalizar_codigo_cliente)
    rtm["fecha_campania"] = pd.to_datetime(rtm["fecha_campania"], errors="coerce")

    rtm_match = (
        rtm[["codigo_cliente_usuario_campania", "fecha_campania"]]
        .dropna(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .drop_duplicates(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .copy()
    )

    df_merge = conv.merge(
        rtm_match,
        how="left",
        left_on="codigo_cliente_usuario_creado",
        right_on="codigo_cliente_usuario_campania",
    )

    fecha_creacion = df_merge["fecha_creacion_usuario"].dt.normalize()
    fecha_campania = df_merge["fecha_campania"].dt.normalize()
    fecha_campania_mas_3m = fecha_campania + pd.DateOffset(months=3)

    df_merge["match_campania"] = (
        fecha_campania.notna()
        & (fecha_creacion >= fecha_campania)
        & (fecha_creacion <= fecha_campania_mas_3m)
    )

    df = (
        df_merge.groupby(["anio", "mes", "id_usuario"], as_index=False)
        .agg(
            dia=("dia", "min"),
            fecha_creacion_usuario=("fecha_creacion_usuario", "min"),
            direccion_lvl_1=("direccion_lvl_1", "first"),
            estado_usuario=("estado_usuario", "first"),
            estado_cliente=("estado_cliente", "first"),
            medio=("match_campania", lambda s: "Medios propios" if s.fillna(False).any() else "Producto"),
        )
        .copy()
    )

    df = df[df["id_usuario"].notna()].copy()
    df["fecha"] = df["fecha_creacion_usuario"].dt.date
    return df


def imprimir_resumen(df: pd.DataFrame) -> None:
    print("=" * 72)
    print("CREACION DE USUARIO SV - ABRIL 2026 (Consola)")
    print("Rango: 2026-04-01 a 2026-04-30")
    print("=" * 72)

    total_usuarios = int(df["id_usuario"].nunique())
    print(f"Total usuarios unicos creados (RECDIST nombre_usuario): {total_usuarios:,}")

    print("\n--- Distribucion por medio ---")
    medio_counts = df.groupby("medio")["id_usuario"].nunique().sort_values(ascending=False)
    for medio, val in medio_counts.items():
        pct = (val / total_usuarios * 100) if total_usuarios else 0
        print(f"{medio:15} {val:>8,}  ({pct:5.2f}%)")

    print("\n--- Creaciones por dia (top 10) ---")
    diario = (
        df.groupby("fecha")["id_usuario"]
        .nunique()
        .sort_values(ascending=False)
        .head(10)
    )
    for fecha, val in diario.items():
        print(f"{fecha}  {val:>8,}")

    print("\n--- Top 10 direccion_lvl_1 ---")
    top_geo1 = (
        df["direccion_lvl_1"].fillna("SIN_DATO").astype(str).str.strip().replace("", "SIN_DATO")
    )
    top_geo1 = top_geo1.value_counts().head(10)
    for geo, val in top_geo1.items():
        print(f"{geo[:30]:30} {val:>8,}")

    print("\n--- Estado usuario (USSTAT) ---")
    estado_usuario = df["estado_usuario"].fillna("SIN_DATO").astype(str).str.strip().replace("", "SIN_DATO").value_counts()
    for estado, val in estado_usuario.items():
        print(f"{estado:15} {val:>8,}")

    print("\n--- Estado cliente (CLSTAT) ---")
    estado_cliente = df["estado_cliente"].fillna("SIN_DATO").astype(str).str.strip().replace("", "SIN_DATO").value_counts()
    for estado, val in estado_cliente.items():
        print(f"{estado:15} {val:>8,}")


if __name__ == "__main__":
    conversion_df, rtm_df = cargar_bases()
    data = preparar_datos(conversion_df, rtm_df)
    imprimir_resumen(data)
