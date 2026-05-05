"""
validar_superpack_febrero_canales.py
-------------------------------------
Validacion de compras de Superpack Claro en febrero 2026 por canal de contacto.

Lee la lista unificada de clientes contactados (RTM / PAUTA) y cruza contra
las compras reales del Superpack en el mes para determinar conversion por canal.

Ejecucion:
    python3 "productos/Superpack Claro/febrero 2026/validar_superpack_febrero_canales.py"

Argumentos opcionales:
    --fecha-inicio   YYYY-MM-DD  (default: 2026-02-01)
    --fecha-fin      YYYY-MM-DD  (default: 2026-03-01, exclusiva)
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query


BASE_DIR = Path(__file__).resolve().parent
SUPERPACK_DIR = BASE_DIR.parent
QUERY_PATH = BASE_DIR / "queries" / "compras_superpack_febrero_2026.sql"
INPUT_CLIENTES = SUPERPACK_DIR / "exports" / "clientes_contactados_unificados_prioridad_rtm.xlsx"
OUTPUT_JSON = BASE_DIR / "exports" / "validacion_superpack_febrero_canales.json"

PREFERRED_COLUMNS = (
    "codigo_cliente",
    "cod_cliente",
    "cif",
    "cliente",
    "codigo",
)


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def normalizar_codigo_cliente(value: object) -> object:
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return pd.NA

    text = re.sub(r"\.0$", "", text)
    letter_match = re.search(r"[A-Za-z]", text)
    if letter_match:
        if letter_match.start() == 0:
            return pd.NA
        text = text[:letter_match.start()]

    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^0-9]", "", text)
    if text == "":
        return pd.NA

    return text.zfill(8)[-8:]


def seleccionar_columna_cliente(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    cols_map = {str(c).strip().lower(): c for c in cols}

    for pref in PREFERRED_COLUMNS:
        if pref in cols_map:
            return cols_map[pref]

    for col in cols:
        low = str(col).strip().lower()
        if "cif" in low or "cliente" in low or ("codigo" in low and "producto" not in low):
            return col

    raise ValueError(
        "No se pudo detectar la columna de cliente automaticamente. "
        f"Columnas encontradas: {cols}"
    )


def seleccionar_columna_origen(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    cols_map = {str(c).strip().lower(): c for c in cols}
    if "origen" in cols_map:
        return cols_map["origen"]
    if "canal" in cols_map:
        return cols_map["canal"]
    raise ValueError(
        "No se encontro columna de origen/canal en el archivo unificado. "
        f"Columnas encontradas: {cols}"
    )


def normalizar_origen(value: object) -> str:
    if pd.isna(value):
        return "SIN_ORIGEN"
    text = str(value).strip().upper()
    if text in ("RTM", "PAUTA"):
        return text
    return "SIN_ORIGEN"


def normalizar_canal_compra(canal_raw: object, canal_codigo: object) -> str:
    text = "" if pd.isna(canal_raw) else str(canal_raw).strip().upper()
    if "APP" in text or text in ("AP", "APP"):
        return "APP"
    if "WEB" in text or text in ("IB",):
        return "WEB"
    if text in ("", "SIN_DATO", "NULL", "NONE", "NAN"):
        return "SIN_DATO"
    return text


def consolidar_canales_cliente(series: pd.Series) -> str:
    valores = sorted(set(v for v in series.dropna().astype(str).tolist() if v and v != "SIN_DATO"))
    if not valores:
        return "SIN_DATO"
    if "APP" in valores and "WEB" in valores:
        return "MIXTO"
    if len(valores) == 1:
        return valores[0]
    return "+".join(valores)


def cargar_clientes_unificados(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de clientes unificados: {path}")

    df = pd.read_excel(path, dtype=str)
    if df.empty:
        raise ValueError("El archivo de clientes unificados no contiene filas.")

    col_cliente = seleccionar_columna_cliente(df)
    col_origen = seleccionar_columna_origen(df)

    base = df.copy()
    base["codigo_cliente"] = base[col_cliente].apply(normalizar_codigo_cliente)
    base["origen"] = base[col_origen].apply(normalizar_origen)
    base = base.loc[base["codigo_cliente"].notna(), ["codigo_cliente", "origen"]].copy()

    base["prioridad"] = base["origen"].map({"RTM": 0, "PAUTA": 1}).fillna(9)
    base = base.sort_values(["codigo_cliente", "prioridad", "origen"], ascending=[True, True, True])
    base = base.drop_duplicates(subset=["codigo_cliente"], keep="first")
    return base[["codigo_cliente", "origen"]]


def cargar_compras_superpack(query_path: Path, fecha_inicio: str, fecha_fin_exclusiva: str) -> pd.DataFrame:
    if not query_path.exists():
        raise FileNotFoundError(f"No existe la query: {query_path}")

    sql = query_path.read_text(encoding="utf-8")
    compras = run_query(sql, params={"fecha_inicio": fecha_inicio, "fecha_fin_exclusiva": fecha_fin_exclusiva})
    if compras.empty:
        return pd.DataFrame(
            columns=[
                "codigo_cliente",
                "fecha_operacion",
                "codigo_superpack",
                "canal_compra",
                "monto_operacion",
                "es_reversa",
                "trx_weight",
                "weighted_monto",
            ]
        )

    compras = compras.copy()
    compras["codigo_cliente"] = compras["padded_codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
    compras["fecha_operacion"] = pd.to_datetime(compras["fecha_operacion"], errors="coerce").dt.date
    compras["canal_compra"] = compras.apply(
        lambda row: normalizar_canal_compra(
            row.get("canal_operacion_raw"),
            row.get("canal_operacion_codigo"),
        ),
        axis=1,
    )
    compras["monto_operacion"] = pd.to_numeric(compras["monto_operacion"], errors="coerce").fillna(0.0)
    compras["es_reversa"] = compras["es_reversa"].fillna("N").astype(str).str.strip().str.upper()
    compras["trx_weight"] = compras["es_reversa"].apply(lambda x: -1 if x == "S" else 1)
    compras["weighted_monto"] = compras.apply(
        lambda r: -r["monto_operacion"] if r["es_reversa"] == "S" else r["monto_operacion"], axis=1
    )
    compras = compras.loc[
        compras["codigo_cliente"].notna(),
        ["codigo_cliente", "fecha_operacion", "codigo_superpack", "canal_compra",
         "monto_operacion", "es_reversa", "trx_weight", "weighted_monto"],
    ].copy()
    return compras


def preparar_validacion(clientes: pd.DataFrame, compras: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if compras.empty:
        resumen_compras = pd.DataFrame(
            columns=["codigo_cliente", "total_tx", "monto_total_tx", "canal_compra_cliente"]
        )
    else:
        resumen_tx = (
            compras.groupby("codigo_cliente", as_index=False)
            .agg(
                total_tx=("trx_weight", "sum"),
                monto_total_tx=("weighted_monto", "sum"),
            )
            .sort_values(["total_tx", "monto_total_tx"], ascending=[False, False])
        )
        resumen_canal_cliente = (
            compras.groupby("codigo_cliente", as_index=False)
            .agg(canal_compra_cliente=("canal_compra", consolidar_canales_cliente))
        )
        resumen_compras = resumen_tx.merge(resumen_canal_cliente, on="codigo_cliente", how="left")

    detalle = clientes.merge(resumen_compras, on="codigo_cliente", how="left")
    detalle["compro_superpack"] = (detalle["total_tx"].notna() & (detalle["total_tx"] > 0)).astype(int)
    detalle["total_tx"] = pd.to_numeric(detalle["total_tx"], errors="coerce").fillna(0).astype(int)
    detalle["monto_total_tx"] = pd.to_numeric(detalle["monto_total_tx"], errors="coerce").fillna(0.0)
    detalle["canal_compra_cliente"] = detalle["canal_compra_cliente"].fillna("NO_COMPRA")
    return detalle, resumen_compras


def imprimir_resumen(detalle: pd.DataFrame, compras: pd.DataFrame) -> None:
    resumen_canal = (
        detalle.groupby("origen", as_index=False)
        .agg(
            clientes_unicos_lista=("codigo_cliente", "nunique"),
            clientes_que_compraron=("compro_superpack", "sum"),
            monto_total_clientes=("monto_total_tx", "sum"),
        )
        .sort_values("origen")
    )
    resumen_canal["clientes_que_no_compraron"] = (
        resumen_canal["clientes_unicos_lista"] - resumen_canal["clientes_que_compraron"]
    )

    total_lista = int(detalle["codigo_cliente"].nunique())
    total_compraron_lista = int(detalle["compro_superpack"].sum())
    total_no_compraron_lista = total_lista - total_compraron_lista

    total_clientes_compradores_universo = int(compras["codigo_cliente"].nunique()) if not compras.empty else 0
    total_tx_universo = int(compras["trx_weight"].sum()) if not compras.empty else 0
    monto_total_universo = float(compras["weighted_monto"].sum()) if not compras.empty else 0.0

    print("\n===== RESUMEN VALIDACION SUPERPACK FEBRERO 2026 =====")
    print(f"Clientes unicos en lista unificada: {total_lista:,}")
    print(f"Clientes de la lista que compraron: {total_compraron_lista:,}")
    print(f"Clientes de la lista que no compraron: {total_no_compraron_lista:,}")
    print(f"Universo total compradores febrero (no solo lista): {total_clientes_compradores_universo:,}")
    print(f"Universo total transacciones febrero: {total_tx_universo:,}")
    print(f"Universo monto total febrero: {monto_total_universo:,.2f}")
    print("------------------------------------------------------")

    print("Resumen por canal (lista unificada):")
    tabla = resumen_canal.copy()
    tabla["clientes_unicos_lista"] = tabla["clientes_unicos_lista"].map(lambda x: f"{int(x):,}")
    tabla["clientes_que_compraron"] = tabla["clientes_que_compraron"].map(lambda x: f"{int(x):,}")
    tabla["clientes_que_no_compraron"] = tabla["clientes_que_no_compraron"].map(lambda x: f"{int(x):,}")
    tabla["monto_total_clientes"] = tabla["monto_total_clientes"].map(lambda x: f"{float(x):,.2f}")
    print(tabla.to_string(index=False))

    print("\nResumen por canal de compra (universo febrero):")
    if compras.empty:
        print("Sin transacciones para clasificar canal de compra.")
    else:
        resumen_canal_compra = (
            compras.groupby("canal_compra", as_index=False)
            .agg(
                clientes_unicos=("codigo_cliente", "nunique"),
                total_tx=("trx_weight", "sum"),
                monto_total=("weighted_monto", "sum"),
            )
            .sort_values("canal_compra")
        )
        tabla_canal = resumen_canal_compra.copy()
        tabla_canal["clientes_unicos"] = tabla_canal["clientes_unicos"].map(lambda x: f"{int(x):,}")
        tabla_canal["total_tx"] = tabla_canal["total_tx"].map(lambda x: f"{int(x):,}")
        tabla_canal["monto_total"] = tabla_canal["monto_total"].map(lambda x: f"{float(x):,.2f}")
        print(tabla_canal.to_string(index=False))

    print("\nCruce origen (RTM/PAUTA) vs canal de compra (solo quienes compraron):")
    compradores_lista = detalle.loc[detalle["compro_superpack"] == 1].copy()
    if compradores_lista.empty:
        print("Ningun cliente de la lista compro superpack en febrero.")
    else:
        cruce = (
            compradores_lista.groupby(["origen", "canal_compra_cliente"], as_index=False)
            .agg(
                clientes_unicos=("codigo_cliente", "nunique"),
                total_tx=("total_tx", "sum"),
                monto_total=("monto_total_tx", "sum"),
            )
            .sort_values(["origen", "canal_compra_cliente"])
        )
        tabla_cruce = cruce.copy()
        tabla_cruce["clientes_unicos"] = tabla_cruce["clientes_unicos"].map(lambda x: f"{int(x):,}")
        tabla_cruce["total_tx"] = tabla_cruce["total_tx"].map(lambda x: f"{int(x):,}")
        tabla_cruce["monto_total"] = tabla_cruce["monto_total"].map(lambda x: f"{float(x):,.2f}")
        print(tabla_cruce.to_string(index=False))
    print("======================================================\n")


def construir_payload_json(detalle: pd.DataFrame, compras: pd.DataFrame) -> dict:
    total_lista = int(detalle["codigo_cliente"].nunique())
    total_compraron = int(detalle["compro_superpack"].sum())

    resumen_canal = (
        detalle.groupby("origen", as_index=False)
        .agg(
            clientes_en_lista=("codigo_cliente", "nunique"),
            clientes_que_compraron=("compro_superpack", "sum"),
            monto_total=("monto_total_tx", "sum"),
        )
    )
    resumen_canal["clientes_que_no_compraron"] = (
        resumen_canal["clientes_en_lista"] - resumen_canal["clientes_que_compraron"]
    )

    por_canal_contacto = [
        {
            "origen": row["origen"],
            "clientes_en_lista": int(row["clientes_en_lista"]),
            "clientes_que_compraron": int(row["clientes_que_compraron"]),
            "clientes_que_no_compraron": int(row["clientes_que_no_compraron"]),
            "monto_total": round(float(row["monto_total"]), 2),
        }
        for _, row in resumen_canal.iterrows()
    ]

    por_canal_compra = []
    cruce_origen_canal = []
    if not compras.empty:
        por_canal_compra = [
            {
                "canal_compra": row["canal_compra"],
                "clientes_unicos": int(row["clientes_unicos"]),
                "total_tx": int(row["total_tx"]),
                "monto_total": round(float(row["monto_total"]), 2),
            }
            for _, row in (
                compras.groupby("canal_compra", as_index=False)
                .agg(
                    clientes_unicos=("codigo_cliente", "nunique"),
                    total_tx=("trx_weight", "sum"),
                    monto_total=("weighted_monto", "sum"),
                )
                .iterrows()
            )
        ]
        compradores_lista = detalle.loc[detalle["compro_superpack"] == 1]
        if not compradores_lista.empty:
            cruce = (
                compradores_lista.groupby(["origen", "canal_compra_cliente"], as_index=False)
                .agg(
                    clientes_unicos=("codigo_cliente", "nunique"),
                    total_tx=("total_tx", "sum"),
                    monto_total=("monto_total_tx", "sum"),
                )
            )
            cruce_origen_canal = [
                {
                    "origen": row["origen"],
                    "canal_compra": row["canal_compra_cliente"],
                    "clientes_unicos": int(row["clientes_unicos"]),
                    "total_tx": int(row["total_tx"]),
                    "monto_total": round(float(row["monto_total"]), 2),
                }
                for _, row in cruce.iterrows()
            ]

    return {
        "periodo": "febrero_2026",
        "generado_en": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "resumen_general": {
            "clientes_en_lista": total_lista,
            "clientes_que_compraron": total_compraron,
            "clientes_que_no_compraron": total_lista - total_compraron,
            "universo_compradores_febrero": int(compras["codigo_cliente"].nunique()) if not compras.empty else 0,
            "universo_transacciones_febrero": int(compras["trx_weight"].sum()) if not compras.empty else 0,
            "universo_monto_total_febrero": round(float(compras["weighted_monto"].sum()) if not compras.empty else 0.0, 2),
        },
        "por_canal_contacto": por_canal_contacto,
        "por_canal_compra": por_canal_compra,
        "cruce_origen_canal_compra": cruce_origen_canal,
    }


def exportar_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validacion compras Superpack febrero 2026 por canal de contacto."
    )
    parser.add_argument("--fecha-inicio", default="2026-02-01", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin",    default="2026-03-01", help="Fecha fin exclusiva (YYYY-MM-DD). Default: 2026-03-01")
    args = parser.parse_args()

    try:
        print(f"Leyendo clientes unificados: {INPUT_CLIENTES}")
        clientes = cargar_clientes_unificados(INPUT_CLIENTES)
        print(f"Clientes unicos listos para validar: {len(clientes):,}")

        print(f"Ejecutando query de compras febrero (hasta {args.fecha_fin} exclusiva): {QUERY_PATH}")
        compras = cargar_compras_superpack(QUERY_PATH, args.fecha_inicio, args.fecha_fin)
        print(f"Transacciones de superpack febrero: {len(compras):,}")

        detalle, _ = preparar_validacion(clientes, compras)
        imprimir_resumen(detalle, compras)

        payload = construir_payload_json(detalle, compras)
        exportar_json(payload, OUTPUT_JSON)
        print(f"JSON exportado: {OUTPUT_JSON}")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
