"""
demografia_superpack_abril_promo_claro.py
-----------------------------------------
Resumen demografico de compradores del Superpack Claro en abril 2026,
exclusivamente para los clientes que aparecen en la lista de contactados
por promo Claro (no incluye RTM).

Muestra en consola:
- Distribucion por genero
- Distribucion por generacion
- Top 5 departamentos

Ejecucion:
    python3 "productos/Superpack Claro/abril 2026/demografia_superpack_abril_promo_claro.py"
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path


import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query


BASE_DIR      = Path(__file__).resolve().parent
SUPERPACK_DIR = BASE_DIR.parent

INPUT_LISTA_UNIFICADA = SUPERPACK_DIR / "exports" / "clientes_contactados_unificados_prioridad_rtm.xlsx"
OUTPUT_JSON           = BASE_DIR / "exports" / "demografia_superpack_abril_promo_claro.json"

FECHA_INICIO          = "2026-04-01"
FECHA_FIN_EXCLUSIVA   = "2026-05-01"
CODIGO_SUPERPACK      = 498
TOP_DEPTOS            = 5

PREFERRED_COLUMNS = (
    "codigo_cliente",
    "cod_cliente",
    "cif",
    "cliente",
    "codigo",
)

SQL_COMPRADORES_SUPERPACK = """
WITH trx_superpack AS (
    SELECT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )),
            8
        ) AS codigo_cliente
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
)
SELECT DISTINCT codigo_cliente
FROM trx_superpack
WHERE codigo_cliente IS NOT NULL
"""

# Plantilla compatible con SQL Server 2012+. Los {placeholders} se reemplazan
# en Python con los codigos del lote antes de ejecutar.
SQL_DEMOGRAFIA_BATCH = """
SELECT
    RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS codigo_cliente,
    MAX(c.CLISEX)                                  AS genero_raw,
    MAX(CAST(c.DW_FECHA_NACIMIENTO AS DATE))        AS fecha_nacimiento,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(d.dw_nivel_geo2)), ''), 'SIN DATO')) AS departamento_raw
FROM DW_CIF_CLIENTES c
LEFT JOIN DW_CIF_DIRECCIONES_PRINCIPAL d
    ON RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) =
       RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
WHERE RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) IN ({placeholders})
GROUP BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
"""

DEMOGRAFIA_BATCH_SIZE = 500


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
        text = text[: letter_match.start()]
    text = re.sub(r"[^0-9]", "", text)
    return text.zfill(8)[-8:] if text else pd.NA


def seleccionar_columna_cliente(df: pd.DataFrame) -> str:
    cols_map = {str(c).strip().lower(): c for c in df.columns}
    for pref in PREFERRED_COLUMNS:
        if pref in cols_map:
            return cols_map[pref]
    for col in df.columns:
        low = str(col).strip().lower()
        if "cif" in low or "cliente" in low or ("codigo" in low and "producto" not in low):
            return col
    raise ValueError(
        f"No se pudo detectar la columna de cliente. Columnas: {list(df.columns)}"
    )


def normalizar_genero(value: object) -> str:
    if pd.isna(value):
        return "SIN DATO"
    text = str(value).strip().upper()
    if text in ("F", "FEMENINO", "MUJER"):
        return "MUJER"
    if text in ("M", "H", "MASCULINO", "HOMBRE"):
        return "HOMBRE"
    return "SIN DATO"


def clasificar_generacion(fecha_nacimiento: object) -> str:
    if pd.isna(fecha_nacimiento):
        return "SIN DATO"
    anio = int(fecha_nacimiento.year)
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generacion Z (1997-2012)"
    return "OTRA GENERACION"


def cargar_lista_promo_claro() -> pd.DataFrame:
    if not INPUT_LISTA_UNIFICADA.exists():
        raise FileNotFoundError(f"No existe el archivo unificado: {INPUT_LISTA_UNIFICADA}")
    df = pd.read_excel(INPUT_LISTA_UNIFICADA, dtype=str)
    if df.empty:
        raise ValueError("El archivo unificado no contiene filas.")
    col_cliente = seleccionar_columna_cliente(df)
    out = df[[col_cliente]].copy()
    out["codigo_cliente"] = out[col_cliente].apply(normalizar_codigo_cliente)
    return out.loc[out["codigo_cliente"].notna(), ["codigo_cliente"]].drop_duplicates()


def obtener_compradores_abril() -> pd.DataFrame:
    params = {
        "fecha_inicio": FECHA_INICIO,
        "fecha_fin_exclusiva": FECHA_FIN_EXCLUSIVA,
        "codigo_superpack": CODIGO_SUPERPACK,
    }
    df = run_query(SQL_COMPRADORES_SUPERPACK, params=params)
    df["codigo_cliente"] = df["codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
    return df.dropna(subset=["codigo_cliente"]).drop_duplicates(subset=["codigo_cliente"])


def obtener_demografia(codigos: list[str]) -> pd.DataFrame:
    if not codigos:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])

    codigos_unicos = sorted({str(c).strip() for c in codigos if pd.notna(c) and str(c).strip()})
    if not codigos_unicos:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])

    # Consulta en lotes para compatibilidad con SQL Server 2012+
    lotes = [
        codigos_unicos[i : i + DEMOGRAFIA_BATCH_SIZE]
        for i in range(0, len(codigos_unicos), DEMOGRAFIA_BATCH_SIZE)
    ]
    partes = []
    for lote in lotes:
        placeholders = ", ".join(f"'{c}'" for c in lote)
        sql = SQL_DEMOGRAFIA_BATCH.format(placeholders=placeholders)
        df_lote = run_query(sql)
        if not df_lote.empty:
            partes.append(df_lote)

    if not partes:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])

    df = pd.concat(partes, ignore_index=True)
    df["codigo_cliente"]   = df["codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
    df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], errors="coerce")
    df["genero"]           = df["genero_raw"].apply(normalizar_genero)
    df["generacion"]       = df["fecha_nacimiento"].apply(clasificar_generacion)
    df["departamento"]     = df["departamento_raw"].fillna("SIN DATO").str.strip().str.upper().replace("", "SIN DATO")
    return (
        df[["codigo_cliente", "genero", "generacion", "departamento"]]
        .dropna(subset=["codigo_cliente"])
        .drop_duplicates(subset=["codigo_cliente"], keep="first")
    )


def imprimir_tabla(titulo: str, df: pd.DataFrame, col_categoria: str, total: int) -> None:
    tabla = (
        df.groupby(col_categoria, as_index=False)["codigo_cliente"]
        .nunique()
        .rename(columns={"codigo_cliente": "cantidad", col_categoria: "categoria"})
        .sort_values("cantidad", ascending=False)
    )
    if col_categoria == "departamento":
        tabla = tabla.head(TOP_DEPTOS)

    print(f"\n{titulo}")
    print(f"  {'Categoria':<35} {'Clientes':>10} {'%':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*8}")
    for _, row in tabla.iterrows():
        pct = row["cantidad"] / total * 100 if total else 0
        print(f"  {str(row['categoria']):<35} {int(row['cantidad']):>10,} {pct:>7.1f}%")


def construir_tabla_dict(df: pd.DataFrame, col: str, total: int, top_n: int | None = None) -> list[dict]:
    tabla = (
        df.groupby(col, as_index=False)["codigo_cliente"]
        .nunique()
        .rename(columns={"codigo_cliente": "cantidad", col: "categoria"})
        .sort_values("cantidad", ascending=False)
    )
    if top_n is not None:
        tabla = tabla.head(top_n)
    return [
        {
            "categoria": row["categoria"],
            "clientes": int(row["cantidad"]),
            "porcentaje": round(row["cantidad"] / total * 100, 2) if total else 0.0,
        }
        for _, row in tabla.iterrows()
    ]


def exportar_json_demografia(base: pd.DataFrame, total_lista: int, total_compradores: int, path: Path) -> None:
    payload = {
        "periodo": "abril_2026",
        "generado_en": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "fuente": "promo_claro",
        "resumen_general": {
            "clientes_en_lista_promo": total_lista,
            "compradores": total_compradores,
            "conversion_pct": round(total_compradores / total_lista * 100, 2) if total_lista else 0.0,
        },
        "genero": construir_tabla_dict(base, "genero", total_compradores),
        "generacion": construir_tabla_dict(base, "generacion", total_compradores),
        "top_departamentos": construir_tabla_dict(base, "departamento", total_compradores, top_n=TOP_DEPTOS),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    try:
        print(f"Leyendo lista unificada (RTM + PAUTA): {INPUT_LISTA_UNIFICADA}")
        lista = cargar_lista_promo_claro()
        total_lista = lista["codigo_cliente"].nunique()
        print(f"Clientes unicos en lista unificada (RTM + PAUTA): {total_lista:,}")

        print("Consultando compradores de abril 2026 en SQL Server...")
        compradores = obtener_compradores_abril()
        print(f"Compradores unicos superpack (universo abril): {compradores['codigo_cliente'].nunique():,}")

        compradores_en_lista = (
            lista.merge(compradores[["codigo_cliente"]], on="codigo_cliente", how="inner")
            .drop_duplicates(subset=["codigo_cliente"])
        )
        total_compradores = compradores_en_lista["codigo_cliente"].nunique()

        if total_compradores == 0:
            print("\n[INFO] No hay compradores de superpack en la lista promo Claro para abril 2026.")
            return

        print("Consultando demografia en SQL Server...")
        demografia = obtener_demografia(compradores_en_lista["codigo_cliente"].tolist())
        base = compradores_en_lista.merge(demografia, on="codigo_cliente", how="left")
        base["genero"]       = base["genero"].fillna("SIN DATO")
        base["generacion"]   = base["generacion"].fillna("SIN DATO")
        base["departamento"] = base["departamento"].fillna("SIN DATO")

        print("\n============================================================")
        print(" DEMOGRAFIA COMPRADORES SUPERPACK - ABRIL 2026 (PROMO CLARO)")
        print("============================================================")
        print(f"  Periodo:                  {FECHA_INICIO} al 30-04-2026")
        print(f"  Clientes en lista promo:  {total_lista:,}")
        print(f"  Compradores del periodo:  {total_compradores:,}")
        print(f"  Conversion:               {total_compradores / total_lista * 100:.1f}%")

        imprimir_tabla("Genero:", base, "genero", total_compradores)
        imprimir_tabla("Generacion:", base, "generacion", total_compradores)
        imprimir_tabla(f"Top {TOP_DEPTOS} Departamentos:", base, "departamento", total_compradores)

        print("\n============================================================\n")

        exportar_json_demografia(base, total_lista, total_compradores, OUTPUT_JSON)
        print(f"JSON exportado: {OUTPUT_JSON}")

    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except SQLAlchemyError as exc:
        print(f"[ERROR] Fallo en SQL Server: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
