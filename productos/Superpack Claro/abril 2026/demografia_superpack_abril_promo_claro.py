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
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query


BASE_DIR     = Path(__file__).resolve().parent
SUPERPACK_DIR = BASE_DIR.parent

INPUT_PROMO_CLARO = SUPERPACK_DIR / "inputs" / "clientes Contactados promo Claro.xlsx"

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

SQL_DEMOGRAFIA_POR_CODIGOS = """
WITH codigos AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM([value])), 8) AS codigo_cliente
    FROM OPENJSON(CAST(:codigos_json AS NVARCHAR(MAX)))
    WHERE [value] IS NOT NULL
      AND LTRIM(RTRIM([value])) <> ''
),
clientes_base AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS codigo_cliente,
        c.CLISEX AS genero_raw,
        CAST(c.DW_FECHA_NACIMIENTO AS DATE) AS fecha_nacimiento
    FROM DW_CIF_CLIENTES c
    INNER JOIN codigos k
        ON RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) = k.codigo_cliente
),
direcciones_base AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS codigo_cliente,
        COALESCE(NULLIF(LTRIM(RTRIM(d.dw_nivel_geo2)), ''), 'SIN DATO') AS departamento_raw
    FROM DW_CIF_DIRECCIONES_PRINCIPAL d
    INNER JOIN codigos k
        ON RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) = k.codigo_cliente
)
SELECT
    k.codigo_cliente,
    MAX(c.genero_raw)        AS genero_raw,
    MAX(c.fecha_nacimiento)  AS fecha_nacimiento,
    MAX(d.departamento_raw)  AS departamento_raw
FROM codigos k
LEFT JOIN clientes_base  c ON c.codigo_cliente = k.codigo_cliente
LEFT JOIN direcciones_base d ON d.codigo_cliente = k.codigo_cliente
GROUP BY k.codigo_cliente
"""


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
    if not INPUT_PROMO_CLARO.exists():
        raise FileNotFoundError(f"No existe el archivo: {INPUT_PROMO_CLARO}")
    df = pd.read_excel(INPUT_PROMO_CLARO, dtype=str)
    if df.empty:
        raise ValueError("El archivo de promo Claro no contiene filas.")
    col = seleccionar_columna_cliente(df)
    out = df[[col]].copy()
    out["codigo_cliente"] = out[col].apply(normalizar_codigo_cliente)
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
    params = {"codigos_json": json.dumps(sorted(set(codigos)))}
    df = run_query(SQL_DEMOGRAFIA_POR_CODIGOS, params=params)
    if df.empty:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])
    df["codigo_cliente"]  = df["codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
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


def main() -> None:
    try:
        print(f"Leyendo lista promo Claro: {INPUT_PROMO_CLARO}")
        lista = cargar_lista_promo_claro()
        total_lista = lista["codigo_cliente"].nunique()
        print(f"Clientes unicos en lista promo Claro: {total_lista:,}")

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
