"""
demografia_superpack_abril_universo.py
---------------------------------------
Resumen demografico de TODOS los compradores del Superpack Claro en abril 2026,
sin filtrar por lista de contactados (universo completo).

Filtros estandar:
  - Codigo Superpack: 498 (Claro)
  - Canales banca electronica: SPCPCO IN (1, 7)
  - Solo lempiras: CLMOCO IN ('001', 'L')
  - Excluye reversas: sppafr = 'N'
  - Excluye juridicos: CLTIPE <> 'J'

Muestra en consola:
  - Distribucion por genero
  - Distribucion por generacion
  - Top 5 departamentos

Ejecucion:
    python3 "productos/Superpack Claro/abril 2026/demografia_superpack_abril_universo.py"
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query, get_engine

FECHA_INICIO_DEFAULT        = "2026-04-01"
FECHA_FIN_EXCLUSIVA_DEFAULT = "2026-05-01"
CODIGO_SUPERPACK            = 498
TOP_DEPTOS                  = 5

SQL_COMPRADORES = """
WITH trx_superpack AS (
    SELECT
        ClientesBel.CLCCLI AS codigo_cliente
    FROM dw_mul_sppadat p
    LEFT JOIN dw_mul_spmaco m
        ON m.spcodc = p.spcodc
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
            LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
    ) ClientesBel
        ON LTRIM(RTRIM(p.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
    LEFT JOIN (
        SELECT LTRIM(RTRIM(CLDOC)) CLDOC, CLTIPE,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(CLDOC))
                ORDER BY CASE WHEN CLTIPE = 'N' THEN 1 WHEN CLTIPE IS NULL THEN 2 ELSE 3 END
            ) AS RN
        FROM DW_CIF_CLIENTES
    ) CIF ON CIF.CLDOC = ClientesBel.CLCCLI AND CIF.RN = 1
    WHERE p.DW_FECHA_OPERACION_SP >= :fecha_inicio
      AND p.DW_FECHA_OPERACION_SP <  :fecha_fin_exclusiva
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
      AND p.sppafr = 'N'
      AND ClientesBel.CLCCLI IS NOT NULL
      AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
)
SELECT DISTINCT codigo_cliente
FROM trx_superpack
WHERE codigo_cliente IS NOT NULL
"""

SQL_DEMOGRAFIA = """
SELECT
    RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)                         AS codigo_cliente,
    MAX(c.CLISEX)                                                          AS genero_raw,
    MAX(CAST(c.DW_FECHA_NACIMIENTO AS DATE))                               AS fecha_nacimiento,
    MAX(COALESCE(NULLIF(LTRIM(RTRIM(d.dw_nivel_geo2)), ''), 'SIN DATO'))  AS departamento_raw
FROM #tmp_dem_codigos t
INNER JOIN DW_CIF_CLIENTES c
    ON RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) = t.codigo
LEFT JOIN DW_CIF_DIRECCIONES_PRINCIPAL d
    ON RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) =
       RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
GROUP BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
"""


def normalizar_genero(value: object) -> str:
    if pd.isna(value):
        return "SIN DATO"
    text_val = str(value).strip().upper()
    if text_val in ("F", "FEMENINO", "MUJER"):
        return "MUJER"
    if text_val in ("M", "H", "MASCULINO", "HOMBRE"):
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


def obtener_compradores(fecha_inicio: str, fecha_fin_exclusiva: str) -> list[str]:
    params = {
        "fecha_inicio":        fecha_inicio,
        "fecha_fin_exclusiva": fecha_fin_exclusiva,
        "codigo_superpack":    CODIGO_SUPERPACK,
    }
    df = run_query(SQL_COMPRADORES, params=params)
    df.columns = [c.lower() for c in df.columns]
    return df["codigo_cliente"].dropna().astype(str).str.strip().unique().tolist()


def obtener_demografia(codigos: list[str]) -> pd.DataFrame:
    codigos_unicos = sorted({c for c in codigos if c})
    if not codigos_unicos:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE #tmp_dem_codigos (codigo VARCHAR(8))"))
        conn.execute(
            text("INSERT INTO #tmp_dem_codigos (codigo) VALUES (:codigo)"),
            [{"codigo": c} for c in codigos_unicos],
        )
        df = pd.read_sql(text(SQL_DEMOGRAFIA), conn)

    if df.empty:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "generacion", "departamento"])

    df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], errors="coerce")
    df["genero"]           = df["genero_raw"].apply(normalizar_genero)
    df["generacion"]       = df["fecha_nacimiento"].apply(clasificar_generacion)
    df["departamento"]     = (
        df["departamento_raw"].fillna("SIN DATO").str.strip().str.upper().replace("", "SIN DATO")
    )
    return df[["codigo_cliente", "genero", "generacion", "departamento"]].drop_duplicates(
        subset=["codigo_cliente"], keep="first"
    )


def imprimir_tabla(titulo: str, df: pd.DataFrame, col: str, total: int, top_n: int | None = None) -> None:
    tabla = (
        df.groupby(col, as_index=False)["codigo_cliente"]
        .nunique()
        .rename(columns={"codigo_cliente": "cantidad", col: "categoria"})
        .sort_values("cantidad", ascending=False)
    )
    if top_n is not None:
        tabla = tabla.head(top_n)

    print(f"\n{titulo}")
    print(f"  {'Categoria':<35} {'Clientes':>10} {'%':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*8}")
    for _, row in tabla.iterrows():
        pct = row["cantidad"] / total * 100 if total else 0
        print(f"  {str(row['categoria']):<35} {int(row['cantidad']):>10,} {pct:>7.1f}%")


def main() -> None:
    try:
        print(f"Consultando compradores Superpack Claro abril 2026...")
        codigos = obtener_compradores(FECHA_INICIO_DEFAULT, FECHA_FIN_EXCLUSIVA_DEFAULT)
        total_compradores = len(codigos)
        print(f"Compradores unicos encontrados: {total_compradores:,}")

        if total_compradores == 0:
            print("[INFO] Sin compradores para el periodo indicado.")
            return

        print("Consultando demografia...")
        df = obtener_demografia(codigos)
        df["codigo_cliente"] = df["codigo_cliente"].astype(str)

        print("\n" + "=" * 60)
        print(" DEMOGRAFIA COMPRADORES SUPERPACK - ABRIL 2026 (UNIVERSO)")
        print(" (todos los compradores, sin filtro de lista)")
        print("=" * 60)
        print(f"  Periodo:                  {FECHA_INICIO_DEFAULT} — {FECHA_FIN_EXCLUSIVA_DEFAULT} (exclusiva)")
        print(f"  Total compradores:        {total_compradores:,}")

        imprimir_tabla("Genero:", df, "genero", total_compradores)
        imprimir_tabla("Generacion:", df, "generacion", total_compradores)
        imprimir_tabla(f"Top {TOP_DEPTOS} Departamentos:", df, "departamento", total_compradores, top_n=TOP_DEPTOS)

        print("\n" + "=" * 60 + "\n")

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
