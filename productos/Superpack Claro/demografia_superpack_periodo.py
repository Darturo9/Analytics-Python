import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_LISTA = BASE_DIR / "exports" / "clientes_contactados_unificados_prioridad_rtm.xlsx"
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
SELECT DISTINCT
    codigo_cliente
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


def normalizar_departamento(value: object) -> str:
    if pd.isna(value):
        return "SIN DATO"
    text = str(value).strip().upper()
    return text if text else "SIN DATO"


def cargar_lista_clientes(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de lista: {path}")

    df = pd.read_excel(path, dtype=str)
    if df.empty:
        raise ValueError("El archivo de lista no contiene filas.")

    col_cliente = seleccionar_columna_cliente(df)
    out = df.copy()
    out["codigo_cliente"] = out[col_cliente].apply(normalizar_codigo_cliente)
    out = out.loc[out["codigo_cliente"].notna(), ["codigo_cliente"]].drop_duplicates()
    return out


def obtener_compradores_superpack(
    fecha_inicio: str,
    fecha_fin_exclusiva: str,
    codigo_superpack: int,
) -> pd.DataFrame:
    params = {
        "fecha_inicio": fecha_inicio,
        "fecha_fin_exclusiva": fecha_fin_exclusiva,
        "codigo_superpack": codigo_superpack,
    }
    return run_query(SQL_COMPRADORES_SUPERPACK, params=params)


def obtener_demografia_por_codigos(codigos: list[str]) -> pd.DataFrame:
    if not codigos:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "fecha_nacimiento", "departamento"])

    codigos_unicos = sorted(
        {
            str(codigo).strip()
            for codigo in codigos
            if pd.notna(codigo) and str(codigo).strip() != ""
        }
    )
    if not codigos_unicos:
        return pd.DataFrame(columns=["codigo_cliente", "genero", "fecha_nacimiento", "departamento"])

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
        return pd.DataFrame(columns=["codigo_cliente", "genero", "fecha_nacimiento", "departamento"])

    df = pd.concat(partes, ignore_index=True)
    df["codigo_cliente"] = df["codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
    df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], errors="coerce")
    df["genero"] = df["genero_raw"].apply(normalizar_genero)
    df["generacion"] = df["fecha_nacimiento"].apply(clasificar_generacion)
    df["departamento"] = df["departamento_raw"].apply(normalizar_departamento)
    df = (
        df[["codigo_cliente", "genero", "generacion", "departamento"]]
        .dropna(subset=["codigo_cliente"])
        .drop_duplicates(subset=["codigo_cliente"], keep="first")
    )
    return df


def construir_tabla_conteo_pct(
    df: pd.DataFrame,
    columna: str,
    total_clientes: int,
    top_n: int | None = None,
) -> pd.DataFrame:
    tabla = (
        df.groupby(columna, as_index=False)["codigo_cliente"]
        .nunique()
        .rename(columns={"codigo_cliente": "cantidad", columna: "categoria"})
        .sort_values(["cantidad", "categoria"], ascending=[False, True])
    )
    if top_n is not None:
        tabla = tabla.head(top_n).copy()
    tabla["porcentaje"] = (
        (tabla["cantidad"] / total_clientes * 100.0).round(2) if total_clientes else 0.0
    )
    return tabla


def imprimir_tabla(titulo: str, tabla: pd.DataFrame) -> None:
    if tabla.empty:
        print(f"{titulo}: sin datos")
        return
    out = tabla.copy()
    out["cantidad"] = out["cantidad"].astype(int).map(lambda x: f"{x:,}")
    out["porcentaje"] = out["porcentaje"].map(lambda x: f"{x:.2f}%")
    print(titulo)
    print(out.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resumen demografico de compradores superpack en lista RTM/PAUTA para un periodo."
    )
    parser.add_argument("--fecha-inicio", default="2026-04-01", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default="2026-05-01",
        help="Fecha fin exclusiva (YYYY-MM-DD).",
    )
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo de servicio Multipago.")
    parser.add_argument(
        "--input-lista",
        default=str(DEFAULT_INPUT_LISTA),
        help="Ruta al Excel con clientes unificados RTM/PAUTA.",
    )
    parser.add_argument(
        "--top-deptos",
        type=int,
        default=5,
        help="Cantidad de departamentos a mostrar en el top.",
    )
    args = parser.parse_args()

    try:
        input_lista = Path(args.input_lista)
        print(f"Leyendo lista unificada: {input_lista}")
        lista = cargar_lista_clientes(input_lista)
        total_clientes_lista = int(lista["codigo_cliente"].nunique())
        print(f"Clientes unicos en lista: {total_clientes_lista:,}")

        print("Consultando compradores de superpack en SQL Server...")
        compradores = obtener_compradores_superpack(
            fecha_inicio=args.fecha_inicio,
            fecha_fin_exclusiva=args.fecha_fin_exclusiva,
            codigo_superpack=args.codigo_superpack,
        )
        compradores["codigo_cliente"] = compradores["codigo_cliente"].astype(str).apply(normalizar_codigo_cliente)
        compradores = compradores.dropna(subset=["codigo_cliente"]).drop_duplicates(subset=["codigo_cliente"])
        total_compradores_universo = int(compradores["codigo_cliente"].nunique())
        print(f"Compradores unicos superpack en universo: {total_compradores_universo:,}")

        compradores_lista = lista.merge(
            compradores[["codigo_cliente"]],
            on="codigo_cliente",
            how="inner",
        ).drop_duplicates(subset=["codigo_cliente"])
        total_compradores_lista = int(compradores_lista["codigo_cliente"].nunique())

        print("\n===== DEMOGRAFIA COMPRADORES SUPERPACK =====")
        print(f"Periodo: {args.fecha_inicio} a {args.fecha_fin_exclusiva} (fin exclusiva)")
        print(f"Codigo superpack: {args.codigo_superpack}")
        print(f"Clientes unicos en lista: {total_clientes_lista:,}")
        print(f"Clientes unicos compradores en lista: {total_compradores_lista:,}")

        if total_compradores_lista == 0:
            print("No hay compras de superpack para los clientes de la lista en el periodo indicado.")
            print("============================================")
            return

        print(
            "Consultando demografia en SQL Server "
            "(lectura en lote de clientes compradores, no cliente por cliente)..."
        )
        demografia = obtener_demografia_por_codigos(compradores_lista["codigo_cliente"].tolist())
        base = compradores_lista.merge(demografia, on="codigo_cliente", how="left")
        base["genero"] = base["genero"].fillna("SIN DATO")
        base["generacion"] = base["generacion"].fillna("SIN DATO")
        base["departamento"] = base["departamento"].fillna("SIN DATO")

        tabla_genero = construir_tabla_conteo_pct(base, "genero", total_compradores_lista)
        tabla_generacion = construir_tabla_conteo_pct(base, "generacion", total_compradores_lista)
        tabla_departamento = construir_tabla_conteo_pct(
            base,
            "departamento",
            total_compradores_lista,
            top_n=max(1, int(args.top_deptos)),
        )

        print("------------------------------------------------")
        imprimir_tabla("\nDistribucion por genero:", tabla_genero)
        imprimir_tabla("\nDistribucion por generacion:", tabla_generacion)
        imprimir_tabla(f"\nTop {max(1, int(args.top_deptos))} departamentos:", tabla_departamento)
        print("============================================")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
