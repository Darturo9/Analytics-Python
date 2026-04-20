import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query
from core.utils import exportar_excel_multi


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR / "inputs" / "clientes Contactados promo Claro.xlsx"
DEFAULT_OUTPUT = BASE_DIR / "exports" / "validacion_superpack_claro_abril_2026.xlsx"
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
        ) AS padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
)
SELECT
    padded_codigo_cliente,
    COUNT(*) AS total_tx,
    CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total,
    MIN(fecha_operacion) AS primera_fecha_operacion,
    MAX(fecha_operacion) AS ultima_fecha_operacion
FROM trx_superpack
WHERE padded_codigo_cliente IS NOT NULL
GROUP BY padded_codigo_cliente
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def cargar_lista(path: str, sheet: str) -> pd.DataFrame:
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {source}")

    suffix = source.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(source, dtype=str)
    else:
        read_sheet = sheet if sheet else 0
        df = pd.read_excel(source, sheet_name=read_sheet, dtype=str)

    if df.empty:
        raise ValueError("El archivo de entrada no contiene filas.")
    return df


def seleccionar_columna_cliente(df: pd.DataFrame, columna_arg: str) -> str:
    cols = list(df.columns)
    cols_map = {str(c).strip().lower(): c for c in cols}

    if columna_arg:
        key = columna_arg.strip().lower()
        if key in cols_map:
            return cols_map[key]
        raise ValueError(
            f"La columna '{columna_arg}' no existe en el archivo. Columnas disponibles: {cols}"
        )

    for pref in PREFERRED_COLUMNS:
        if pref in cols_map:
            return cols_map[pref]

    for col in cols:
        low = str(col).strip().lower()
        if "cif" in low or "cliente" in low or ("codigo" in low and "producto" not in low):
            return col

    raise ValueError(
        "No se pudo detectar la columna de cliente automaticamente. "
        "Usa --cliente-column con el nombre exacto."
    )


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


def obtener_compradores_superpack(
    fecha_inicio: str, fecha_fin_exclusiva: str, codigo_superpack: int
) -> pd.DataFrame:
    params = {
        "fecha_inicio": fecha_inicio,
        "fecha_fin_exclusiva": fecha_fin_exclusiva,
        "codigo_superpack": codigo_superpack,
    }
    return run_query(SQL_COMPRADORES_SUPERPACK, params=params)


def construir_salidas(
    df_lista: pd.DataFrame,
    col_cliente: str,
    df_compradores: pd.DataFrame,
    fecha_inicio: str,
    fecha_fin_exclusiva: str,
    codigo_superpack: int,
) -> dict[str, pd.DataFrame]:
    work = df_lista.copy()
    work["_fila_input"] = range(1, len(work) + 1)
    work["codigo_cliente_input"] = work[col_cliente]
    work["padded_codigo_cliente"] = work[col_cliente].apply(normalizar_codigo_cliente)
    work["codigo_valido"] = work["padded_codigo_cliente"].notna().astype(int)

    compradores = df_compradores.copy()
    compradores["padded_codigo_cliente"] = compradores["padded_codigo_cliente"].astype(str)

    detalle_lista = work.merge(compradores, on="padded_codigo_cliente", how="left")
    detalle_lista["compro_superpack"] = detalle_lista["total_tx"].notna().astype(int)

    clientes_unicos = (
        work.loc[work["padded_codigo_cliente"].notna(), ["padded_codigo_cliente"]]
        .drop_duplicates()
        .merge(compradores, on="padded_codigo_cliente", how="left")
    )
    clientes_unicos["compro_superpack"] = clientes_unicos["total_tx"].notna().astype(int)

    clientes_match = clientes_unicos.loc[clientes_unicos["compro_superpack"] == 1].copy()
    clientes_no_match = clientes_unicos.loc[clientes_unicos["compro_superpack"] == 0].copy()

    total_registros = int(len(work))
    total_validos = int(work["codigo_valido"].sum())
    total_unicos = int(len(clientes_unicos))
    total_match = int((clientes_unicos["compro_superpack"] == 1).sum())
    total_no_match = int((clientes_unicos["compro_superpack"] == 0).sum())
    pct_match = round((100.0 * total_match / total_unicos), 2) if total_unicos else 0.0

    resumen = pd.DataFrame(
        [
            {"metrica": "fecha_inicio", "valor": fecha_inicio},
            {"metrica": "fecha_fin_exclusiva", "valor": fecha_fin_exclusiva},
            {"metrica": "codigo_superpack", "valor": codigo_superpack},
            {"metrica": "total_registros_lista", "valor": total_registros},
            {"metrica": "total_codigos_validos_lista", "valor": total_validos},
            {"metrica": "total_clientes_unicos_lista", "valor": total_unicos},
            {"metrica": "clientes_unicos_que_compraron", "valor": total_match},
            {"metrica": "clientes_unicos_que_no_compraron", "valor": total_no_match},
            {"metrica": "pct_clientes_unicos_que_compraron", "valor": pct_match},
            {"metrica": "total_universo_superpack_mes", "valor": int(len(compradores))},
        ]
    )

    detalle_lista = detalle_lista[
        [
            "_fila_input",
            "codigo_cliente_input",
            "padded_codigo_cliente",
            "codigo_valido",
            "compro_superpack",
            "total_tx",
            "monto_total",
            "primera_fecha_operacion",
            "ultima_fecha_operacion",
        ]
    ].sort_values(["compro_superpack", "total_tx", "_fila_input"], ascending=[False, False, True])

    clientes_match = clientes_match.sort_values(
        ["total_tx", "monto_total", "padded_codigo_cliente"], ascending=[False, False, True]
    )
    clientes_no_match = clientes_no_match.sort_values(["padded_codigo_cliente"], ascending=[True])
    compradores = compradores.sort_values(
        ["total_tx", "monto_total", "padded_codigo_cliente"], ascending=[False, False, True]
    )

    return {
        "resumen": resumen,
        "detalle_lista": detalle_lista,
        "clientes_match": clientes_match,
        "clientes_no_match": clientes_no_match,
        "universo_superpack": compradores,
    }


def normalizar_columnas_para_export(sheets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    normalizadas: dict[str, pd.DataFrame] = {}
    for nombre_hoja, df in sheets.items():
        copia = df.copy()
        copia.columns = [str(col) for col in copia.columns]
        normalizadas[nombre_hoja] = copia
    return normalizadas


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Valida una lista de clientes (Excel/CSV) contra compradores de Superpack Claro."
    )
    parser.add_argument(
        "--input",
        default="",
        help=(
            "Ruta del Excel/CSV con la lista de clientes (opcional). "
            "Default: inputs/clientes Contactados promo Claro.xlsx"
        ),
    )
    parser.add_argument(
        "--sheet",
        default="",
        help="Nombre de hoja (opcional). Si no se indica, usa la primera hoja.",
    )
    parser.add_argument(
        "--cliente-column",
        default="",
        help="Nombre de columna del cliente (opcional). Si no se indica, se detecta automaticamente.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta de salida del Excel (opcional). Default: exports/validacion_superpack_claro_abril_2026.xlsx",
    )
    parser.add_argument("--fecha-inicio", default="2026-04-01", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default="2026-05-01",
        help="Fecha fin exclusiva (YYYY-MM-DD).",
    )
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo de servicio Multipago.")
    args = parser.parse_args()

    try:
        input_path = Path(args.input.strip()) if args.input.strip() else DEFAULT_INPUT
        print(f"Leyendo archivo: {input_path}")
        df_lista = cargar_lista(str(input_path), args.sheet)
        col_cliente = seleccionar_columna_cliente(df_lista, args.cliente_column)
        print(f"Columna de cliente usada: {col_cliente}")
        print(f"Filas en lista de entrada: {len(df_lista):,}")

        print("Consultando compradores de Superpack en SQL Server...")
        df_compradores = obtener_compradores_superpack(
            args.fecha_inicio, args.fecha_fin_exclusiva, args.codigo_superpack
        )
        print(f"Clientes unicos compradores en el periodo: {len(df_compradores):,}")

        print("Construyendo validacion y resumen...")
        sheets = construir_salidas(
            df_lista=df_lista,
            col_cliente=col_cliente,
            df_compradores=df_compradores,
            fecha_inicio=args.fecha_inicio,
            fecha_fin_exclusiva=args.fecha_fin_exclusiva,
            codigo_superpack=args.codigo_superpack,
        )
        sheets = normalizar_columnas_para_export(sheets)

        output_path = Path(args.output.strip()) if args.output.strip() else DEFAULT_OUTPUT
        exportar_excel_multi(sheets, str(output_path))
        print(f"Archivo generado: {output_path}")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
