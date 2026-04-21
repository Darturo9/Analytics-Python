import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query
from core.utils import exportar_excel


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR / "inputs" / "clientes Contactados promo Claro.xlsx"
DEFAULT_OUTPUT_COMPRADORES = BASE_DIR / "exports" / "clientes_que_compraron_superpack_abril_2026.xlsx"
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

SQL_RESUMEN_DIARIO_SUPERPACK = """
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
),
trx_validas AS (
    SELECT
        padded_codigo_cliente,
        fecha_operacion,
        monto_operacion
    FROM trx_superpack
    WHERE padded_codigo_cliente IS NOT NULL
),
clientes_diarios AS (
    SELECT
        fecha_operacion,
        COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos_compradores,
        COUNT(*) AS total_tx_dia
    FROM trx_validas
    GROUP BY fecha_operacion
),
montos_diarios AS (
    SELECT
        fecha_operacion,
        monto_operacion,
        COUNT(*) AS frecuencia_monto,
        ROW_NUMBER() OVER (
            PARTITION BY fecha_operacion
            ORDER BY COUNT(*) DESC, monto_operacion ASC
        ) AS rn
    FROM trx_validas
    GROUP BY fecha_operacion, monto_operacion
)
SELECT
    c.fecha_operacion,
    c.clientes_unicos_compradores,
    c.total_tx_dia,
    m.monto_operacion AS monto_mas_comun,
    m.frecuencia_monto AS frecuencia_monto_mas_comun
FROM clientes_diarios c
LEFT JOIN montos_diarios m
    ON c.fecha_operacion = m.fecha_operacion
   AND m.rn = 1
ORDER BY c.fecha_operacion
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


def obtener_resumen_diario_superpack(
    fecha_inicio: str, fecha_fin_exclusiva: str, codigo_superpack: int
) -> pd.DataFrame:
    params = {
        "fecha_inicio": fecha_inicio,
        "fecha_fin_exclusiva": fecha_fin_exclusiva,
        "codigo_superpack": codigo_superpack,
    }
    return run_query(SQL_RESUMEN_DIARIO_SUPERPACK, params=params)


def imprimir_resumen_diario_en_consola(df_resumen_diario: pd.DataFrame) -> None:
    print("===== RESUMEN DIARIO SUPERPACK =====")
    if df_resumen_diario.empty:
        print("Sin compras en el periodo consultado.")
        print("====================================\n")
        return

    tabla = df_resumen_diario.copy()
    tabla["fecha_operacion"] = pd.to_datetime(tabla["fecha_operacion"]).dt.strftime("%Y-%m-%d")
    tabla["clientes_unicos_compradores"] = tabla["clientes_unicos_compradores"].fillna(0).astype(int)
    tabla["total_tx_dia"] = tabla["total_tx_dia"].fillna(0).astype(int)
    tabla["frecuencia_monto_mas_comun"] = tabla["frecuencia_monto_mas_comun"].fillna(0).astype(int)
    tabla["monto_mas_comun"] = pd.to_numeric(tabla["monto_mas_comun"], errors="coerce").fillna(0.0)
    tabla["monto_mas_comun"] = tabla["monto_mas_comun"].map(lambda x: f"{x:,.2f}")

    tabla = tabla.rename(
        columns={
            "fecha_operacion": "fecha",
            "clientes_unicos_compradores": "clientes_unicos",
            "total_tx_dia": "total_tx",
            "monto_mas_comun": "monto_mas_comun",
            "frecuencia_monto_mas_comun": "frecuencia_monto",
        }
    )
    print(tabla.to_string(index=False))
    print("====================================\n")


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


def to_int_safe(value: object) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


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
    parser.add_argument("--fecha-inicio", default="2026-04-01", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default="2026-05-01",
        help="Fecha fin exclusiva (YYYY-MM-DD).",
    )
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo de servicio Multipago.")
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="No genera Excel. Muestra resultados solo en consola.",
    )
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
        df_resumen_diario = obtener_resumen_diario_superpack(
            args.fecha_inicio, args.fecha_fin_exclusiva, args.codigo_superpack
        )
        imprimir_resumen_diario_en_consola(df_resumen_diario)

        print("Construyendo validacion y resumen...")
        sheets = construir_salidas(
            df_lista=df_lista,
            col_cliente=col_cliente,
            df_compradores=df_compradores,
            fecha_inicio=args.fecha_inicio,
            fecha_fin_exclusiva=args.fecha_fin_exclusiva,
            codigo_superpack=args.codigo_superpack,
        )

        resumen_df = sheets["resumen"].copy()
        resumen_map = {
            str(row["metrica"]): row["valor"]
            for _, row in resumen_df.iterrows()
        }
        print("\n===== RESUMEN =====")
        print(f"Total registros lista: {to_int_safe(resumen_map.get('total_registros_lista', 0)):,}")
        print(f"Total codigos validos: {to_int_safe(resumen_map.get('total_codigos_validos_lista', 0)):,}")
        print(f"Total clientes unicos lista: {to_int_safe(resumen_map.get('total_clientes_unicos_lista', 0)):,}")
        print(f"Clientes unicos que compraron: {to_int_safe(resumen_map.get('clientes_unicos_que_compraron', 0)):,}")
        print(f"Clientes unicos que no compraron: {to_int_safe(resumen_map.get('clientes_unicos_que_no_compraron', 0)):,}")
        print(f"% que compraron: {resumen_map.get('pct_clientes_unicos_que_compraron', 0)}")
        print("===================\n")

        if args.no_export:
            print("Modo --no-export activo. No se genero archivo Excel.")
            return

        compradores_path = DEFAULT_OUTPUT_COMPRADORES
        compradores_df = sheets["clientes_match"].copy()
        compradores_df.columns = [str(col) for col in compradores_df.columns]
        exportar_excel(compradores_df, str(compradores_path), hoja="compradores_superpack")
        print(f"Archivo compradores generado: {compradores_path}")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
