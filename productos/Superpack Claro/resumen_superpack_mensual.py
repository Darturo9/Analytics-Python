import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query

BASE_DIR = Path(__file__).resolve().parent


NOMBRE_MES = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}

SQL_RESUMEN_MENSUAL_SUPERPACK = """
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
resumen AS (
    SELECT
        YEAR(fecha_operacion) AS anio,
        MONTH(fecha_operacion) AS mes,
        COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos,
        COUNT(*) AS total_transacciones,
        CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total_transacciones,
        CAST(AVG(monto_operacion) AS DECIMAL(18, 2)) AS monto_promedio
    FROM trx_validas
    GROUP BY YEAR(fecha_operacion), MONTH(fecha_operacion)
),
montos AS (
    SELECT
        YEAR(fecha_operacion) AS anio,
        MONTH(fecha_operacion) AS mes,
        monto_operacion,
        COUNT(*) AS frecuencia_monto,
        ROW_NUMBER() OVER (
            PARTITION BY YEAR(fecha_operacion), MONTH(fecha_operacion)
            ORDER BY COUNT(*) DESC, monto_operacion ASC
        ) AS rn
    FROM trx_validas
    GROUP BY YEAR(fecha_operacion), MONTH(fecha_operacion), monto_operacion
)
SELECT
    r.anio,
    r.mes,
    r.clientes_unicos,
    r.total_transacciones,
    r.monto_total_transacciones,
    r.monto_promedio,
    m.monto_operacion AS monto_mas_comun
FROM resumen r
LEFT JOIN montos m
    ON r.anio = m.anio
   AND r.mes = m.mes
   AND m.rn = 1
ORDER BY r.anio, r.mes
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def validar_rango_meses(mes_inicio: int, mes_fin: int) -> None:
    if not (1 <= mes_inicio <= 12 and 1 <= mes_fin <= 12):
        raise ValueError("Los meses deben estar entre 1 y 12.")
    if mes_inicio > mes_fin:
        raise ValueError("mes_inicio no puede ser mayor que mes_fin.")


def siguiente_mes(anio: int, mes: int) -> tuple[int, int]:
    if mes == 12:
        return anio + 1, 1
    return anio, mes + 1


def armar_parametros(anio: int, mes_inicio: int, mes_fin: int, codigo_superpack: int) -> dict[str, object]:
    fecha_inicio = date(anio, mes_inicio, 1)
    next_year, next_month = siguiente_mes(anio, mes_fin)
    fecha_fin_exclusiva = date(next_year, next_month, 1)
    return {
        "fecha_inicio": fecha_inicio.isoformat(),
        "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        "codigo_superpack": codigo_superpack,
    }


def construir_base_meses(anio: int, mes_inicio: int, mes_fin: int) -> pd.DataFrame:
    rows = [{"anio": anio, "mes": mes} for mes in range(mes_inicio, mes_fin + 1)]
    return pd.DataFrame(rows)


def preparar_salida(df_sql: pd.DataFrame, anio: int, mes_inicio: int, mes_fin: int) -> pd.DataFrame:
    base = construir_base_meses(anio, mes_inicio, mes_fin)
    df = base.merge(df_sql, on=["anio", "mes"], how="left")

    int_cols = ["clientes_unicos", "total_transacciones"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    money_cols = ["monto_total_transacciones", "monto_promedio", "monto_mas_comun"]
    for col in money_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["periodo"] = df.apply(lambda row: f"{NOMBRE_MES[int(row['mes'])]} {int(row['anio'])}", axis=1)

    df = df[
        [
            "periodo",
            "clientes_unicos",
            "total_transacciones",
            "monto_total_transacciones",
            "monto_promedio",
            "monto_mas_comun",
        ]
    ]
    return df


def imprimir_tabla(df: pd.DataFrame, anio: int, mes_inicio: int, mes_fin: int, codigo_superpack: int) -> None:
    tabla = df.copy()
    tabla["clientes_unicos"] = tabla["clientes_unicos"].map(lambda x: f"{int(x):,}")
    tabla["total_transacciones"] = tabla["total_transacciones"].map(lambda x: f"{int(x):,}")
    tabla["monto_total_transacciones"] = tabla["monto_total_transacciones"].map(lambda x: f"{x:,.2f}")
    tabla["monto_promedio"] = tabla["monto_promedio"].map(lambda x: f"{x:,.2f}")
    tabla["monto_mas_comun"] = tabla["monto_mas_comun"].map(lambda x: f"{x:,.2f}")
    tabla = tabla.rename(
        columns={
            "total_transacciones": "total_tx",
            "monto_total_transacciones": "monto_total_tx",
        }
    )

    print("\n===== RESUMEN MENSUAL SUPERPACK =====")
    print(
        f"Periodo: {NOMBRE_MES[mes_inicio]} {anio} a {NOMBRE_MES[mes_fin]} {anio} | "
        f"codigo_superpack={codigo_superpack}"
    )
    print(tabla.to_string(index=False))
    print("=====================================")


def exportar_json(df: pd.DataFrame, anio: int, mes_inicio: int, mes_fin: int, codigo_superpack: int) -> Path:
    meses = [
        {
            "periodo": row["periodo"],
            "clientes_unicos": int(row["clientes_unicos"]),
            "total_transacciones": int(row["total_transacciones"]),
            "monto_total": round(float(row["monto_total_transacciones"]), 2),
            "monto_promedio": round(float(row["monto_promedio"]), 2),
            "monto_mas_comun": round(float(row["monto_mas_comun"]), 2),
        }
        for _, row in df.iterrows()
    ]

    payload = {
        "generado_en": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "anio": anio,
        "mes_inicio": NOMBRE_MES[mes_inicio],
        "mes_fin": NOMBRE_MES[mes_fin],
        "codigo_superpack": codigo_superpack,
        "meses": meses,
    }

    nombre = f"resumen_superpack_{anio}_{mes_inicio:02d}_{mes_fin:02d}.json"
    output_path = BASE_DIR / "exports" / nombre
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resumen mensual de compras de Superpack (clientes unicos, tx y montos)."
    )
    parser.add_argument("--anio", type=int, default=2026, help="Anio del analisis (default: 2026).")
    parser.add_argument("--mes-inicio", type=int, default=1, help="Mes inicio (default: 1).")
    parser.add_argument("--mes-fin", type=int, default=4, help="Mes fin (default: 4).")
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo multipago de Superpack.")
    args = parser.parse_args()

    try:
        validar_rango_meses(args.mes_inicio, args.mes_fin)
        params = armar_parametros(
            anio=args.anio,
            mes_inicio=args.mes_inicio,
            mes_fin=args.mes_fin,
            codigo_superpack=args.codigo_superpack,
        )
        df_sql = run_query(SQL_RESUMEN_MENSUAL_SUPERPACK, params=params)
        df_salida = preparar_salida(df_sql, args.anio, args.mes_inicio, args.mes_fin)
        imprimir_tabla(df_salida, args.anio, args.mes_inicio, args.mes_fin, args.codigo_superpack)
        output = exportar_json(df_salida, args.anio, args.mes_inicio, args.mes_fin, args.codigo_superpack)
        print(f"JSON exportado: {output}")
    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
