"""
crecimiento_superpack_valor_minimo.py
--------------------------------------
Evalua el crecimiento mensual de compras de Superpack filtrando
solo transacciones con monto >= valor minimo (default: 120).

Muestra por mes:
- Clientes unicos
- Total transacciones
- Monto total
- Crecimiento mes a mes (%) para cada metrica

Ejecucion:
    python3 "productos/Superpack Claro/crecimiento_superpack_valor_minimo.py"
    python3 "productos/Superpack Claro/crecimiento_superpack_valor_minimo.py" --valor-minimo 150
    python3 "productos/Superpack Claro/crecimiento_superpack_valor_minimo.py" --anio 2026 --mes-inicio 1 --mes-fin 4
"""

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
    1: "enero",    2: "febrero",  3: "marzo",     4: "abril",
    5: "mayo",     6: "junio",    7: "julio",      8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

SQL_CRECIMIENTO = """
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
      AND CAST(p.sppava AS DECIMAL(18, 2)) >= :valor_minimo
),
trx_validas AS (
    SELECT padded_codigo_cliente, fecha_operacion, monto_operacion
    FROM trx_superpack
    WHERE padded_codigo_cliente IS NOT NULL
)
SELECT
    YEAR(fecha_operacion)  AS anio,
    MONTH(fecha_operacion) AS mes,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos,
    COUNT(*)                              AS total_transacciones,
    CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total
FROM trx_validas
GROUP BY YEAR(fecha_operacion), MONTH(fecha_operacion)
ORDER BY anio, mes
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()
    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def siguiente_mes(anio: int, mes: int) -> tuple[int, int]:
    return (anio + 1, 1) if mes == 12 else (anio, mes + 1)


def agregar_crecimiento(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas de crecimiento % mes a mes para clientes, tx y monto."""
    for col, col_crecimiento in [
        ("clientes_unicos",     "crec_clientes_pct"),
        ("total_transacciones", "crec_tx_pct"),
        ("monto_total",         "crec_monto_pct"),
    ]:
        anterior = df[col].shift(1)
        df[col_crecimiento] = (
            ((df[col] - anterior) / anterior * 100)
            .where(anterior.notna() & (anterior != 0))
            .round(1)
        )
    return df


def preparar_salida(df_sql: pd.DataFrame, anio: int, mes_inicio: int, mes_fin: int) -> pd.DataFrame:
    base = pd.DataFrame([{"anio": anio, "mes": m} for m in range(mes_inicio, mes_fin + 1)])
    df = base.merge(df_sql, on=["anio", "mes"], how="left")

    for col in ["clientes_unicos", "total_transacciones"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["monto_total"] = pd.to_numeric(df["monto_total"], errors="coerce").fillna(0.0)
    df["periodo"] = df.apply(lambda r: f"{NOMBRE_MES[int(r['mes'])]} {int(r['anio'])}", axis=1)

    df = agregar_crecimiento(df)
    return df[["periodo", "clientes_unicos", "crec_clientes_pct",
               "total_transacciones", "crec_tx_pct",
               "monto_total", "crec_monto_pct"]]


def imprimir_tabla(df: pd.DataFrame, valor_minimo: float, anio: int, mes_inicio: int, mes_fin: int) -> None:
    def fmt_pct(v):
        if pd.isna(v):
            return "  —  "
        signo = "+" if v >= 0 else ""
        return f"{signo}{v:.1f}%"

    print("\n============================================================")
    print(f" CRECIMIENTO SUPERPACK — VALOR MINIMO: L {valor_minimo:,.2f}")
    print(f" Periodo: {NOMBRE_MES[mes_inicio]} {anio} a {NOMBRE_MES[mes_fin]} {anio}")
    print("============================================================")
    print(f"{'Mes':<16} {'Clientes':>10} {'Crec.':>8}  {'Transacc.':>10} {'Crec.':>8}  {'Monto Total':>14} {'Crec.':>8}")
    print(f"{'-'*16} {'-'*10} {'-'*8}  {'-'*10} {'-'*8}  {'-'*14} {'-'*8}")

    for _, row in df.iterrows():
        print(
            f"{row['periodo']:<16} "
            f"{int(row['clientes_unicos']):>10,} {fmt_pct(row['crec_clientes_pct']):>8}  "
            f"{int(row['total_transacciones']):>10,} {fmt_pct(row['crec_tx_pct']):>8}  "
            f"L {float(row['monto_total']):>12,.2f} {fmt_pct(row['crec_monto_pct']):>8}"
        )
    print("============================================================\n")


def exportar_json(df: pd.DataFrame, valor_minimo: float, anio: int,
                  mes_inicio: int, mes_fin: int) -> Path:
    meses = [
        {
            "periodo":             row["periodo"],
            "clientes_unicos":     int(row["clientes_unicos"]),
            "crec_clientes_pct":   None if pd.isna(row["crec_clientes_pct"]) else float(row["crec_clientes_pct"]),
            "total_transacciones": int(row["total_transacciones"]),
            "crec_tx_pct":         None if pd.isna(row["crec_tx_pct"]) else float(row["crec_tx_pct"]),
            "monto_total":         round(float(row["monto_total"]), 2),
            "crec_monto_pct":      None if pd.isna(row["crec_monto_pct"]) else float(row["crec_monto_pct"]),
        }
        for _, row in df.iterrows()
    ]

    payload = {
        "generado_en":   datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "anio":          anio,
        "mes_inicio":    NOMBRE_MES[mes_inicio],
        "mes_fin":       NOMBRE_MES[mes_fin],
        "valor_minimo":  valor_minimo,
        "meses":         meses,
    }

    nombre = f"crecimiento_superpack_{anio}_{mes_inicio:02d}_{mes_fin:02d}_min{int(valor_minimo)}.json"
    output_path = BASE_DIR / "exports" / nombre
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crecimiento mensual de compras Superpack con valor minimo."
    )
    parser.add_argument("--anio",           type=int,   default=2026,  help="Anio del analisis (default: 2026).")
    parser.add_argument("--mes-inicio",     type=int,   default=1,     help="Mes inicio (default: 1).")
    parser.add_argument("--mes-fin",        type=int,   default=4,     help="Mes fin (default: 4).")
    parser.add_argument("--valor-minimo",   type=float, default=120.0, help="Monto minimo por transaccion (default: 120).")
    parser.add_argument("--codigo-superpack", type=int, default=498,   help="Codigo multipago de Superpack.")
    args = parser.parse_args()

    if not (1 <= args.mes_inicio <= 12 and 1 <= args.mes_fin <= 12):
        print("[ERROR] Los meses deben estar entre 1 y 12.")
        sys.exit(1)
    if args.mes_inicio > args.mes_fin:
        print("[ERROR] --mes-inicio no puede ser mayor que --mes-fin.")
        sys.exit(1)

    next_anio, next_mes = siguiente_mes(args.anio, args.mes_fin)
    params = {
        "fecha_inicio":        date(args.anio, args.mes_inicio, 1).isoformat(),
        "fecha_fin_exclusiva": date(next_anio, next_mes, 1).isoformat(),
        "codigo_superpack":    args.codigo_superpack,
        "valor_minimo":        args.valor_minimo,
    }

    try:
        print(f"Consultando compras con monto >= {args.valor_minimo:,.2f} ...")
        df_sql = run_query(SQL_CRECIMIENTO, params=params)
        df_salida = preparar_salida(df_sql, args.anio, args.mes_inicio, args.mes_fin)
        imprimir_tabla(df_salida, args.valor_minimo, args.anio, args.mes_inicio, args.mes_fin)

        output = exportar_json(df_salida, args.valor_minimo, args.anio, args.mes_inicio, args.mes_fin)
        print(f"JSON exportado: {output}")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
