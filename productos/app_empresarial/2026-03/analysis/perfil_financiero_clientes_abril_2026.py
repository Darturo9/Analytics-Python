"""
perfil_financiero_clientes_abril_2026.py
----------------------------------------
Analiza el perfil financiero de los clientes de:
Clientes_Abril2026_ConModulos.*

Incluye:
- Total clientes del universo
- Clientes con saldo positivo en abril 2026
- Saldo promedio de abril por cliente
- Saldo al corte del 30-abr-2026
- Top segmentos y responsables

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/perfil_financiero_clientes_abril_2026.py
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query


EXPORTS_DIR = PROJECT_ROOT / "productos" / "app_empresarial" / "2026-03" / "exports"
INPUT_TXT = EXPORTS_DIR / "Clientes_Abril2026_ConModulos.txt"
INPUT_XLSX = EXPORTS_DIR / "Clientes_Abril2026_ConModulos.xlsx"

OUT_XLSX = EXPORTS_DIR / "PerfilFinancieroClientes_Abril2026.xlsx"
OUT_TXT = EXPORTS_DIR / "ResumenPerfilFinancieroClientes_Abril2026.txt"

FECHA_INICIO = "2026-04-01"
FECHA_FIN = "2026-05-01"
FECHA_CORTE = "2026-04-30"
CHUNK_SIZE = 400


def _fmt_int(value: float | int) -> str:
    return f"{int(round(float(value or 0))):,}"


def _fmt_dec(value: float | int) -> str:
    return f"{float(value or 0):,.2f}"


def normalizar_codigo_cliente(valor: object) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    if texto == "":
        return ""
    solo_digitos = re.sub(r"\D", "", texto)
    if solo_digitos == "":
        return ""
    return solo_digitos[-8:].zfill(8)


def cargar_universo_clientes() -> list[str]:
    if INPUT_TXT.exists():
        lineas = INPUT_TXT.read_text(encoding="utf-8").splitlines()
        clientes = [normalizar_codigo_cliente(x) for x in lineas]
    elif INPUT_XLSX.exists():
        df = pd.read_excel(INPUT_XLSX)
        if df.empty:
            return []
        primera_columna = df.columns[0]
        clientes = [normalizar_codigo_cliente(x) for x in df[primera_columna].tolist()]
    else:
        raise FileNotFoundError(
            "No se encontro el archivo de universo de abril. "
            f"Esperado: {INPUT_TXT} o {INPUT_XLSX}"
        )

    clientes = sorted({c for c in clientes if c})
    return clientes


def construir_query_chunk(clientes_chunk: list[str]) -> str:
    values_sql = ",\n            ".join(f"('{c}')" for c in clientes_chunk)

    return f"""
WITH clientes AS (
    SELECT v.padded_codigo_cliente
    FROM (VALUES
            {values_sql}
    ) v(padded_codigo_cliente)
),
cuentas_cliente AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
        d.DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos d
    INNER JOIN clientes c
        ON c.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
    WHERE d.DW_PRODUCTO = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
saldos_dia AS (
    SELECT
        cc.padded_codigo_cliente,
        CAST(h.dw_fecha_informacion AS DATE) AS fecha_saldo,
        SUM(CAST(COALESCE(h.ctt001, 0) AS DECIMAL(18, 2))) AS saldo_total_dia
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN cuentas_cliente cc
        ON cc.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '{FECHA_INICIO}'
      AND h.dw_fecha_informacion <  '{FECHA_FIN}'
    GROUP BY
        cc.padded_codigo_cliente,
        CAST(h.dw_fecha_informacion AS DATE)
),
saldo_cliente AS (
    SELECT
        c.padded_codigo_cliente,
        AVG(COALESCE(sd.saldo_total_dia, 0)) AS saldo_promedio_abril,
        SUM(
            CASE WHEN sd.fecha_saldo = '{FECHA_CORTE}'
                 THEN COALESCE(sd.saldo_total_dia, 0)
                 ELSE 0
            END
        ) AS saldo_corte_30_abril,
        MAX(CASE WHEN COALESCE(sd.saldo_total_dia, 0) > 0 THEN 1 ELSE 0 END) AS con_saldo_positivo_abril
    FROM clientes c
    LEFT JOIN saldos_dia sd
        ON sd.padded_codigo_cliente = c.padded_codigo_cliente
    GROUP BY
        c.padded_codigo_cliente
),
segmento_cliente AS (
    SELECT
        x.padded_codigo_cliente,
        x.segmento,
        x.responsable
    FROM (
        SELECT
            RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
            t.CLTJDE AS segmento,
            e.CLEJDE AS responsable,
            ROW_NUMBER() OVER (
                PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
                ORDER BY d.DW_FEHA_APERTURA DESC
            ) AS rn
        FROM DWHBP..DW_DEP_DEPOSITOS d
        LEFT JOIN DWHBP..tr_cif_clejne e
            ON d.CLRESP = e.CLEJCO
           AND e.EMPCOD = 1
        LEFT JOIN DWHBP..TR_CIF_CLTIEJ t
            ON t.CLTJCO = e.CLTJCO
        INNER JOIN clientes c
            ON c.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
        WHERE t.CLTJDE IN (
            'GTE CTA INTER COMERCIAL',
            'GERENTE DE CUENTA CORPORATIVO',
            'GTE DE CUENTA CASH MANAGEMENT',
            'GERENTE DE CUENTA PYME'
        )
    ) x
    WHERE x.rn = 1
)
SELECT
    c.padded_codigo_cliente,
    CAST(COALESCE(sc.saldo_promedio_abril, 0) AS DECIMAL(18, 2)) AS saldo_promedio_abril,
    CAST(COALESCE(sc.saldo_corte_30_abril, 0) AS DECIMAL(18, 2)) AS saldo_corte_30_abril,
    CAST(COALESCE(sc.con_saldo_positivo_abril, 0) AS INT) AS con_saldo_positivo_abril,
    COALESCE(seg.segmento, 'SIN SEGMENTO') AS segmento,
    COALESCE(seg.responsable, 'SIN RESPONSABLE') AS responsable
FROM clientes c
LEFT JOIN saldo_cliente sc
    ON sc.padded_codigo_cliente = c.padded_codigo_cliente
LEFT JOIN segmento_cliente seg
    ON seg.padded_codigo_cliente = c.padded_codigo_cliente;
"""


def ejecutar_perfil_clientes(clientes: list[str]) -> pd.DataFrame:
    if not clientes:
        return pd.DataFrame(
            columns=[
                "padded_codigo_cliente",
                "saldo_promedio_abril",
                "saldo_corte_30_abril",
                "con_saldo_positivo_abril",
                "segmento",
                "responsable",
            ]
        )

    frames: list[pd.DataFrame] = []
    total_chunks = (len(clientes) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for idx in range(total_chunks):
        inicio = idx * CHUNK_SIZE
        fin = min((idx + 1) * CHUNK_SIZE, len(clientes))
        chunk = clientes[inicio:fin]
        print(f"Procesando bloque {idx + 1}/{total_chunks} ({len(chunk):,} clientes)...")
        sql = construir_query_chunk(chunk)
        df_chunk = run_query(sql)
        df_chunk.columns = [str(c).strip().lower() for c in df_chunk.columns]
        frames.append(df_chunk)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return df

    df["padded_codigo_cliente"] = df["padded_codigo_cliente"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["padded_codigo_cliente"]).reset_index(drop=True)
    return df


def construir_top(df: pd.DataFrame, columna: str, top_n: int = 10) -> pd.DataFrame:
    top = (
        df.groupby(columna, as_index=False)
        .agg(
            clientes_unicos=("padded_codigo_cliente", "nunique"),
            saldo_promedio_abril_prom=("saldo_promedio_abril", "mean"),
            saldo_corte_30_abril_total=("saldo_corte_30_abril", "sum"),
        )
        .sort_values(["clientes_unicos", "saldo_corte_30_abril_total"], ascending=[False, False])
        .head(top_n)
        .reset_index(drop=True)
    )
    return top


def imprimir_resumen(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    total_clientes = len(df)
    clientes_con_saldo = int((df["con_saldo_positivo_abril"] == 1).sum())
    pct_con_saldo = (clientes_con_saldo / total_clientes * 100.0) if total_clientes > 0 else 0.0

    saldo_promedio_universo = float(df["saldo_promedio_abril"].mean()) if total_clientes > 0 else 0.0
    saldo_total_corte = float(df["saldo_corte_30_abril"].sum()) if total_clientes > 0 else 0.0
    saldo_promedio_corte = float(df["saldo_corte_30_abril"].mean()) if total_clientes > 0 else 0.0

    top_segmento = construir_top(df, "segmento", top_n=10)
    top_responsable = construir_top(df, "responsable", top_n=10)

    print("\n============================================================")
    print(" PERFIL FINANCIERO - CLIENTES ABRIL 2026")
    print("============================================================")
    print(f"Universo de clientes:                  {_fmt_int(total_clientes)}")
    print(f"Clientes con saldo positivo en abril:  {_fmt_int(clientes_con_saldo)}")
    print(f"% clientes con saldo positivo:         {_fmt_dec(pct_con_saldo)}%")
    print("------------------------------------------------------------")
    print(f"Saldo promedio abril (por cliente):    L {_fmt_dec(saldo_promedio_universo)}")
    print(f"Saldo total al 30-abr-2026:            L {_fmt_dec(saldo_total_corte)}")
    print(f"Saldo promedio al 30-abr-2026:         L {_fmt_dec(saldo_promedio_corte)}")
    print("============================================================\n")

    print("Top segmentos (clientes):")
    print(top_segmento.to_string(index=False))
    print("\nTop responsables (clientes):")
    print(top_responsable.to_string(index=False))
    print()

    return top_segmento, top_responsable


def exportar_resultados(df: pd.DataFrame, top_segmento: pd.DataFrame, top_responsable: pd.DataFrame) -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(OUT_XLSX) as writer:
        df.sort_values("padded_codigo_cliente").to_excel(writer, sheet_name="DetalleCliente", index=False)
        top_segmento.to_excel(writer, sheet_name="TopSegmento", index=False)
        top_responsable.to_excel(writer, sheet_name="TopResponsable", index=False)

    total_clientes = len(df)
    clientes_con_saldo = int((df["con_saldo_positivo_abril"] == 1).sum()) if total_clientes > 0 else 0
    pct_con_saldo = (clientes_con_saldo / total_clientes * 100.0) if total_clientes > 0 else 0.0
    saldo_promedio_universo = float(df["saldo_promedio_abril"].mean()) if total_clientes > 0 else 0.0
    saldo_total_corte = float(df["saldo_corte_30_abril"].sum()) if total_clientes > 0 else 0.0
    saldo_promedio_corte = float(df["saldo_corte_30_abril"].mean()) if total_clientes > 0 else 0.0

    lineas = [
        "PERFIL FINANCIERO - CLIENTES ABRIL 2026",
        f"Universo de clientes: {_fmt_int(total_clientes)}",
        f"Clientes con saldo positivo en abril: {_fmt_int(clientes_con_saldo)}",
        f"% clientes con saldo positivo: {_fmt_dec(pct_con_saldo)}%",
        f"Saldo promedio abril (por cliente): L {_fmt_dec(saldo_promedio_universo)}",
        f"Saldo total al 30-abr-2026: L {_fmt_dec(saldo_total_corte)}",
        f"Saldo promedio al 30-abr-2026: L {_fmt_dec(saldo_promedio_corte)}",
        "",
        "Top 10 segmentos (clientes)",
    ]
    for _, row in top_segmento.iterrows():
        lineas.append(
            f"- {row['segmento']}: clientes={_fmt_int(row['clientes_unicos'])}, "
            f"saldo_prom_abril=L {_fmt_dec(row['saldo_promedio_abril_prom'])}, "
            f"saldo_corte_total=L {_fmt_dec(row['saldo_corte_30_abril_total'])}"
        )

    lineas.extend(["", "Top 10 responsables (clientes)"])
    for _, row in top_responsable.iterrows():
        lineas.append(
            f"- {row['responsable']}: clientes={_fmt_int(row['clientes_unicos'])}, "
            f"saldo_prom_abril=L {_fmt_dec(row['saldo_promedio_abril_prom'])}, "
            f"saldo_corte_total=L {_fmt_dec(row['saldo_corte_30_abril_total'])}"
        )

    OUT_TXT.write_text("\n".join(lineas), encoding="utf-8")


def main() -> int:
    try:
        t0 = time.perf_counter()
        clientes = cargar_universo_clientes()
        print(f"Clientes cargados en universo abril: {len(clientes):,}")
        print(f"Fuente universo: {INPUT_TXT if INPUT_TXT.exists() else INPUT_XLSX}")
        if not clientes:
            print("[INFO] El universo de clientes esta vacio.")
            return 0

        df = ejecutar_perfil_clientes(clientes)
        if df.empty:
            print("[INFO] No se encontraron datos para los clientes consultados.")
            return 0

        for col in ["saldo_promedio_abril", "saldo_corte_30_abril", "con_saldo_positivo_abril"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df["con_saldo_positivo_abril"] = df["con_saldo_positivo_abril"].astype(int)

        top_segmento, top_responsable = imprimir_resumen(df)
        exportar_resultados(df, top_segmento, top_responsable)

        t1 = time.perf_counter()
        print(f"Archivo Excel generado: {OUT_XLSX}")
        print(f"Archivo resumen txt:    {OUT_TXT}")
        print(f"Tiempo total:           {t1 - t0:.2f}s")
        return 0
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        return 1
    except SQLAlchemyError as exc:
        print(f"[ERROR] Fallo de base de datos: {exc}")
        return 1
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

