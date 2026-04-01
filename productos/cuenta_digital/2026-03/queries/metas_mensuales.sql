/*
==============================================================================
metas_mensuales.sql
------------------------------------------------------------------------------
Propósito:
  Mantener intacta la query original (analisis.sql) y proveer una nueva
  consulta para Tableau con:
    - Cuentas reales por mes
    - Meta mensual
    - Brecha (real - meta)
    - % cumplimiento

Notas:
  - Esta query conserva la lógica de deduplicación por cuenta corporativa
    (rn = 1) de la consulta original.
  - Las metas se mantienen en un CTE VALUES para edición rápida.
==============================================================================
*/

WITH base_cuentas AS (
    SELECT
        depositos.dw_cuenta_corporativa AS numero_cuenta,
        CAST(depositos.dw_feha_apertura AS DATE) AS fecha_apertura,
        ROW_NUMBER() OVER (
            PARTITION BY depositos.dw_cuenta_corporativa
            ORDER BY
                clientes.cldoc,
                direcciones.dw_nivel_geo1,
                direcciones.dw_nivel_geo2,
                direcciones.dw_nivel_geo3
        ) AS rn
    FROM dw_dep_depositos depositos
    LEFT JOIN dw_cif_clientes clientes
        ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes.cldoc))
    LEFT JOIN dw_cif_direcciones_principal direcciones
        ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(direcciones.cldoc))
    LEFT JOIN dw_bel_ibclie clientes_bel
        ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes_bel.clccli))
    LEFT JOIN dw_bel_ibuser usuarios_bel
        ON clientes_bel.CLCCLI = usuarios_bel.CLCCLI
        AND usuarios_bel.dw_fecha_creacion = depositos.dw_feha_apertura
    LEFT JOIN tr_cif_clagen agencia
        ON agencia.clagcd = clientes_bel.clucra
        AND agencia.empcod = 1
        AND agencia.clagno LIKE '%servicios electronicos%'
    WHERE
        depositos.dw_feha_apertura >= '2025-01-01'
        AND depositos.dw_producto = 'CUENTA DIGITAL'
),
base_deduplicada AS (
    SELECT
        numero_cuenta,
        fecha_apertura
    FROM base_cuentas
    WHERE rn = 1
),
reales AS (
    SELECT
        DATEFROMPARTS(YEAR(fecha_apertura), MONTH(fecha_apertura), 1) AS mes,
        COUNT(DISTINCT numero_cuenta) AS cuentas_reales
    FROM base_deduplicada
    GROUP BY DATEFROMPARTS(YEAR(fecha_apertura), MONTH(fecha_apertura), 1)
),
metas AS (
    SELECT
        CAST(v.mes AS DATE) AS mes,
        v.meta_cuentas
    FROM (VALUES
        ('2026-01-01', 1180),
        ('2026-02-01', 1370),
        ('2026-03-01', 1370),
        ('2026-04-01', 1490),
        ('2026-05-01', 1350),
        ('2026-06-01', 1345),
        ('2026-07-01', 1345),
        ('2026-08-01', 1350),
        ('2026-09-01', 1350),
        ('2026-10-01', 1350),
        ('2026-11-01', 1370),
        ('2026-12-01', 1370)
    ) v(mes, meta_cuentas)
)
SELECT
    COALESCE(r.mes, m.mes) AS mes,
    YEAR(COALESCE(r.mes, m.mes)) AS anio,
    MONTH(COALESCE(r.mes, m.mes)) AS mes_num,
    COALESCE(r.cuentas_reales, 0) AS cuentas_reales,
    COALESCE(m.meta_cuentas, 0) AS meta_cuentas,
    COALESCE(r.cuentas_reales, 0) - COALESCE(m.meta_cuentas, 0) AS brecha_cuentas,
    CASE
        WHEN COALESCE(m.meta_cuentas, 0) = 0 THEN NULL
        ELSE CAST((COALESCE(r.cuentas_reales, 0) * 100.0) / m.meta_cuentas AS DECIMAL(10, 2))
    END AS pct_cumplimiento
FROM reales r
FULL OUTER JOIN metas m
    ON r.mes = m.mes
ORDER BY COALESCE(r.mes, m.mes);
