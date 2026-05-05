-- Comparativo mensual 2026 de Cuenta Digital:
-- - cuentas creadas en febrero, marzo y abril
-- - cuantas de esas cuentas tuvieron al menos 1 dia con fondos
--   dentro del mismo mes de creacion.

WITH periodos AS (
    SELECT
        1 AS orden,
        'Febrero 2026' AS mes,
        CAST('2026-02-01' AS date) AS fecha_inicio,
        CAST('2026-03-01' AS date) AS fecha_fin
    UNION ALL
    SELECT
        2 AS orden,
        'Marzo 2026' AS mes,
        CAST('2026-03-01' AS date) AS fecha_inicio,
        CAST('2026-04-01' AS date) AS fecha_fin
    UNION ALL
    SELECT
        3 AS orden,
        'Abril 2026' AS mes,
        CAST('2026-04-01' AS date) AS fecha_inicio,
        CAST('2026-05-01' AS date) AS fecha_fin
),
cuentas_mes AS (
    SELECT DISTINCT
        p.orden,
        p.mes,
        p.fecha_inicio,
        p.fecha_fin,
        d.DW_CUENTA_CORPORATIVA
    FROM periodos p
    INNER JOIN dw_dep_depositos d
        ON d.dw_feha_apertura >= p.fecha_inicio
       AND d.dw_feha_apertura <  p.fecha_fin
       AND d.dw_producto = 'CUENTA DIGITAL'
       AND d.PRCODP = 1
       AND d.PRSUBP = 51
)
SELECT
    p.mes,
    COUNT(DISTINCT c.DW_CUENTA_CORPORATIVA) AS cuentas_creadas_mes,
    COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN c.DW_CUENTA_CORPORATIVA END) AS cuentas_fondeadas_al_menos_1_dia_mes,
    COUNT(DISTINCT c.DW_CUENTA_CORPORATIVA)
        - COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN c.DW_CUENTA_CORPORATIVA END) AS cuentas_sin_fondear_mes,
    CAST(
        CASE
            WHEN COUNT(DISTINCT c.DW_CUENTA_CORPORATIVA) = 0 THEN 0
            ELSE (
                COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN c.DW_CUENTA_CORPORATIVA END) * 100.0
                / COUNT(DISTINCT c.DW_CUENTA_CORPORATIVA)
            )
        END
        AS DECIMAL(10, 2)
    ) AS tasa_fondeo_pct
FROM periodos p
LEFT JOIN cuentas_mes c
    ON p.orden = c.orden
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON h.DW_CUENTA_CORPORATIVA = c.DW_CUENTA_CORPORATIVA
   AND h.dw_fecha_informacion >= p.fecha_inicio
   AND h.dw_fecha_informacion <  p.fecha_fin
GROUP BY
    p.orden,
    p.mes
ORDER BY
    p.orden;
