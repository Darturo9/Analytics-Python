-- Cuentas con primer fondeo por mes en Q1 2026
-- Regla: cada cuenta cuenta una sola vez, en el primer mes donde fondeo en Q1.
-- Ejemplo: si fondea en enero y marzo, solo cuenta en enero.
-- Universo: cuentas CD que abrieron entre enero y marzo 2026.

WITH UniversoCuentas AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura >= '2026-01-01'
      AND dw_feha_apertura <  '2026-04-01'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
),
CreadasMensual AS (
    SELECT
        DATEFROMPARTS(YEAR(dw_feha_apertura), MONTH(dw_feha_apertura), 1) AS mes_inicio,
        COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_creadas_mes
    FROM dw_dep_depositos
    WHERE dw_feha_apertura >= '2026-01-01'
      AND dw_feha_apertura <  '2026-04-01'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
    GROUP BY DATEFROMPARTS(YEAR(dw_feha_apertura), MONTH(dw_feha_apertura), 1)
),
PrimerFondeoQ1 AS (
    SELECT
        u.DW_CUENTA_CORPORATIVA,
        MIN(h.dw_fecha_informacion) AS primer_fondeo_q1
    FROM UniversoCuentas u
    INNER JOIN HIS_DEP_DEPOSITOS_VIEW h
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
    GROUP BY u.DW_CUENTA_CORPORATIVA
),
ResumenMensual AS (
    SELECT
        DATEFROMPARTS(YEAR(primer_fondeo_q1), MONTH(primer_fondeo_q1), 1) AS mes_inicio,
        COUNT(*) AS cuentas_primer_fondeo_mes
    FROM PrimerFondeoQ1
    GROUP BY DATEFROMPARTS(YEAR(primer_fondeo_q1), MONTH(primer_fondeo_q1), 1)
),
Totales AS (
    SELECT COUNT(*) AS cuentas_abiertas_q1
    FROM UniversoCuentas
)
SELECT
    m.mes,
    m.orden,
    COALESCE(c.cuentas_creadas_mes, 0) AS cuentas_creadas_mes,
    COALESCE(r.cuentas_primer_fondeo_mes, 0) AS cuentas_primer_fondeo_mes,
    t.cuentas_abiertas_q1
FROM (
    VALUES
        (CAST('2026-01-01' AS DATE), 'Enero 2026', 1),
        (CAST('2026-02-01' AS DATE), 'Febrero 2026', 2),
        (CAST('2026-03-01' AS DATE), 'Marzo 2026', 3)
) AS m(mes_inicio, mes, orden)
LEFT JOIN CreadasMensual c
    ON c.mes_inicio = m.mes_inicio
LEFT JOIN ResumenMensual r
    ON r.mes_inicio = m.mes_inicio
CROSS JOIN Totales t
ORDER BY m.orden;
