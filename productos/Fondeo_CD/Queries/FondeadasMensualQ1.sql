-- Cuentas fondeadas unicas por mes (Q1 2026)
-- Universo: cuentas que abrieron entre enero y marzo 2026.
-- Regla: una cuenta puede contar en uno o varios meses si tuvo saldo > 0 en ese mes,
-- sin importar en que mes del Q1 abrio.
--
-- Optimizacion:
-- 1) Filtra HIS_DEP_DEPOSITOS_VIEW a Q1 y ctt001 > 0 en una sola pasada.
-- 2) Agrega por mes y cuenta antes del conteo final.

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
FondeosQ1 AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA,
        DATEFROMPARTS(
            YEAR(h.dw_fecha_informacion),
            MONTH(h.dw_fecha_informacion),
            1
        ) AS mes_inicio
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN UniversoCuentas u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
    GROUP BY
        h.DW_CUENTA_CORPORATIVA,
        DATEFROMPARTS(YEAR(h.dw_fecha_informacion), MONTH(h.dw_fecha_informacion), 1)
),
Resumen AS (
    SELECT
        mes_inicio,
        COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas
    FROM FondeosQ1
    GROUP BY mes_inicio
)
SELECT
    m.mes,
    m.orden,
    COALESCE(r.cuentas_fondeadas, 0) AS cuentas_fondeadas
FROM (
    VALUES
        (CAST('2026-01-01' AS DATE), 'Enero 2026', 1),
        (CAST('2026-02-01' AS DATE), 'Febrero 2026', 2),
        (CAST('2026-03-01' AS DATE), 'Marzo 2026', 3)
) AS m(mes_inicio, mes, orden)
LEFT JOIN Resumen r
    ON r.mes_inicio = m.mes_inicio
ORDER BY m.orden;
