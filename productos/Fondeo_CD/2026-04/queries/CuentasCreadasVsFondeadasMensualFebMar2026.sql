-- Cuentas de Cuenta Digital creadas del 1 al 15 de febrero y marzo 2026,
-- y cuantas de esas tuvieron saldo > 0 al menos una vez dentro
-- del mes completo de su creación.
-- Basado en la lógica de ComparacionMensual.sql.

WITH Periodos AS (
    SELECT
        1 AS orden,
        'Febrero 2026 (altas 1-15)' AS mes,
        CAST('2026-02-01' AS date) AS fecha_apertura_inicio,
        CAST('2026-02-16' AS date) AS fecha_apertura_fin,
        CAST('2026-02-01' AS date) AS fecha_fondeo_inicio,
        CAST('2026-03-01' AS date) AS fecha_fondeo_fin
    UNION ALL
    SELECT
        2 AS orden,
        'Marzo 2026 (altas 1-15)' AS mes,
        CAST('2026-03-01' AS date) AS fecha_apertura_inicio,
        CAST('2026-03-16' AS date) AS fecha_apertura_fin,
        CAST('2026-03-01' AS date) AS fecha_fondeo_inicio,
        CAST('2026-04-01' AS date) AS fecha_fondeo_fin
)
SELECT
    p.mes,
    COUNT(DISTINCT d.DW_CUENTA_CORPORATIVA) AS cuentas_creadas_1_15,
    COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN d.DW_CUENTA_CORPORATIVA END) AS cuentas_fondeadas_al_menos_una_vez_en_mes_creacion,
    COUNT(DISTINCT d.DW_CUENTA_CORPORATIVA)
        - COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN d.DW_CUENTA_CORPORATIVA END) AS cuentas_sin_fondear_en_mes_creacion,
    CAST(
        CASE
            WHEN COUNT(DISTINCT d.DW_CUENTA_CORPORATIVA) = 0 THEN 0
            ELSE (
                COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN d.DW_CUENTA_CORPORATIVA END) * 100.0
                / COUNT(DISTINCT d.DW_CUENTA_CORPORATIVA)
            )
        END
        AS DECIMAL(10, 2)
    ) AS tasa_fondeo_pct
FROM Periodos p
LEFT JOIN dw_dep_depositos d
    ON d.dw_feha_apertura >= p.fecha_apertura_inicio
   AND d.dw_feha_apertura <  p.fecha_apertura_fin
   AND d.dw_producto = 'CUENTA DIGITAL'
   AND d.PRCODP = 1
   AND d.PRSUBP = 51
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON h.DW_CUENTA_CORPORATIVA = d.DW_CUENTA_CORPORATIVA
   AND h.dw_fecha_informacion >= p.fecha_fondeo_inicio
   AND h.dw_fecha_informacion <  p.fecha_fondeo_fin
GROUP BY
    p.orden,
    p.mes
ORDER BY
    p.orden;
