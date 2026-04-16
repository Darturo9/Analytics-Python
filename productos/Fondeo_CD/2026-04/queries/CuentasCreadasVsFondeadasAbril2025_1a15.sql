-- Cuentas de Cuenta Digital creadas del 1 al 15 de abril 2025,
-- y cuantas de esas tuvieron saldo > 0 al menos una vez
-- dentro del mismo rango (1 al 15 de abril 2025).
-- Basado en la lógica de ComparacionMensual.sql.

WITH Abril_2025_1a15 AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura >= '2025-04-01'
      AND dw_feha_apertura <  '2025-04-16'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
)
SELECT
    'Abril 2025 (1-15)' AS periodo,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) AS cuentas_creadas_1_15,
    COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN u.DW_CUENTA_CORPORATIVA END) AS cuentas_fondeadas_1_15,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA)
        - COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN u.DW_CUENTA_CORPORATIVA END) AS cuentas_sin_fondear_1_15,
    CAST(
        CASE
            WHEN COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) = 0 THEN 0
            ELSE (
                COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN u.DW_CUENTA_CORPORATIVA END) * 100.0
                / COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA)
            )
        END
        AS DECIMAL(10, 2)
    ) AS tasa_fondeo_pct
FROM Abril_2025_1a15 u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
   AND h.dw_fecha_informacion >= '2025-04-01'
   AND h.dw_fecha_informacion <  '2025-04-16';
