-- Cuentas de Cuenta Digital creadas en abril 2026
-- y cuantas tuvieron saldo > 0 al menos una vez durante abril 2026.
-- Basado en la lógica de ComparacionMensual.sql.

WITH Abril AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura >= '2026-04-01'
      AND dw_feha_apertura <  '2026-05-01'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
)
SELECT
    'Abril 2026' AS mes,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) AS cuentas_creadas_abril,
    COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN u.DW_CUENTA_CORPORATIVA END) AS cuentas_fondeadas_al_menos_una_vez_abril,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA)
        - COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN u.DW_CUENTA_CORPORATIVA END) AS cuentas_sin_fondear_en_abril,
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
FROM Abril u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
   AND h.dw_fecha_informacion >= '2026-04-01'
   AND h.dw_fecha_informacion <  '2026-05-01';
