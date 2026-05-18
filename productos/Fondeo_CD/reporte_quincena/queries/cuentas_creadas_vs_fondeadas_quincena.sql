WITH cuentas_quincena AS (
    SELECT DISTINCT
        d.DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= :fecha_inicio_quincena
      AND d.dw_feha_apertura <  :fecha_fin_quincena_exclusiva
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
)
SELECT
    :periodo_quincena AS periodo_quincena,
    :periodo_fondeo AS periodo_fondeo,
    COUNT(DISTINCT q.DW_CUENTA_CORPORATIVA) AS cuentas_creadas_quincena,
    COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN q.DW_CUENTA_CORPORATIVA END) AS cuentas_fondeadas_periodo,
    COUNT(DISTINCT q.DW_CUENTA_CORPORATIVA)
        - COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN q.DW_CUENTA_CORPORATIVA END) AS cuentas_sin_fondear_periodo,
    CAST(
        CASE
            WHEN COUNT(DISTINCT q.DW_CUENTA_CORPORATIVA) = 0 THEN 0
            ELSE (
                COUNT(DISTINCT CASE WHEN h.ctt001 > 0 THEN q.DW_CUENTA_CORPORATIVA END) * 100.0
                / COUNT(DISTINCT q.DW_CUENTA_CORPORATIVA)
            )
        END
        AS DECIMAL(10, 2)
    ) AS tasa_fondeo_pct
FROM cuentas_quincena q
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON q.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
   AND h.dw_fecha_informacion >= :fecha_inicio_fondeo
   AND h.dw_fecha_informacion <  :fecha_fin_fondeo_exclusiva;
