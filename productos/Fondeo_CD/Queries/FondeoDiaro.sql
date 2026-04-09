WITH UniversoCuentas AS (
    -- Primero aislamos solo las cuentas que nos interesan
    SELECT DISTINCT 
        cldoc, 
        DW_CUENTA_CORPORATIVA, 
        dw_moneda, 
        dw_feha_apertura
    FROM dw_dep_depositos
    WHERE dw_feha_apertura BETWEEN '2026-01-01' AND '2026-03-31'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1 AND PRSUBP = 51
)
SELECT
    RIGHT('00000000' + RTRIM(LTRIM(u.cldoc)), 8) AS padded_codigo_cliente,
    u.DW_CUENTA_CORPORATIVA AS cuenta,
    u.dw_moneda AS moneda,
    u.dw_feha_apertura AS fecha_apertura,
    MAX(h.ctt001) AS saldo_maximo_mes,
    AVG(CASE WHEN h.ctt001 > 0 THEN h.ctt001 END) AS saldo_promedio_dias_fondeados,
    MIN(CASE WHEN h.ctt001 > 0 THEN h.dw_fecha_informacion END) AS primer_dia_fondeo,
    COUNT(CASE WHEN h.ctt001 > 0 THEN 1 END) AS dias_con_fondos
FROM UniversoCuentas u
INNER JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
WHERE h.dw_fecha_informacion BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY
    u.cldoc, u.DW_CUENTA_CORPORATIVA, u.dw_moneda, u.dw_feha_apertura
HAVING MAX(h.ctt001) > 0;