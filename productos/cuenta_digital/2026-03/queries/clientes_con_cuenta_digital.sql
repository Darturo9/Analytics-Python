SELECT DISTINCT
    RIGHT('00000000' + LTRIM(RTRIM(CAST(depositos.cldoc AS VARCHAR(50)))), 8) AS padded_codigo_cliente
FROM dw_dep_depositos depositos
WHERE depositos.dw_producto = 'CUENTA DIGITAL'
  AND depositos.dw_feha_apertura >= '2025-01-01'
  AND depositos.cldoc IS NOT NULL;
