SELECT
    RIGHT('00000000' + RTRIM(LTRIM(depositos.cldoc)), 8) AS padded_codigo_cliente,
    COALESCE(depositos.CTCTRX, 0) AS transacciones,
    depositos.DW_CUENTA_CORPORATIVA AS cuenta,
    depositos.DW_FECHA_ULTIMO_MOVIMIENTO AS fecha_mov,
    depositos.dw_moneda AS moneda,
    COALESCE(depositos.ctt001, 0) AS saldo_ayer,
    COALESCE(depositos.dw_saldo_promedio, 0) AS saldo_promedio,
    depositos.CTSTA AS estatus_cuenta,
    depositos.dw_fecha_informacion AS fecha_informacion,
    depositos.dw_feha_apertura AS fecha_apertura
FROM HIS_DEP_DEPOSITOS_VIEW depositos
WHERE depositos.PRCODP = 1
  AND depositos.PRSUBP = 51
  AND depositos.dw_producto = 'CUENTA DIGITAL'
  AND depositos.dw_fecha_informacion = '2026-03-31';
