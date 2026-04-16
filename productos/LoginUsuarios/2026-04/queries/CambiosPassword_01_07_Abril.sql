SELECT
    DW_BEL_IBUSER.CLCCLI AS codigo_cliente,
    DW_BEL_IBUSER.dw_fecha_cambio_pass AS fecha_cambio_pass
FROM DW_BEL_IBUSER
WHERE DW_BEL_IBUSER.dw_fecha_cambio_pass IS NOT NULL
  AND DW_BEL_IBUSER.dw_fecha_cambio_pass >= '2026-04-01'
  AND DW_BEL_IBUSER.dw_fecha_cambio_pass < '2026-04-08';
