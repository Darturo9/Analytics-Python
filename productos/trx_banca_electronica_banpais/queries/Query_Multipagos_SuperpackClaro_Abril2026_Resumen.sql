-------- Resumen Multipagos - SUPERPACK-CLARO - Abril 2026
SELECT
  Canal,
  COUNT(DISTINCT Codigo_Cliente)  AS TotalClientes,
  COUNT(*)                        AS TotalTransacciones,
  SUM(Valor)                      AS MontoTotal,
  SUM(ValorLempirizado)           AS MontoTotalLempiras,
  SUM(ValorDolarizado)            AS MontoTotalDolares

FROM (

  SELECT
    ClientesBel.CLCCLI              AS Codigo_Cliente,
    DW_MUL_SPPADAT.SPCPDE           AS Canal,
    DW_MUL_SPPADAT.SPPAVA           AS Valor,
    DW_MUL_SPPADAT.SPPAVA *
      CASE
        WHEN CLMOCO = 'US$' THEN (
          SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT
          FROM DW_CON_TIPOS_CAMBIO
          WHERE 'US$' = DW_CON_TIPOS_CAMBIO.DW_MONCOD
            AND DW_CON_TIPOS_CAMBIO.dw_fecha <= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
          ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
        )
        ELSE 1
      END                           AS ValorLempirizado,
    DW_MUL_SPPADAT.SPPAVA /
      CASE
        WHEN CLMOCO = 'US$' THEN (
          SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT
          FROM DW_CON_TIPOS_CAMBIO
          WHERE 'US$' = DW_CON_TIPOS_CAMBIO.DW_MONCOD
            AND DW_CON_TIPOS_CAMBIO.dw_fecha <= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
          ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
        )
        ELSE 1
      END                           AS ValorDolarizado
  FROM
    DW_MUL_SPMACO
      INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC = DW_MUL_SPPADAT.SPCODC)
      LEFT JOIN (
        SELECT
          LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
          LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
      ) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
  WHERE
    DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-04-01'
    AND DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP <  '2026-05-01'
    AND DW_MUL_SPPADAT.SPCPCO IN (1, 7)
    AND DW_MUL_SPMACO.SPNOMC = 'SUPERPACK-CLARO'

) Base

GROUP BY Canal
ORDER BY MontoTotalLempiras DESC
