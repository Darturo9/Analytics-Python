-------- Resumen Multipagos - SUPERPACK-CLARO - Abril 2026
SELECT
  Canal,
  TipoBanca,
  Tipo_Cliente,
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
      END                           AS ValorDolarizado,
    ISNULL(
      CASE
        WHEN (DW_BEL_IBCLIE.CLTIPE = 'N' AND DW_BEL_IBUSER.PEUSUA IN ('BPES','PYME'))
          OR DW_BEL_IBCLIE.CLTIPE = 'J' THEN 'Banca Empresas'
        ELSE 'Banca Personas'
      END,
    'Banca Personas')               AS TipoBanca,
    ISNULL(DW_CIF_CLIENTES.CLTIPE, 'N/A') AS Tipo_Cliente

  FROM
    DW_MUL_SPMACO
      INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC = DW_MUL_SPPADAT.SPCODC)
      LEFT JOIN (
        SELECT
          LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
          LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
      ) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
      LEFT JOIN DW_BEL_IBCLIE ON DW_BEL_IBCLIE.CLCCLI = ClientesBel.CLCCLI
      LEFT JOIN DW_BEL_IBUSER ON DW_BEL_IBUSER.CLCCLI = ClientesBel.CLCCLI
        AND DW_BEL_IBUSER.USCODE = ClientesBel.USCODE
      LEFT JOIN DW_CIF_CLIENTES ON LTRIM(RTRIM(DW_CIF_CLIENTES.CLDOC)) = ClientesBel.CLCCLI
  WHERE
    DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-04-01'
    AND DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP <  '2026-05-01'
    AND DW_MUL_SPPADAT.SPCPCO IN (1, 7)
    AND DW_MUL_SPMACO.SPNOMC = 'SUPERPACK-CLARO'
    AND DW_MUL_SPPADAT.SPPAFR = 'N'
    AND (DW_CIF_CLIENTES.CLTIPE <> 'J' OR DW_CIF_CLIENTES.CLTIPE IS NULL)
    AND LTRIM(RTRIM(ClientesBel.CLCCLI)) NOT IN ('2169011','2285579','2496312','2285625','2058276')

) Base

GROUP BY Canal, TipoBanca, Tipo_Cliente
ORDER BY MontoTotalLempiras DESC
