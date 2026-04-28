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
        WHEN (CIE.CLTIPE = 'N' AND DW_BEL_IBUSER.PEUSUA IN ('BPES','PYME'))
          OR CIE.CLTIPE = 'J' THEN 'Banca Empresas'
        ELSE 'Banca Personas'
      END,
    'Banca Personas')               AS TipoBanca,
    ISNULL(CIF.CLTIPE, 'N/A')       AS Tipo_Cliente

  FROM
    DW_MUL_SPMACO
      INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC = DW_MUL_SPPADAT.SPCODC)
      LEFT JOIN (
        SELECT
          LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
          LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
      ) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
      LEFT JOIN (
        SELECT LTRIM(RTRIM(CLCCLI)) CLCCLI, CLTIPE,
          ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CLCCLI)) ORDER BY CLCCLI) AS RN
        FROM DW_BEL_IBCLIE
      ) CIE ON CIE.CLCCLI = ClientesBel.CLCCLI AND CIE.RN = 1
      LEFT JOIN DW_BEL_IBUSER ON DW_BEL_IBUSER.CLCCLI = ClientesBel.CLCCLI
        AND DW_BEL_IBUSER.USCODE = ClientesBel.USCODE
      LEFT JOIN (
        SELECT LTRIM(RTRIM(CLDOC)) CLDOC, CLTIPE,
          ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(CLDOC))
            ORDER BY CASE WHEN CLTIPE = 'N' THEN 1 WHEN CLTIPE IS NULL THEN 2 ELSE 3 END
          ) AS RN
        FROM DW_CIF_CLIENTES
      ) CIF ON CIF.CLDOC = ClientesBel.CLCCLI AND CIF.RN = 1
  WHERE
    DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-04-01'
    AND DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP <  '2026-05-01'
    AND DW_MUL_SPPADAT.SPCPDE IN ('Web', 'App', 'Banca Remota', 'AP', 'App nueva', 'IB', 'LG', 'Web y App')
    AND DW_MUL_SPMACO.SPNOMC = 'SUPERPACK-CLARO'
    AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)

) Base

GROUP BY Canal, TipoBanca, Tipo_Cliente
ORDER BY MontoTotalLempiras DESC
