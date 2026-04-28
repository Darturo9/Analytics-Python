-------- Multipagos - SUPERPACK-CLARO - Abril 2026
SELECT
  Transacciones.Fecha,
  Transacciones.Usuario,
  Transacciones.Modulo,
  Transacciones.Codigo_Cliente,
  Transacciones.Moneda,
  Transacciones.Valor,
  Transacciones.ValorLempirizado,
  Transacciones.ValorDolarizado,
  Transacciones.Canal,
  Transacciones.Operación,
  Transacciones.Cuenta,
  Transacciones.[Es Reversa],
  Transacciones.[Cantidad],
  Transacciones.Hora,
  TipoBanca=ISNULL(datosBel.TipoBanca,'Banca Personas'),
  PERFIL_CONVENIO=ISNULL(datosBel.PERFIL_CONVENIO,'Otros'),
  PERFIL_USUARIO=ISNULL(datosBel.PERFIL_USUARIO,'Otros'),
  TIPO_TOKEN=ISNULL(datosBel.TIPO_TOKEN,'Otros'),
  TIPO_USUARIO=ISNULL(datosBel.TIPO_USUARIO,'Otros'),
  Responsable=ISNULL(Cuentas.Responsable,'Otros'),
  Segmento=ISNULL(Cuentas.Segmento,'Otros'),
  Perfil_UsuarioDesc,
  [Tipo de cliente].[Tipo_Cliente] AS [Tipo_Cliente],
  NombreCliente,
  Zona,
  Depto,
  BancaE,
  DW_SECTOR_DESCRIPCION,
  UsuarioActivo,
  UsuarioInactivo,
  CLSTAT,
  CantidadUsuario,
  Clientes_consorcio

FROM (

  SELECT
    DW_FECHA_OPERACION_SP AS Fecha,
    ClientesBel.USCODE Usuario,
    'Multipagos' AS Modulo,
    ClientesBel.CLCCLI Codigo_Cliente,
    CLMOCO Moneda,
    DW_MUL_SPPADAT.SPPAVA AS Valor,
    DW_MUL_SPPADAT.SPPAVA *
      CASE
      WHEN CLMOCO='US$' THEN
      (
      SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT
      FROM DW_CON_TIPOS_CAMBIO
      WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND
      DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
      ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
      )
    ELSE 1 END ValorLempirizado,
    DW_MUL_SPPADAT.SPPAVA /
      CASE
      WHEN CLMOCO='US$' THEN
      (
      SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT
      FROM
      DW_CON_TIPOS_CAMBIO
      WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND
      DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
      ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
      )
    ELSE 1 END AS ValorDolarizado,
    DW_MUL_SPPADAT.SPCPDE AS Canal,
    DW_MUL_SPMACO.SPNOMC AS Operación,
    NULL Cuenta,
    DW_MUL_SPPADAT.SPPAHR Hora,
    DW_MUL_SPPADAT.SPPAFR AS [Es Reversa],
    0 AS [Cantidad]
  FROM
  DW_MUL_SPMACO
    INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC=DW_MUL_SPPADAT.SPCODC)
    LEFT JOIN (
      SELECT
        LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
        LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
      FROM
        DW_BEL_IBUSER
      ) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI+ClientesBel.USCODE)
  WHERE
    (
    DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-04-01'
    AND DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP < '2026-05-01'
    AND DW_MUL_SPPADAT.SPCPCO IN (1, 7)
    AND DW_MUL_SPMACO.SPNOMC = 'SUPERPACK-CLARO'
    )

) Transacciones

LEFT JOIN
  (
  SELECT
      CLCCLI,
      USCODE,
      TipoBanca,
      TIPO_TOKEN,
      NombreCliente,
      PERFIL_CONVENIO,
      PERFIL_USUARIO,
      TIPO_USUARIO,
      Perfil_UsuarioDesc,
      UsuarioActivo,
      UsuarioInactivo,
      CantidadUsuario,
      CLSTAT,
      Clientes_consorcio

    FROM
      (
      SELECT
        LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI)) CLCCLI,
        LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE,
        TipoBanca=
        CASE WHEN  ( DW_BEL_IBCLIE.CLTIPE  =  'N' AND DW_BEL_IBUSER.PEUSUA IN ('BPES','PYME') ) OR DW_BEL_IBCLIE.CLTIPE  =  'J' THEN 'Banca Empresas'
        ELSE 'Banca Personas' END,
        CASE
          WHEN SOFT_TOKEN.USCODE IS NOT NULL THEN 'SOFT TOKEN'
          WHEN DW_BEL_IBPNUS.USPNST = 1 AND DW_BEL_IBPNUS.USBPST = 1 THEN 'PUSH NOTIFICATIONS'
          WHEN RTRIM(LTRIM(DW_BEL_IBUSER.USTRCA)) <> '' THEN 'TOKEN SMS'
          WHEN DW_BEL_IBUSER.USSTOK <> 0 THEN 'TOKEN FISICO'
        ELSE 'NO TIENE TOKEN'
        END TIPO_TOKEN,
        DW_BEL_IBCLIE.CLNOCL NombreCliente,
        DW_BEL_IBCLIE.PECODE AS PERFIL_CONVENIO,
        DW_BEL_IBUSER.PEUSUA AS PERFIL_USUARIO,
        DW_BEL_IBUSER.USTIUS AS TIPO_USUARIO,
        TR_BEL_IBPERF.PEDESC AS Perfil_UsuarioDesc,
        CLSTAT,
        Clientes_consorcio,
        UsuarioActivo=(SELECT SUM(CASE WHEN USSTAT='A' THEN 1 ELSE 0 END) FROM DW_BEL_IBUSER WHERE DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI),
        UsuarioInactivo=(SELECT SUM(CASE WHEN USSTAT='I' THEN 1 ELSE 0 END) FROM DW_BEL_IBUSER WHERE DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI),
        CantidadUsuario=(SELECT COUNT(USCODE) FROM DW_BEL_IBUSER WHERE DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI),
        RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI)) ORDER BY DW_BEL_IBCLIE.CLCCLI,CLSTAT,USSTAT ASC,DW_BEL_IBUSER.dw_fecha_creacion asc)
      FROM
        DW_BEL_IBCLIE
        LEFT JOIN (
        SELECT
          e1.CLCCLI ,
          STUFF((
            SELECT ',' + e2.CLCCLC + ' - ' + e2.CLNOCL
            FROM dw_bel_IBCLIC AS e2
            WHERE e2.CLCCLI = e1.CLCCLI
            FOR XML PATH(''), TYPE
          ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS Clientes_consorcio,
          RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(e1.CLCCLI)) ORDER BY e1.CLCCLI asc)
        FROM dw_bel_IBCLIC AS e1
        GROUP BY CLCCLI
        ) Consorcio ON Consorcio.CLCCLI = DW_BEL_IBCLIE.CLCCLI AND Consorcio.RN=1
          INNER JOIN
          (
          SELECT DW_BEL_IBUSER.CLCCLI,
               DW_BEL_IBUSER.PEUSUA,
               DW_BEL_IBUSER.USTIUS,
               DW_BEL_IBUSER.USSTOK,
               DW_BEL_IBUSER.USTRCA,
               DW_BEL_IBUSER.USCODE,
               DW_BEL_IBUSER.USSTAT,
               DW_BEL_IBUSER.dw_fecha_creacion
          FROM DW_BEL_IBUSER
          )
          DW_BEL_IBUSER ON (DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI)
           LEFT JOIN TR_BEL_IBPERF ON (DW_BEL_IBUSER.PEUSUA=TR_BEL_IBPERF.PECODE)
          LEFT JOIN
            (
              SELECT
                DW_BEL_IBAUTUSR.USCODE,
                DW_BEL_IBAUTUSR.IBASST,
                DW_BEL_IBAUTUSR.IBESST
              FROM
                DW_BEL_IBAUTUSR
              WHERE
                ( DW_BEL_IBAUTUSR.IBASST  =  'S'   AND   DW_BEL_IBAUTUSR.IBESST  =  'A' )
            ) SOFT_TOKEN ON (DW_BEL_IBUSER.USCODE = SOFT_TOKEN.USCODE)
          LEFT JOIN DW_BEL_IBPNUS WITH (NOLOCK) ON (DW_BEL_IBUSER.USCODE = DW_BEL_IBPNUS.USECODE)
      ) Datos
    WHERE
      RN=1
  ) datosBel ON Transacciones.Codigo_Cliente = datosBel.CLCCLI

LEFT JOIN
(
  SELECT
    TR_CIF_CLTIEJ.CLTJDE Segmento,
    tr_cif_clejne.CLEJDE Responsable,
    DW_DEP_DEPOSITOS.CLRESP,
    LTRIM(RTRIM(DW_DEP_DEPOSITOS.CLDOC)) CLDOC,
    DW_DEP_DEPOSITOS.DW_CUENTA_CORPORATIVA,
    DW_DEP_DEPOSITOS.DW_FEHA_APERTURA,
    RN=ROW_Number()OVER(PARTITION BY DW_DEP_DEPOSITOS.CLDOC ORDER BY DW_DEP_DEPOSITOS.DW_FEHA_APERTURA DESC)
  FROM
    DWHBP..TR_CIF_CLTIEJ
      INNER JOIN DWHBP..tr_cif_clejne ON (TR_CIF_CLTIEJ.CLTJCO=tr_cif_clejne.CLTJCO)
      RIGHT OUTER JOIN DWHBP..DW_DEP_DEPOSITOS ON (DW_DEP_DEPOSITOS.CLRESP=tr_cif_clejne.CLEJCO and tr_cif_clejne.EMPCOD=1)
  WHERE
    TR_CIF_CLTIEJ.CLTJDE IN
      (
      'GTE CTA INTER COMERCIAL',
      'GERENTE DE CUENTA CORPORATIVO',
      'GTE DE CUENTA CASH MANAGEMENT',
      'GERENTE DE CUENTA PYME'
      )
) Cuentas ON Cuentas.CLDOC = Transacciones.Codigo_Cliente AND Cuentas.RN=1

LEFT JOIN (
    SELECT DW_CIF_CLIENTES.DW_SECTOR_DESCRIPCION,
         LTRIM(RTRIM(DW_CIF_CLIENTES.CLDOC)) AS CIF,
         DW_CIF_CLIENTES.CLTIPE AS Tipo_Cliente,
     DW_CIF_DIRECCIONES.DW_NIVEL_GEO1 Zona,
     DW_CIF_DIRECCIONES.DW_NIVEL_GEO2 Depto,
     DW_CIF_CLIENTES.dw_usuarios_bel_cnt BancaE,
     DW_CIF_DIRECCIONES.CLDICO,
     RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(DW_CIF_DIRECCIONES.CLDOC)) ORDER BY DW_CIF_DIRECCIONES.dw_fecha DESC)
  FROM
        DW_CIF_CLIENTES
    LEFT JOIN DW_CIF_DIRECCIONES ON DW_CIF_CLIENTES.CLDOC=DW_CIF_DIRECCIONES.CLDOC
  WHERE DW_CIF_DIRECCIONES.CLDICO=1
) [Tipo de cliente] ON ([Transacciones].[Codigo_Cliente] = [Tipo de cliente].[CIF] AND [Tipo de cliente].RN=1)
