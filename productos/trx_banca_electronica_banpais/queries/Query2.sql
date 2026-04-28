SELECT 
  base.codigo_usuario           AS codigo_usuario,
  base.Fecha_Primer_Login       AS fecha_inicio,      
  base.Fecha_Creación_Usuario   AS fecha_creación_usuario

  FROM 
  (SELECT
   RIGHT('00000000' + RTRIM(LTRIM(dw_bel_IBSTTRA_VIEW.clccli)),8) as codigo_usuario,
  DW_BEL_IBSTTRA_VIEW.dw_fecha_trx              AS Fecha_Primer_Login,
  DW_BEL_IBUSER.dw_fecha_creacion            AS Fecha_Creación_Usuario,

  ROW_NUMBER() OVER(
  PARTITION BY dw_bel_IBSTTRA_VIEW.clccli
  ORDER BY DW_BEL_IBSTTRA_VIEW.dw_fecha_trx
  ASC) AS rn


  FROM

   DW_BEL_IBCLIE CLI INNER JOIN DW_BEL_IBUSER ON (right('00000000'+RTRIM(LTRIM(CLI.CLCCLI)),8)=right('00000000'+RTRIM(LTRIM(DW_BEL_IBUSER.CLCCLI)),8))
  INNER JOIN dw_bel_IBSTTRA_VIEW  ON (CLI.CLCCLI=dw_bel_IBSTTRA_VIEW.CLCCLI)


  INNER JOIN (
  SELECT
  cmp.start_date                            AS fecha_comunicacion,
  CASE
    WHEN cmp_comm.channelid = 6 THEN 'SMS'
    WHEN cmp_comm.channelid = 7 THEN 'Email'
    ELSE NULL
  END                                  AS canal_digital,
  RIGHT('00000000' + LEFT(RTRIM(LTRIM(hist_envios.codigo_cliente)),
      LEN(RTRIM(LTRIM(hist_envios.codigo_cliente))) - 1), 8)    AS padded_codigo_cliente
FROM
  dwhbi.dbo.dw_rtm_app_hiscampaignuniverso hist_envios
INNER JOIN
  dwhbi.dbo.dw_rtm_app_campaign cmp
  ON hist_envios.campaignid = cmp.campaignid
  AND cmp.start_date = '2026-01-16'
  AND cmp.countryid = 3
  AND cmp.name LIKE '%SIN LOG%'
  AND cmp.description LIKE '%reg sqvd%'
INNER JOIN
  dwhbi.dbo.dw_rtm_app_campaigncommunication cmp_comm
  ON hist_envios.campaignid = cmp_comm.campaignid
WHERE
  YEAR(hist_envios.dw_creationdate) >= 2026

  )

  RTM
  ON (RIGHT('00000000'+RTRIM(LTRIM(RTM.padded_codigo_cliente)),8) = RIGHT('00000000' + RTRIM(LTRIM(dw_bel_IBSTTRA_VIEW.CLCCLI)),8)
)

WHERE 
dw_fecha_trx BETWEEN '2026-01-01' AND CONVERT(SMALLDATETIME, {fn CURDATE()})-1
AND SECODE  in ('app-login','web-login','login')
) base

WHERE
rn = 1