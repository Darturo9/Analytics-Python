SELECT * FROM (
SELECT
	ROW_NUMBER() OVER (
		PARTITION BY historial_envio.codigo_cliente, YEAR(historial_envio.dw_creationdate), MONTH(historial_envio.dw_creationdate)
		ORDER BY historial_envio.dw_creationdate
	) AS ORD,
	RIGHT('00000000' + LEFT(RTRIM(LTRIM(historial_envio.codigo_cliente)), LEN(RTRIM(LTRIM(historial_envio.codigo_cliente))) - 1), 8) AS codigo_cliente_usuario_campania,
	historial_envio.dw_creationdate AS fecha_campania
FROM
	dwhbi.dbo.dw_rtm_app_hiscampaignuniverso historial_envio
	INNER JOIN dwhbi.dbo.dw_rtm_app_campaign info_campania ON (historial_envio.campaignid = info_campania.campaignid)
WHERE
	info_campania.start_date >= '2024-09-01 00:00:00'
	AND info_campania.countryid = 2
	AND (
		info_campania.name LIKE '%creacion de usuario%'
		OR info_campania.name LIKE '%creación de usuario%'
		OR info_campania.name LIKE '% cu %'
	)
	AND info_campania.description LIKE '%reg sqvd%'
	AND historial_envio.dw_creationdate >= '2024-09-01 00:00:00'
) tbl_tem
WHERE ORD = 1;
