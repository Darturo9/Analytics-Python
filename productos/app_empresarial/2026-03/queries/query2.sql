SELECT RIGHT('00000000' + LEFT(RTRIM(LTRIM(hist_envios.codigo_cliente)),
				LEN(RTRIM(LTRIM(hist_envios.codigo_cliente))) - 1), 8)		AS padded_codigo_cliente,
        MIN(month(cmp.start_date ))       as mes,
	MIN(YEAR(cmp.start_date))		  as año,
	MIN(cmp.start_date) as fecha
	FROM
		dwhbi.dbo.dw_rtm_app_hiscampaignuniverso hist_envios
	INNER JOIN
		dwhbi.dbo.dw_rtm_app_campaign cmp
		ON hist_envios.campaignid = cmp.campaignid
		AND cmp.start_date >= '2025-05-01'
		AND cmp.countryid = 3
		AND cmp.name LIKE '%app empresarial%'
		AND cmp.name LIKE '%72049%'
		AND cmp.description LIKE '%reg sqvd%'
GROUP BY
hist_envios.codigo_cliente