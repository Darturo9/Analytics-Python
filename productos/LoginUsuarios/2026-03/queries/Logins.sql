SELECT clccli as codigo_usuario,
        RIGHT('00000000' + RTRIM(LTRIM(clccli)),8) as padded_codigo_usuario,
	   uscode as nombre_usuario,
           secode as canal_login,
	   dw_fecha_trx as fecha_inicio
FROM dw_bel_IBSTTRA_VIEW
WHERE dw_fecha_trx between '2026-01-01' and '2026-02-10'
AND SECODE  in ('app-login','web-login','login')