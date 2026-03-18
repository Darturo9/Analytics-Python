SELECT
	RIGHT('00000000' + RTRIM(LTRIM(txn_bxi.clccli)), 8)				AS cif_toda_transaccion,
	txn_bxi.jovalo													AS valor,
	descripcion_servicio.SEDESC										AS descrip,
	'123'															AS codigo,
	'123'															AS maestro_multi_cat,
	txn_bxi.dw_fecha_journal										AS fecha_pago,
	descripcion_servicio.secode										AS codigo_multi,
        txn_bxi.JOLOTE + 'J' as id_transaccion
FROM
	-- TRX BXI (Journal)
	dw_bel_ibjour AS txn_bxi
	INNER JOIN
	-- DescripciOn del servicio
	dw_bel_ibserv descripcion_servicio
	ON txn_bxi.secode = descripcion_servicio.secode
	-- ParAmetros / Filtros de descripciOn:
	-- interfaz, tipo servicio, usa productos
	AND descripcion_servicio.inserv = 'APP'
	AND descripcion_servicio.tiserv = 'O'
	AND descripcion_servicio.seuspr = 'S'
	-- C�digo de servicio "Recargas CLARO" (Paquetigos)
	-- ibw (, pago tigo app)
	AND descripcion_servicio.secode IN ('ap-pagclar',
	'app-pagcla',
	'ope-rccl',
	'app-reccla', -- claro, 
	'app-ptigo','pag-tigo','app-rectig','ope-rctg',--tigo,
	'app-paenee','pag-enee','app-asps',-- agua,
	'pag-asps',--agua
	'app-achtrf',
        'app-trach',
        'app-transf',
        'app-transh',
        'app-transt',
        'app-tcpago',
        'app-pagotc',
		'app-paptmo',--pago prestamo
		'pago-ptmos',
		'ope-psarah' --Impuestos Aduanas
	)
	WHERE
		txn_bxi.dw_fecha_journal >='2025-01-01'
		-- Par�metros / Filtros de Journal
		-- estatus, secuencia, valor
		AND txn_bxi.jostat = 1
		AND txn_bxi.josecu = 1
		AND txn_bxi.jovalo > 0

UNION ALL

SELECT
	RIGHT('00000000' + RTRIM(LTRIM(SUBSTRING(datos_pago.spinus, 1, PATINDEX('%[A-Za-z]%', datos_pago.spinus) - 1))), 8)															AS cif_toda_transaccion,	
	datos_pago.SPPAVA			as valor,
	maestro_multipago.SPNOMC		as descrip,
	datos_pago.spcodc					as codigo,
	maestro_multipago.SPCCAT			as maestro_multi_cat,
	datos_pago.DW_FECHA_OPERACION_SP	as fecha_pago,
	'NAN' as codigo_multi,
        datos_pago.SPNUPA + 'M'                 as id_transaccion
			

		FROM
			dw_mul_sppadat datos_pago
		INNER JOIN
			dw_mul_spmaco maestro_multipago
			ON datos_pago.spcodc = maestro_multipago.spcodc
		WHERE
			datos_pago.dw_fecha_operacion_sp >= '2025-01-01'
			AND datos_pago.spcpde = 'App'
			-- Es reversa = No
			AND datos_pago.sppafr = 'N'
			AND datos_pago.spcodc IN ('866','882', --impuestos
			'130',
			'143',
			'184',
			'227',
			'237',
			'238',
			'309',
			'368',
			'371',
			'446',
			'459',
			'478',
			'507',
			'512',
			'526',
			'571',
			'574',
			'643',
			'680',
			'687',
			'734',
			'755',
			'885',
			'888', --aguaa
			'481','907', -- energia electrica
			'693', --licencias
			'572','573','732',
			'498',--claro) --cable)
			'524',--tigo)
			'513',
			'868',
			'869',
			'408',--Muni SPS
			'784'
			)