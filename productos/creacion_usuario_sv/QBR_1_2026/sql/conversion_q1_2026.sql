SELECT  
	usuarios.dw_fecha_creacion										as fecha_creacion_usuario,
	RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8)			as codigo_cliente_usuario_creado,
    usuarios.uscode													as nombre_usuario,
	info_clientes.CLISEX											as genero_cliente,
    info_clientes.DW_FECHA_NACIMIENTO								as fecha_nacimiento_usuario,
    usuarios.USSTAT													as estado_usuario,
	clientes_bel.CLSTAT												as estado_cliente,
    -- Direcciones
	direcciones.dw_nivel_geo1										AS direccion_lvl_1,
	direcciones.dw_nivel_geo2										AS direccion_lvl_2,
	direcciones.dw_nivel_geo3										AS direccion_lvl_3
FROM
	dw_bel_ibuser usuarios
	INNER JOIN dw_bel_ibclie clientes_bel ON (usuarios.CLCCLI = clientes_bel.CLCCLI)
	INNER JOIN DW_CIF_CLIENTES info_clientes ON (RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8) = RIGHT('00000000' + RTRIM(LTRIM(info_clientes.CLDOC)), 8))
	LEFT JOIN  dw_cif_direcciones_principal direcciones ON (RTRIM(LTRIM(info_clientes.cltdoc)) = RTRIM(LTRIM(direcciones.cldoc)))
WHERE
usuarios.dw_fecha_creacion >= '2026-01-01 00:00:00'
AND usuarios.dw_fecha_creacion <  '2026-04-01 00:00:00'
AND
clientes_bel.cltipe = 'N'
