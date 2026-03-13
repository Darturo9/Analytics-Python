SELECT
	c_digitales.*
FROM (
	SELECT
		depositos.dw_cuenta_corporativa					AS numero_cuenta,
		clientes_bel.CLEJEV							Ejecutivo_de_Cuentas,
		usuarios_bel.USCODE							Usuario_Banca,
	    usuarios_bel.dw_fecha_creacion				Fecha_Creacion_Usuario,
            clientes.dw_fecha_alta as fecha_ingreso_banco,
		DATEDIFF(DAY, CAST(clientes.dw_fecha_alta AS DATE), depositos.dw_feha_apertura) as dif,
		clientes.DW_CLASE_CLIENTE_DESCRIPCION,
        clientes.DW_ESTADO_CIVIL_DESCRIPCION as estado_civil,
		agencia.clagno,
		CAST(depositos.dw_feha_apertura AS DATE)		AS fecha_apertura,
        COALESCE(clientes.dw_sms_cnt, 0)                      AS bp_movil, 
		CASE
			WHEN DAY(depositos.dw_feha_apertura) <= 7	THEN 1
			WHEN DAY(depositos.dw_feha_apertura) <= 14	THEN 2
			WHEN DAY(depositos.dw_feha_apertura) <= 21	THEN 3
			ELSE 4
		END												AS semana_apertura,
		CASE
			WHEN MONTH(depositos.dw_feha_apertura) <= 3 	THEN 'Q1'
			WHEN MONTH(depositos.dw_feha_apertura) <= 6		THEN 'Q2'
			WHEN MONTH(depositos.dw_feha_apertura) <= 9		THEN 'Q3'
			WHEN MONTH(depositos.dw_feha_apertura) <= 12	THEN 'Q4'
		END												AS trimestre,
		depositos.dw_estado_cuenta						AS estado_cuenta,
		depositos.dw_aplicacion							AS aplicacion,
		depositos.dw_producto							AS producto,
		depositos.dpagen								AS codigo_agencia,
		depositos.dw_agencia_cta						AS nombre_agencia,

		-- M�tricas de cuenta
		depositos.dw_moneda								AS moneda,
		COALESCE(depositos.ctt001, 0)					AS saldo_cuenta,
		COALESCE(depositos.ctctrx, 0)					AS cant_transacciones,

		-- Clientes
		RTRIM(LTRIM(depositos.cldoc))					AS codigo_cliente,
                RIGHT('00000000' + RTRIM(LTRIM(depositos.cldoc)),8)					AS padded_codigo_cliente,
		CAST(clientes.dw_fecha_alta AS DATE)			AS fecha_ingreso,
		clientes.cltipe									AS tipo_cliente,
		clientes.dw_clase_cliente_descripcion			AS descripcion_cliente,
		clientes.clclco									AS codigo_descripcion_cliente,
        clientes.clisex                                         AS genero,
        clientes.DW_FECHA_NACIMIENTO                            AS fecha_nac,
		CASE
			WHEN clientes.clclco = 10 THEN 1
			ELSE 0
		END												AS empleado_banpais,
		-- Direcciones
		direcciones.dw_nivel_geo1						AS direccion_lvl_1,
		direcciones.dw_nivel_geo2						AS direccion_lvl_2,
		direcciones.dw_nivel_geo3						AS direccion_lvl_3,
		direcciones.dw_nivel_geo1						                AS direccion_1,
		direcciones.dw_nivel_geo2						AS direccion_2,
		direcciones.dw_nivel_geo3						AS direccion_3,
		depositos.dw_ultima_transaccion as ultima_transaccion,

		ROW_NUMBER() OVER(PARTITION BY depositos.dw_cuenta_corporativa
				ORDER BY clientes.cldoc, 
				direcciones.dw_nivel_geo1, direcciones.dw_nivel_geo2, direcciones.dw_nivel_geo3)						AS rn
	FROM
		dw_dep_depositos depositos -- depositos monetarios
	LEFT JOIN
		dw_cif_clientes clientes -- todos los clientes
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes.cldoc))
	LEFT JOIN
		dw_cif_direcciones_principal direcciones -- información de donde viven 
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(direcciones.cldoc))
	LEFT JOIN
		dw_bel_ibclie clientes_bel -- Clientes BEL
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes_bel.clccli)) 
	LEFT JOIN
		dw_bel_ibuser usuarios_bel -- Usuarios BEL 
		ON (clientes_bel.CLCCLI=usuarios_bel.CLCCLI
		AND usuarios_bel.dw_fecha_creacion = depositos.dw_feha_apertura
		)
	LEFT JOIN
		tr_cif_clagen agencia -- AGENCIA
		ON agencia.clagcd = clientes_bel.clucra
		AND agencia.empcod = 1
		AND agencia.clagno LIKE '%servicios electronicos%'
	WHERE
	depositos.dw_feha_apertura >= '2026-03-01'
		AND depositos.dw_producto = 'CUENTA DIGITAL'
		--AND usuarios_bel.dw_fecha_creacion = depositos.dw_feha_apertura
) AS c_digitales
WHERE
	c_digitales.rn = 1