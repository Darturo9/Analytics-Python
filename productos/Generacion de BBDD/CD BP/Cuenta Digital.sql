/*
==============================================================================
Autor: Elio David Ramírez Gomez - 72208
Fecha creación / Modificación: 25/11/2025
Descripción: Generación de base de cuenta digital Banpais, los filtros
			 son: 
				* Clientes naturales
				* Clientes con estado activo
				* Usuarios activos
				* Clientes con usuario creado en año 2025 o año actual
				* Clientes que hayan hecho login en año 2025 o año actual
				* Clientes con cuenta digital activa
				* Clientes que no hayan realizado más de una transacción con su cuenta digital
				* Clientes que no sean empleados de banpaís
				* Clientes con notificaciones push ya no
				* Clientes cuyo único producto sea cuenta catracha o ahorro común.
				* Clientes con TC
				* Clientes con BP Movil activo ya mp
				* Cliente con correo y/o celular válido
==============================================================================
*/
WITH clientes_con_un_producto_IPC as (
	SELECT RTRIM(LTRIM(clientes.cldoc))							AS codigo_cliente,
			RIGHT('00000000' + RTRIM(LTRIM(clientes.cldoc)), 8)	AS padded_codigo_cliente
	FROM dw_cif_clientes clientes
	WHERE 
	---Empleados Banpais código 10
	 clientes.clclco != 10   --- Campo Clase cliente = empleado banpais
	-- IPC
	AND
	(
		COALESCE(clientes.dw_ahorros_cnt, 0)	+
		COALESCE(clientes.dw_bonos_cnt, 0)		+
		COALESCE(clientes.dw_monetarios_cnt, 0)	+
		COALESCE(clientes.dw_plazos_cnt, 0)		+
		COALESCE(clientes.dw_prestamos_cnt, 0)	+
		COALESCE(clientes.dw_tc_cnt, 0)			+
		COALESCE(clientes.dw_tcemp_cnt, 0)
		) = 1
	AND clientes.estatu = 'A'   -- Cliente Activo
	AND clientes.cltipe = 'N'	-- Cliente Natural
), 

usuarios_activos as (
SELECT * FROM(
	SELECT clientes.codigo_cliente,
			clientes.padded_codigo_cliente,
			ROW_NUMBER() OVER(PARTITION BY RTRIM(LTRIM(usuarios.CLCCLI)) ORDER BY usuarios.dw_fecha_creacion DESC) [cont]
	FROM dw_bel_ibuser usuarios
	INNER JOIN clientes_con_un_producto_IPC clientes
	ON RTRIM(LTRIM(clientes.codigo_cliente)) = RTRIM(LTRIM(usuarios.CLCCLI))
	INNER JOIN dw_bel_ibpnus notificaciones --Notificaciones Push
	ON RTRIM(LTRIM(usuarios.uscode)) = RTRIM(LTRIM(notificaciones.usecode))
	AND usuarios.usstat = 'A'
	--AND usuarios.dw_fecha_creacion >= '2025-01-01 00:00:00' --DANILO
	AND usuarios.dw_fecha_creacion >= '2026-01-01 00:00:00'
--ya no	AND notificaciones.uspnst = 1 --  Notificaciones push activo
--ya no	AND notificaciones.usbpst = 1  -- BP movil activo
	)as rn
	WHERE rn.cont = 1
),usuarios_login as(
	SELECT DISTINCT usuarios.codigo_cliente,
			usuarios.padded_codigo_cliente
	FROM usuarios_activos usuarios
	INNER join dw_bel_IBSTTRA_VIEW inicio
	ON  RTRIM(LTRIM(inicio.CLCCLI)) = RTRIM(LTRIM(usuarios.codigo_cliente))
	AND inicio.SECODE IS NOT NULL
	--AND YEAR(inicio.dw_fecha_trx) >= '2025' -- DANILO
	AND YEAR(inicio.dw_fecha_trx) >= '2026'

	-- Clientes con cuenta catracha

), clientes_con_cuenta_catracha as (
	SELECT  clientes.* 
	FROM usuarios_login clientes
	INNER JOIN DW_DEP_DEPOSITOS depositos
	ON RTRIM(LTRIM(clientes.codigo_cliente)) = RTRIM(LTRIM(depositos.cldoc))
	--WHERE depositos.DW_PRODUCTO LIKE '%CATRACHA%'
	--WHERE depositos.DW_PRODUCTO =  'CUENTA CATRACHA  -- Cambio Danilo                      '
	WHERE DEPOSITOS.PRTIPO = 21 AND  DEPOSITOS.PRSUBP = 11
   

	--Clientes con cuenta de ahorro

), clientes_con_cuenta_ahorro as (
	SELECT clientes.* 
	FROM usuarios_login clientes
	INNER JOIN DW_DEP_DEPOSITOS depositos
	ON RTRIM(LTRIM(clientes.codigo_cliente)) = RTRIM(LTRIM(depositos.cldoc))
	 --WHERE depositos.DW_APLICACION = 'AHORRO' -- CAMBIO DANILO
    WHERE  DEPOSITOS.TIPPRO = 2

	-- Clientes con tarjeta de crédito

), clientes_con_TC as (
	SELECT clientes_datos.*
	FROM usuarios_login clientes_datos
	INNER JOIN DW_CIF_CLIENTES clientes
	ON RTRIM(LTRIM(clientes.cldoc)) = RTRIM(LTRIM(clientes_datos.codigo_cliente))
	AND (COALESCE(clientes.dw_tc_cnt, 0)			+
		COALESCE(clientes.dw_tcemp_cnt, 0)
	) = 1
	AND 
	( 	COALESCE(clientes.dw_ahorros_cnt, 0)	+
		COALESCE(clientes.dw_bonos_cnt, 0) +
		COALESCE(clientes.dw_monetarios_cnt, 0) +
		COALESCE(clientes.dw_plazos_cnt, 0) +
		COALESCE(clientes.dw_prestamos_cnt, 0)) = 0
), set_bases_antes as(
	SELECT catracha.codigo_cliente, catracha.padded_codigo_cliente FROM clientes_con_cuenta_catracha catracha
	UNION ALL
	SELECT ahorro.codigo_cliente, ahorro.padded_codigo_cliente FROM clientes_con_cuenta_ahorro ahorro
	UNION ALL
	SELECT tc.codigo_cliente, tc.padded_codigo_cliente FROM clientes_con_TC tc

	-- Clientes sin cuenta digital

),set_bases as (
	SELECT DISTINCT base.codigo_cliente,
		base.padded_codigo_cliente
	FROM set_bases_antes base
	WHERE RTRIM(LTRIM(base.codigo_cliente)) NOT IN
	(
		SELECT RTRIM(LTRIM(cldoc))
		FROM DW_DEP_DEPOSITOS 
		WHERE
	--	WHERE DW_PRODUCTO like '%DIGITAL%'
	--	AND  -- CAMBIO DANILO
		PRCODP  =  1
		AND PRSUBP  =  51
		AND CTSTA  =  'A'
	)
)

-- Generación de BDD

SELECT 
	base.codigo_cliente				AS codigo_cliente,
	base.nombre_cliente				AS nombre_cliente,
	base.numero_telefono			AS numero_telefono,
	base.nombre_operador			AS nombre_operador,
	base.correo						AS correo,
	base.anio_nacimiento		    AS anio_nacimiento,
    base.generacion				    AS generacion


FROM (
SELECT
	seg_clientes.codigo_cliente			AS codigo_cliente,
	clientes.clnomb						AS nombre_cliente,
	YEAR(clientes.DW_FECHA_NACIMIENTO)  AS anio_nacimiento,
    CASE
        WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1965 AND 1980 THEN 'Generation X (1965-1980)'
        WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1981 AND 1996 THEN 'Gen Y - Millennials (1981-1996)'
        WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1997 AND 2012 THEN 'Generación Z (1997-2012)'
        WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) < 1965                THEN 'Baby Boomers o anterior'
        WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) > 2012                THEN 'Gen Alpha (2013+)'
        ELSE 'Sin datos'
    END                  AS generacion,
	CASE
		WHEN sms.telefono_sms IS NOT NULL THEN sms.telefono_sms
		WHEN
			COALESCE(LEN(telefonos.cltel1), 0) = 8
			AND SUBSTRING(telefonos.cltel1, 1, 1) IN ('3', '8', '9')
			THEN telefonos.cltel1
		WHEN
			COALESCE(LEN(telefonos.cltel2), 0) = 8
			AND SUBSTRING(telefonos.cltel2, 1, 1  ) IN ('3', '8', '9')
			THEN telefonos.cltel2
	END									AS numero_telefono,
	sms.dw_nombre_operador				AS nombre_operador,
	LOWER(RTRIM(LTRIM(correos.cldire)))	AS correo,
	--
	ROW_NUMBER() OVER (PARTITION BY seg_clientes.codigo_cliente
			ORDER BY sms.telefono_sms, telefonos.cltel1, telefonos.cltel2, correos.cldire) AS rn
FROM
	set_bases seg_clientes
INNER JOIN
	dw_cif_clientes clientes
	ON	RTRIM(LTRIM(seg_clientes.codigo_cliente)) = RTRIM(LTRIM(clientes.cldoc))
LEFT JOIN
	dwhbi.dbo.dw_sms_perfil_usuario sms
	ON RTRIM(LTRIM(seg_clientes.padded_codigo_cliente)) = RTRIM(LTRIM(sms.cif))
	AND sms.dw_descripcion_status = 'ACTIVO'
	--AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H') --DANILO
	AND sms.operador in ('E','F','D')
	AND LEN(sms.telefono_sms) = 8
	AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3', '8', '9')
LEFT JOIN
	dw_cif_direcciones_principal telefonos
	ON RTRIM(LTRIM(seg_clientes.codigo_cliente)) = RTRIM(LTRIM(telefonos.cldoc))
LEFT JOIN
	dw_cif_direcciones correos
	ON RTRIM(LTRIM(seg_clientes.codigo_cliente)) = RTRIM(LTRIM(correos.cldoc))
	AND correos.cldico = 4
	AND correos.cldire LIKE '%_@_%.__%'
) AS base
WHERE
	rn = 1
	--Validación de correo electrónico
	AND NOT (base.numero_telefono IS NULL AND base.correo IS NULL)
	AND  base.correo LIKE '%@%.com' -- contiene @ y termina en .com
	AND LEN( base.correo) - LEN(REPLACE( base.correo, '@', '')) = 1 -- solo una arroba
	AND PATINDEX('%[^a-zA-Z0-9@._-]%',  base.correo) = 0 -- sin caracteres especiales no permitidos
	AND  base.correo NOT LIKE '%www.%' -- no contiene www.
	AND CHARINDEX(' ',  base.correo) = 0 -- sin espacios
	AND CHARINDEX('@',  base.correo) > 1 -- @ no al inicio
