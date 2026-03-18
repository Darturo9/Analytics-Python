/*
==============================================================================
Autor: Monica Isabel García de León - 67382
Fecha creación / Modificación: 19/09/2025
Descripción: Generación de base de monetización de cuenta digital, los filtros
			 son: 
				* Clientes naturales
				* Clientes con estado activo
				* Usuarios activos.
				* Clientes con cuenta digital activa
				* Clientes que en su cuenta tengan un monto igual a 0.
				* Clientes que no sean empleados de banpaís
				* Clientes con notificaciones push activas
==============================================================================
*/

WITH clientes_cuenta_digital_montos as (
	SELECT RIGHT('00000000' + RTRIM(LTRIM(cldoc)),8) AS padded_codigo_cliente_depositos,
		   RTRIM(LTRIM(cldoc)) AS codigo_cliente
	 FROM dw_dep_depositos depositos
	 WHERE depositos.DW_PRODUCTO like '%DIGITAL%'
	 AND depositos.PRCODP  =  1    -- Codigo Producto	
	 AND depositos.PRSUBP  =  51   -- Codigo Sub Producto
	 AND depositos.CTSTA  =  'A'   -- Cuenta digital Activa
	 AND depositos.ctt001 = 0	-- Saldo cero
), usuarios_activos as(
	SELECT * FROM(
		SELECT RTRIM(LTRIM(usuarios.CLCCLI)) as cod_cliente,
			ROW_NUMBER() OVER(PARTITION BY RTRIM(LTRIM(usuarios.CLCCLI)) ORDER BY usuarios.dw_fecha_creacion DESC) [cont]
		FROM dw_bel_ibuser usuarios
		INNER JOIN dw_bel_ibpnus notificaciones --Notificaciones Push
		ON RTRIM(LTRIM(usuarios.uscode)) = RTRIM(LTRIM(notificaciones.usecode))
		WHERE
			 usuarios.usstat = 'A'
			 AND notificaciones.uspnst = 1	  -- Notificaciones Push activo
			 AND notificaciones.usbpst = 1	  -- Notificaciones BP Movil activo
	)as rn
	WHERE rn.cont = 1
), clientes_activos as (
	SELECT * FROM(
		SELECT RTRIM(LTRIM(cldoc)) as cod_cliente,
			ROW_NUMBER() OVER(PARTITION BY RTRIM(LTRIM(cldoc)) ORDER BY dw_fecha_informacion DESC) [cont]
		FROM DW_CIF_CLIENTES 
		WHERE
			 estatu = 'A'  -- Clientes con estatus activo
		AND cltipe = 'N' -- Clientes naturales
		AND clclco != 10 -- Empleados Banpaís
	)as rn
	WHERE rn.cont = 1
), usuario_cliente_activo as (
	SELECT clientes.*
	FROM clientes_activos clientes
	INNER JOIN usuarios_activos usuarios
	ON RTRIM(LTRIM(clientes.cod_cliente)) = RTRIM(LTRIM(usuarios.cod_cliente))
), usuarios_cliente_monto as (
	SELECT clientes.*
	FROM usuario_cliente_activo clientes
	INNER JOIN clientes_cuenta_digital_montos cuenta
	ON RTRIM(LTRIM(clientes.cod_cliente)) = RTRIM(LTRIM(cuenta.codigo_cliente))
), usuarios as (
		SELECT DISTINCT cod_cliente FROM usuarios_cliente_monto
)

SELECT
	base.codigo_cliente				AS codigo_cliente,
	base.nombre_cliente				AS nombre_cliente,
	base.numero_telefono			AS numero_telefono,
	base.nombre_operador			AS nombre_operador,
	base.correo						AS correo,
	base.anio_nac					AS anio_nac
FROM (
SELECT
	seg_clientes.cod_cliente			AS codigo_cliente,
	clientes.clnomb						AS nombre_cliente,
	YEAR(clientes.DW_FECHA_NACIMIENTO)	AS anio_nac,
	CASE
		WHEN sms.telefono_sms IS NOT NULL THEN sms.telefono_sms
		WHEN
			COALESCE(LEN(telefonos.cltel1), 0) = 8
			AND SUBSTRING(telefonos.cltel1, 1, 1) IN ('3', '8', '9')
			THEN telefonos.cltel1
		WHEN
			COALESCE(LEN(telefonos.cltel2), 0) = 8
			AND SUBSTRING(telefonos.cltel2, 1, 1) IN ('3', '8', '9')
			THEN telefonos.cltel2
	END									AS numero_telefono,
	sms.dw_nombre_operador				AS nombre_operador,
	LOWER(RTRIM(LTRIM(correos.cldire)))	AS correo,
	--
	ROW_NUMBER() OVER (PARTITION BY seg_clientes.cod_cliente
			ORDER BY sms.telefono_sms, telefonos.cltel1, telefonos.cltel2, correos.cldire) AS rn
FROM
	usuarios seg_clientes
INNER JOIN
	dw_cif_clientes clientes
	ON	RTRIM(LTRIM(seg_clientes.cod_cliente)) = RTRIM(LTRIM(clientes.cldoc))
LEFT JOIN
	dwhbi.dbo.dw_sms_perfil_usuario sms
	ON RIGHT('00000000' + RTRIM(LTRIM(seg_clientes.cod_cliente)),8) = RTRIM(LTRIM(sms.cif))
	AND sms.dw_descripcion_status = 'ACTIVO'
	AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H')
	AND LEN(sms.telefono_sms) = 8
	AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3', '8', '9')
LEFT JOIN
	dw_cif_direcciones_principal telefonos
	ON RTRIM(LTRIM(seg_clientes.cod_cliente)) = RTRIM(LTRIM(telefonos.cldoc))
LEFT JOIN
	dw_cif_direcciones correos
	ON RTRIM(LTRIM(seg_clientes.cod_cliente)) = RTRIM(LTRIM(correos.cldoc))
	AND correos.cldico = 4
	AND correos.cldire LIKE '%_@_%.__%'
) AS base
WHERE
	rn = 1
	AND NOT (base.numero_telefono IS NULL AND base.correo IS NULL)
	AND  base.correo LIKE '%@%.com' -- contiene @ y termina en .com
	AND LEN( base.correo) - LEN(REPLACE( base.correo, '@', '')) = 1 -- solo una arroba
	AND PATINDEX('%[^a-zA-Z0-9@._]%',  base.correo) = 0 -- sin caracteres especiales no permitidos
	AND  base.correo NOT LIKE '%www.%' -- no contiene www.
	AND CHARINDEX(' ',  base.correo) = 0 -- sin espacios
	AND CHARINDEX('@',  base.correo) > 1 -- @ no al inicio
	AND CHARINDEX('.',  base.correo) > CHARINDEX('@',  base.correo); -- punto después del @
