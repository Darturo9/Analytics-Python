/*
==============================================================================
Autor: José Antonio Ramírez Recinos - 72927
Fecha creación / Modificación:
Descripción:
==============================================================================
*/

USE DWHSV;

-- Parámetros para segmentación de clientes
SELECT
	RTRIM(LTRIM(clientes.cldoc))							AS codigo_cliente,
	RIGHT('00000000' + RTRIM(LTRIM(clientes.cldoc)), 8)		AS padded_codigo_cliente
INTO #seg_clientes
FROM
	his_cif_clientes clientes
LEFT JOIN
	dw_bel_ibclie clientes_bel
	ON RTRIM(LTRIM(clientes.cldoc)) = RTRIM(LTRIM(clientes_bel.clccli))
LEFT JOIN
	dw_bel_ibuser usuarios_bel
	ON RTRIM(LTRIM(clientes.cldoc)) = RTRIM(LTRIM(usuarios_bel.clccli))
WHERE
	clientes.dw_fecha_informacion = EOMONTH(DATEADD(MONTH, -1, GETDATE() - 1))
	AND clientes.estatu = 'A'
	AND clientes.cltipe = 'N'
	AND clientes_bel.clccli IS NULL
	AND usuarios_bel.clccli IS NULL


-- Información de contacto
SELECT
	contacto.codigo_cliente,
	contacto.nombre_cliente,
	contacto.correo
FROM (
	SELECT
		seg_clientes.codigo_cliente								AS codigo_cliente,
		clientes.clnomb											AS nombre_cliente,
		LOWER(RTRIM(LTRIM(correos.cldire)))						AS correo,
		ROW_NUMBER() OVER(PARTITION BY seg_clientes.codigo_cliente
					ORDER BY clientes.clnomb, correos.cldire)	AS rn
	FROM
		#seg_clientes seg_clientes
	INNER JOIN
		dw_cif_clientes clientes
		ON seg_clientes.codigo_cliente = RTRIM(LTRIM(clientes.cldoc))
	INNER JOIN
		dw_cif_direcciones correos
		ON seg_clientes.codigo_cliente = RTRIM(LTRIM(correos.cldoc))
		AND correos.cldico = 4
		AND correos.cldire LIKE '%_@_%.__%'
) AS contacto
WHERE
	contacto.rn = 1
	AND  contacto.correo LIKE '%@%.com' -- contiene @ y termina en .com
	AND LEN( contacto.correo) - LEN(REPLACE( contacto.correo, '@', '')) = 1 -- solo una arroba
	AND PATINDEX('%[^a-zA-Z0-9@._-]%',  contacto.correo) = 0 -- sin caracteres especiales no permitidos
	AND  contacto.correo NOT LIKE '%www.%' -- no contiene www.
	AND CHARINDEX(' ',  contacto.correo) = 0 -- sin espacios
	AND CHARINDEX('@',  contacto.correo) > 1 -- @ no al inicio
	AND LEN( contacto.correo) - LEN(REPLACE( contacto.correo, '@', '')) = 1

-- DROP TABLE #seg_clientes;