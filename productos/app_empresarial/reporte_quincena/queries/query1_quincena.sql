-------- Login web, App y nuevas consultas
;WITH conteo_usuarios AS (
	SELECT
		LTRIM(RTRIM(CLCCLI)) AS CLCCLI,
		SUM(CASE WHEN USSTAT = 'A' THEN 1 ELSE 0 END) AS UsuarioActivo,
		SUM(CASE WHEN USSTAT = 'I' THEN 1 ELSE 0 END) AS UsuarioInactivo,
		COUNT(USCODE) AS CantidadUsuario
	FROM DW_BEL_IBUSER
	GROUP BY LTRIM(RTRIM(CLCCLI))
)
SELECT 
	Transacciones.Fecha,
	Transacciones.Usuario,
	Transacciones.Modulo,
	RIGHT(REPLICATE('0', 8) + RTRIM(LTRIM(	Transacciones.Codigo_Cliente)), 8) 	AS padded_codigo_cliente,
	Transacciones.Moneda,
	Transacciones.Valor,
	Transacciones.ValorLempirizado,
	Transacciones.ValorDolarizado,
	Transacciones.Canal,
	Transacciones.Operación,
	Transacciones.Cuenta,
	Transacciones.[Es Reversa],
	Transacciones.[Cantidad],
	Transacciones.Hora,
	TipoBanca=ISNULL(datosBel.TipoBanca,'Banca Personas'),
	PERFIL_CONVENIO=ISNULL(datosBel.PERFIL_CONVENIO,'Otros'),
	PERFIL_USUARIO=ISNULL(datosBel.PERFIL_USUARIO,'Otros'),
	TIPO_TOKEN=ISNULL(datosBel.TIPO_TOKEN,'Otros'),
	TIPO_USUARIO=ISNULL(datosBel.TIPO_USUARIO,'Otros'),
	Responsable=ISNULL(Cuentas.Responsable,'Otros'),
	Segmento=ISNULL(Cuentas.Segmento,'Otros'),
	Perfil_UsuarioDesc,
	[Tipo de cliente].[Tipo_Cliente] AS [Tipo_Cliente],
	NombreCliente,
	Zona,
	Depto,
	BancaE,
	DW_SECTOR_DESCRIPCION,
	UsuarioActivo,
	UsuarioInactivo,
	CLSTAT,
	CantidadUsuario,
	Clientes_consorcio

FROM (
	SELECT *
	FROM (
	SELECT 
		dw_bel_IBSTTRA_VIEW.dw_fecha_trx Fecha,
		dw_bel_IBSTTRA_VIEW.USCODE Usuario,
		CASE 
		WHEN dw_bel_IBSTTRA_VIEW.SECODE = 'app-login' THEN 'Login'
		ELSE dw_bel_IBSTTRA_VIEW.SECODE END Modulo,
		LTRIM(dw_bel_IBSTTRA_VIEW.CLCCLI) Codigo_Cliente,
		'n/a' Moneda,
		0 Valor,
		0 ValorLempirizado,
		0 ValorDolarizado,
		case 
		when dw_bel_IBSTTRA_VIEW.SECODE IN ( 'login', 'web-login' ) then 'Web' 
		when dw_bel_IBSTTRA_VIEW.SECODE = 'app-login' then 'App nueva' 
		ELSE dw_bel_IBSTTRA_VIEW.CACODE END Canal,
		case 
		when dw_bel_IBSTTRA_VIEW.SECODE IN ( 'login', 'web-login' ) then 'Login Web' 
		ELSE DW_BEL_IBSERV.SEDESC + ' - ' + dw_bel_IBSTTRA_VIEW.SECODE END Operación,
		'n/a' Cuenta,
		dw_bel_IBSTTRA_VIEW.TRHORA Hora,
		NULL [Es Reversa],
		0 AS [Cantidad]
	FROM 
		dw_bel_IBSTTRA_VIEW
			LEFT JOIN DW_BEL_IBSERV ON DW_BEL_IBSERV.SECODE= dw_bel_IBSTTRA_VIEW.SECODE
	WHERE 
		(
		dw_bel_IBSTTRA_VIEW.dw_fecha_trx >= '2026-01-01'
		--BETWEEN '02/01/2025 00:00:00' AND '02/28/2025 23:59:59' 
		
		AND dw_bel_IBSTTRA_VIEW.SECODE IN 
			( 'login', 'web-login','app-login', --Login las demas son consultas
			'pym-desemb','app-edocta',
			'res-edoct2','con-sal2 ',
			'app-hisptm','app-extcns',
			'seg-cnhtrx','mpg-ccnspg',
			'app-cnsdiv','estado-cta',
			'con-sal','res-ctacor',
			'mpg-ccnhp','adq-edocta',
			'dei-cnpag','dei-cnsapg',
			'adq-cnstrx','ptr-cnsptm',
			'pym-cnsinf','ptr-cnssal',
			'chp-cnschq','adq-cnsrec',
			'pym-cnsmov','ptr-cnspar',
			'adq-voltrx','cns-chqtrc',
			'cpr-lincre','app-divinf',
			'adq-cncrad','adq-estcra',
			'ach-cnsope','adq-cnspos',
			'pla-consul',
			'prf-receml', --Gestiones
			'prf-recosm', --Gestiones
			'TRANSF-INT', --Gestiones
			'APP-TRAINT'  --Gestiones
			)
		)
UNION ALL
--INCIDENTES
SELECT  
	Fecha,
	Usuario,
	Modulo,
	Codigo_Cliente,
	Moneda,
	Valor,
	ValorLempirizado,
	ValorDolarizado,
	Canal,
	Operación,
	Cuenta,
	Hora,
	NULL [Es Reversa],
	0 AS [Cantidad]
FROM (
	SELECT DISTINCT 
		Incidentes.Incidente,Incidentes.DW_FECHA_HORA_ENTRADA AS Fecha,
		'n/a' Usuario,
		'Gestiones CRM' Modulo,
		cast( cast( tccli.CIF as int)  as varchar) AS Codigo_Cliente,
		NULL Moneda,
		NULL Valor,
		0 ValorLempirizado,
		0 ValorDolarizado,
		'App' Canal,
		Proceso.Nombre Operación,
		NULL Cuenta,
		Hora = DATEPART(HOUR, Incidentes.Fecha_hora_entrada)
	FROM (
		--Inicio: 1/8/2023
		SELECT 
			Incidente,Proceso,Estado,Fecha_hora_entrada,DW_FECHA_HORA_ENTRADA,DW_CLIENTE_LLAVE
		FROM 
			DWHBI..WF_Incidente
		WHERE 
			Fecha_hora_entrada >= '2026-01-01'
			--BETWEEN '02/01/2025 00:00:00' AND '02/28/2025 23:59:59' 
			AND Proceso IN
				(
				24076,24075,24072,24077,24083,24089, --Inicio: 1/8/2023
				24503,24519,24604,24622,   --Inicio: 22/11/2023
				24840,24842,24843,24642,24630, --Inicio: 6/1/2024
				24447,    --Inicio: 11/1/2023 Nueva Gestion 
				24980,24941,   --Inicio: 09/07/2024 
				25555,25554, ----Inicio 26/07/2024
				25530,25404, -- Inicio 10/09/2023
				21970, -- Inicio 01/01/2023
				21002, -- Inicio 02/01/2023
				26832, -- Inicio 22/01/2025
				26834, -- Inicio 22/01/2025
				27207, -- Inicio 01/05/2025
				27216, -- Inicio 01/05/2025
				25684, -- Inicio 04/08/2025	
				27959 --Inicio 28/10/2025	
				)
		
		UNION ALL
		--Inicio 18/05/2025
		SELECT 
			Incidente,Proceso,Estado,Fecha_hora_entrada,DW_FECHA_HORA_ENTRADA,DW_CLIENTE_LLAVE
		FROM 
			DWHBI..WF_Incidente
		WHERE 
			Fecha_hora_entrada >= '2026-01-01'
			--BETWEEN '02/01/2025 00:00:00' AND '02/28/2025 23:59:59' 
			AND Proceso IN
			(
			27150,27159,27160,  -- Inicio 18/05/2025
			27161,27162,27163,-- Inicio 18/05/2025
			27165,27166,27147,-- Inicio 18/05/2025
			27148,27149,27151,-- Inicio 18/05/2025
			27152,27153,-- Inicio 18/05/2025
			27370,  -- Inicio 10/06/2025 
			27874, --Inicio 01/06/2025
			27279,--25/05/2025
			27544,--31/07/2025
			27527, -- 28/07/2025
			27445, --17/07/2025
			27673-- 14/08/2025

			)

	) Incidentes
		LEFT JOIN DWHBI..WF_Proceso Proceso on Proceso.Proceso=Incidentes.Proceso
		INNER JOIN DWHBI..WF_Etapa_incidente EtapaIncidente ON EtapaIncidente.Incidente=Incidentes.Incidente
		LEFT JOIN DWHBI..DW_TC_CLI_CLIENTE tccli ON tccli.DW_CLIENTE_LLAVE = Incidentes.DW_CLIENTE_LLAVE
	WHERE 
	Incidentes.Estado <> 4
) BaseIncidentes

UNION ALL

SELECT 
	Fecha,
	Usuario,
	Modulo,
	Codigo_Cliente,
	Moneda,
	Valor,
	CASE
		WHEN Moneda in ( 'EUR', 'US$' ) THEN
			Valor *
				(
				SELECT TOP 1
				t.CMVALT
				FROM DWHBP.dbo.DW_CON_TIPOS_CAMBIO t
				WHERE t.DW_MONCOD = Moneda
				AND t.dw_fecha <= Fecha
				ORDER BY t.dw_fecha DESC
				)
	ELSE Valor
	END AS ValorLempirizado,
	CASE
		WHEN Moneda in ('L','001' ) THEN
			Valor /
				(
				SELECT TOP 1
				t.CMVALT
				FROM DWHBP.dbo.DW_CON_TIPOS_CAMBIO t
				WHERE t.DW_MONCOD = 'US$'
				AND t.dw_fecha <= Fecha
				ORDER BY t.dw_fecha DESC
				)
	ELSE Valor
	END AS ValorDolarizado,
	Canal,
	Operación,
	Cuenta,
	Hora,
	NULL [Es Reversa],
	[Cantidad]
FROM
(
---- TRX JOURNAL
	SELECT 
		dw_BEL_IBJOUR.dw_fecha_journal Fecha,
		'n/a' Usuario,
		'Trx Journal' Modulo,
		LTRIM(RTRIM(dw_BEL_IBJOUR.CLCCLI)) Codigo_Cliente,
		dw_BEL_IBJOUR.JOMONT Moneda,
		dw_BEL_IBJOUR.JOVAOR Valor,
		CASE
			WHEN CACODE = 'AP' THEN 'App'
			WHEN CACODE = 'IB' THEN 'Web'
		ELSE CACODE
		END AS Canal,
		DW_BEL_IBSERV_ALIAS.SEDESC + ' - ' + dw_BEL_IBJOUR.SECODE Operación,
		'n/a' Cuenta,
		dw_BEL_IBJOUR.JOTIM Hora,
		0 AS [Cantidad]
	FROM 
		dw_BEL_IBJOUR
			LEFT JOIN DW_BEL_IBSERV DW_BEL_IBSERV_ALIAS ON (DW_BEL_IBSERV_ALIAS.SECODE = dw_BEL_IBJOUR.SECODE)
	WHERE 
	(
		dw_BEL_IBJOUR.dw_fecha_journal >= '2026-01-01'
		AND dw_BEL_IBJOUR.JOSTAT = 1
		AND dw_BEL_IBJOUR.JOSECU = CASE WHEN dw_BEL_IBJOUR.SECODE = 'ope-cmpdiv' AND dw_BEL_IBJOUR.dw_fecha_journal >'06-17-2023'  THEN 2 ELSE 1 END
		AND dw_BEL_IBJOUR.CACODE IN  ('IB','AP')
		AND dw_BEL_IBJOUR.SECODE NOT IN 
		( 'mpg-cpago',
		'cns-cdv',
		'cns-pcit',
		'cns-vdv',
		'app-cpago', 
		'app-cnspci',
		'app-debtar',
		'app-cnscdv'--,
		--'ope-cmpdiv' 
		) -- Excluyo Multipagos y se excluyen consultas por migracion
	)
	
UNION ALL

	-- PLANILLA
	SELECT 
		dw_bel_IBHISPLA.dw_fecha_sistema AS Fecha,
		'n/a' AS Usuario,
		'Planilla' Modulo,
		LTRIM(RTRIM(DW_DEP_DEPOSITOS.CLDOC)) AS Codigo_Cliente,
		dw_bel_IBHISPLA.MONCOD Moneda,
		dw_bel_IBHISPLA.PLDVAL Valor,
		'Web' Canal,
		'Planilla' Operación,
		dw_bel_IBHISPLA.PLCEDT Cuenta,
		PLHGPD Hora,
		dw_bel_IBHISPLA.PLCORT AS [Cantidad]
	FROM 
		dw_bel_IBHISPLA 
			INNER JOIN DW_DEP_DEPOSITOS ON dw_bel_IBHISPLA.PLCEDT= DW_DEP_DEPOSITOS.CTEDIT
	WHERE 
		(
		dw_bel_IBHISPLA.dw_fecha_sistema >= '2026-01-01'
		AND dw_bel_IBHISPLA.PLDERR  =  0
		AND (case when (dw_bel_IBHISPLA.PLDVAL >= 0 and dw_bel_IBHISPLA.PLDVAL<=1) and ltrim(rtrim(DW_DEP_DEPOSITOS.CLDOC)) IN ('2169011','2285579','2496312','2285625','2058276') THEN 'Pruebas' ELSE 'Valida' end)='Valida'
		)

UNION ALL

	-- ACH Y ACH QR
	SELECT 
		DW_DEP_DPMOVM_VIEW.dw_fecha_operacion Fecha,
		'n/a' Usuario,
		CASE WHEN DW_DEP_DPMOVM_VIEW.TRXCOD IN (4210,4212,4232,4234) THEN 'ACH QR'
			 ELSE 'ACH' END Modulo, --Se agrega ACH QR
		LTRIM(RTRIM(DW_DEP_DEPOSITOS.CLDOC)) Codigo_Cliente,
		tr_dep_DPTB09.TB9MON Moneda,
		DW_DEP_DPMOVM_VIEW.MVVAL Valor,
		'Web y App' Canal,
		DW_DEP_DPMOVM_VIEW.dw_transaccion Operación,
		DW_DEP_DPMOVM_VIEW.dw_cuenta_corporativa Cuenta,
		MVHOEM Hora,
		0 AS [Cantidad]
	FROM 
		DW_DEP_DPMOVM_VIEW 
			INNER JOIN TR_DEP_DPTRX ON (DW_DEP_DPMOVM_VIEW.TRXCOD = TR_DEP_DPTRX.TRXCOD)
			INNER JOIN tr_dep_DPTB09 ON (TR_DEP_DPTRX.TRXN01 = tr_dep_DPTB09.TB9COD)
			LEFT JOIN DW_DEP_DEPOSITOS ON (DW_DEP_DEPOSITOS.DW_CUENTA_CORPORATIVA = DW_DEP_DPMOVM_VIEW.dw_cuenta_corporativa)
	WHERE 
		(
		DW_DEP_DPMOVM_VIEW.dw_fecha_operacion >= '2026-01-01'
		AND DW_DEP_DPMOVM_VIEW.TRXCOD IN ( 4823, 4825, 4824, 4822, 4652, 4654, 4651, 4653,4210,4212,4232,4234   )
		AND DW_DEP_DPMOVM_VIEW.MVSTAT <> 'R'
		)

UNION ALL

	--- PROVEEDORES
	SELECT 
		dw_bel_IBHLTP.dw_fecha_aplicado Fecha,
		'n/a' Usuario,
		'Proveedores' Modulo,
		LTRIM(RTRIM(dw_bel_IBHLTP.CLCCLI)) Codigo_Cliente,
		dw_bel_IBHLTP.MNDLOT Moneda,
		dw_bel_IBHLTP.MONLOS Valor,
		'Web' Canal,
		CASE 
			WHEN TIPPAG = '1' THEN 'Crédito a cuentas propias' 
			WHEN TIPPAG = '3' THEN 'ACH proveedores' 
			WHEN TIPPAG = '4' THEN 'Transferencia Internacional' 
		ELSE 'ND'
		end Operación,
		'n/a' Cuenta,
		dw_bel_IBHLTP.HORCRE Hora,
		dw_bel_IBHLTP.CANPRO AS [Cantidad]
	FROM 
		dw_bel_IBHLTP
	WHERE 
		(
		dw_bel_IBHLTP.dw_fecha_aplicado >= '2026-01-01'
		AND dw_bel_IBHLTP.LOCERR = 0
		--AND dw_bel_IBHLTP.CACODE = 'IB'
		--AND dw_bel_IBHLTP.STALOT = 'OPE'
		)

) Transacciones

UNION ALL 
------------------ Multipagos ------------------
	SELECT
		DW_FECHA_OPERACION_SP AS Fecha,
		ClientesBel.USCODE Usuario,
		'Multipagos' AS Modulo,
		ClientesBel.CLCCLI Codigo_Cliente,
		CLMOCO Moneda,
		DW_MUL_SPPADAT.SPPAVA AS Valor,
		DW_MUL_SPPADAT.SPPAVA * 
			CASE 
			WHEN CLMOCO='US$' THEN  
			(
			SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT 
			FROM DW_CON_TIPOS_CAMBIO
			WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND 
			DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
			ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
			) 
		ELSE 1 END ValorLempirizado,
		DW_MUL_SPPADAT.SPPAVA / 
			CASE 
			WHEN CLMOCO='US$' THEN  
			(
			SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT 
			FROM 
			DW_CON_TIPOS_CAMBIO
			WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND 
			DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP
			ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
			) 
		ELSE 1 END AS ValorDolarizado,
		DW_MUL_SPPADAT.SPCPDE AS Canal,
		DW_MUL_SPMACO.SPNOMC AS Operación,
		NULL Cuenta,
		DW_MUL_SPPADAT.SPPAHR Hora,
		DW_MUL_SPPADAT.SPPAFR AS [Es Reversa],
		0 AS [Cantidad]
	FROM
	DW_MUL_SPMACO 
		INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC=DW_MUL_SPPADAT.SPCODC)
		LEFT JOIN (
			SELECT
				LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
				LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
			FROM
				DW_BEL_IBUSER 
			) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI+ClientesBel.USCODE)
	WHERE
		(
		DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-01-01'
		AND DW_MUL_SPPADAT.SPCPCO  IN  ( 1, 7  )
		)
		
UNION ALL

	------Dividelo Todo App
	SELECT 
		DW_BEL_IBLOGDTO.DW_FECHA AS Fecha,
		DW_BEL_IBUSER.USCODE AS Usuario,
		'Dividelo Todo' AS Modulo,
		LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI)) AS Codigo_Cliente,
		DW_BEL_IBLOGDTO.LOMONEDA AS Moneda,
		DW_BEL_IBLOGDTO.LOIMPORT AS Valor ,
		DW_BEL_IBLOGDTO.LOIMPORT  * 
			CASE 
			WHEN DW_BEL_IBLOGDTO.LOMONEDA='US$' OR DW_BEL_IBLOGDTO.LOMONEDA='USD' THEN 
			(
			SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT 
			FROM DW_CON_TIPOS_CAMBIO
			WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND 
			DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_BEL_IBLOGDTO.DW_FECHA 
			ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
			)
		ELSE 1 END AS ValorLempirizado,
		DW_BEL_IBLOGDTO.LOIMPORT / 
			CASE 
			WHEN DW_BEL_IBLOGDTO.LOMONEDA='L' THEN 
			(
			SELECT TOP 1 DW_CON_TIPOS_CAMBIO.CMVALT 
			FROM DW_CON_TIPOS_CAMBIO
			WHERE 'US$'=DW_CON_TIPOS_CAMBIO.DW_MONCOD AND 
			DW_CON_TIPOS_CAMBIO.dw_fecha<= DW_BEL_IBLOGDTO.DW_FECHA 
			ORDER BY DW_CON_TIPOS_CAMBIO.dw_fecha DESC
			)
		ELSE 1 END AS ValorDolarizado,
		'APP' AS Canal,
		'Dividelo Todo App' AS Operación,
		DW_BEL_IBLOGDTO.LONUMTAR AS Cuenta,
		DW_BEL_IBLOGDTO.LOGHOR AS Hora,
		NULL [Es Reversa],
		1 AS Cantidad
	FROM
		DW_BEL_IBCLIE 
			INNER JOIN DW_BEL_IBUSER ON (DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI)
			INNER JOIN DW_BEL_IBLOGDTO ON (DW_BEL_IBUSER.USCODE=DW_BEL_IBLOGDTO.LOGUSR)
	WHERE
		DW_BEL_IBLOGDTO.DW_FECHA >= '2026-01-01'
		and DW_BEL_IBLOGDTO.LOCODERR = 0

UNION ALL

	-----Bloqueo y desbloqueo TC/TD APP-------------------
	SELECT  
		DW_BEL_IBLOGBDT.DW_FECHA AS Fecha,
		DW_BEL_IBLOGBDT.LOGUSR AS Usuario,
		'Bloqueo y desbloqueo TC' AS Modulo,
		LTRIM(RTRIM(MT.NoClienteUnico)) AS Codigo_Cliente,
		'n/a' Moneda,
		0 Valor,
		0 ValorLempirizado,
		0 ValorDolarizado,
		DW_BEL_IBLOGBDT.LOCANAL AS Canal,
		CASE 
			WHEN LOACCION = 'B' AND  DW_BEL_IBLOGBDT.LOTIPTAR = 'C' THEN 'Bloqueo TC'
			WHEN LOACCION = 'B' AND  DW_BEL_IBLOGBDT.LOTIPTAR <> 'C' THEN 'Bloqueo TD'
			WHEN LOACCION = 'D' AND  DW_BEL_IBLOGBDT.LOTIPTAR = 'C' THEN 'Desbloqueo TC'
			WHEN LOACCION = 'D' AND  DW_BEL_IBLOGBDT.LOTIPTAR <> 'C' THEN 'Desbloqueo TD' 
		END Operación,
		DW_BEL_IBLOGBDT.LONUMTAR Cuenta,
		LOGHOR AS Hora,
		NULL [Es Reversa],
		1 as Cantidad
	FROM
		DW_BEL_IBLOGBDT WITH (NOLOCK)
			INNER JOIN DWH_MaestroTarjetas MT WITH (NOLOCK) ON (DW_BEL_IBLOGBDT.LONUMTAR=MT.NumeroTarjeta)
	WHERE
		(
		DW_BEL_IBLOGBDT.DW_FECHA >= '2026-01-01'
		AND	DW_BEL_IBLOGBDT.LOCODERR  =  0
		)
	
UNION ALL

	----------------Limite por usuario y categoria
	SELECT
		DW_BEL_IBLGLXUS.DW_FECHA_CONTROL AS Fecha,
		DW_BEL_IBLGLXUS.USCODE AS Usuario,
		'Limite por usuario y categoria' AS Modulo,
		ClientesBel.CLCCLI  AS Codigo_Cliente,
		'n/a' Moneda,
		0 Valor,
		0 ValorLempirizado,
		0 ValorDolarizado,
		DW_BEL_IBLGLXUS.CACODE AS Canal,
		DW_BEL_IBLGLXUS.IBCCOD AS Operación,
		DW_BEL_IBLGLXUS.LOGCTAS Cuenta,
		cast(SUBSTRING(REPLACE(LMFISO, '.', ''), 12, 6) as int) AS Hora,
		NULL [Es Reversa],
		1 as Cantidad
	FROM
		DW_BEL_IBLGLXUS
			LEFT JOIN (
					SELECT
						LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
						LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
					FROM 
						DW_BEL_IBUSER 
				) ClientesBel ON (DW_BEL_IBLGLXUS.USCODE = ClientesBel.USCODE)
	WHERE   
		DW_BEL_IBLGLXUS.DW_FECHA_CONTROL >= '2026-01-01'

UNION ALL

	--------------------------Aprovisionamiento TC/TD
	SELECT
		DW_BEL_IBLGAPPY.DW_FECHA_CONTROL AS Fecha,
		DW_BEL_IBLGAPPY.IBBUSR AS Usuario,
		'Aprovisionamiento TC-TD' AS Modulo,
		ClientesBel.CLCCLI  AS Codigo_Cliente,
		'n/a' Moneda,
		0 Valor,
		0 ValorLempirizado,
		0 ValorDolarizado,
		DW_BEL_IBLGAPPY.IBBCAN AS Canal,
		CASE WHEN DW_BEL_IBLGAPPY.IBTYPT = 'C' THEN 'Aprovisionamiento TC' ELSE 'Aprovisionamiento TD' END AS Operación,
		DW_BEL_IBLGAPPY.IBBCTA Cuenta,
		cast(SUBSTRING(REPLACE(IBBFEC, '.', ''), 12, 6) as int) AS Hora,
		NULL [Es Reversa],
		1 as Cantidad
	FROM
		DW_BEL_IBLGAPPY
			LEFT JOIN (
				SELECT
					LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
					LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
				FROM
					DW_BEL_IBUSER 
				) ClientesBel ON (DW_BEL_IBLGAPPY.IBBUSR = ClientesBel.USCODE)
	WHERE   
		DW_BEL_IBLGAPPY.DW_FECHA_CONTROL >= '2026-01-01'
		
	) TransaccionesBase
	WHERE
		TransaccionesBase.Canal in ( 'App nueva','AP','APP')
		AND TransaccionesBase.Fecha >= '2026-01-01'
) Transacciones

LEFT JOIN 
	(
	SELECT 
			CLCCLI,
			USCODE,
			TipoBanca,
			TIPO_TOKEN,
			NombreCliente,
			PERFIL_CONVENIO,
			PERFIL_USUARIO,
			TIPO_USUARIO,
			Perfil_UsuarioDesc,
			UsuarioActivo,
			UsuarioInactivo,
			CantidadUsuario,
			CLSTAT,
			Clientes_consorcio
			 
		FROM 
			(
			SELECT
				LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI)) CLCCLI,
				LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE,
				TipoBanca=
				CASE WHEN  ( DW_BEL_IBCLIE.CLTIPE  =  'N' AND DW_BEL_IBUSER.PEUSUA IN ('BPES','PYME') ) OR DW_BEL_IBCLIE.CLTIPE  =  'J' THEN 'Banca Empresas' 
				ELSE 'Banca Personas' END,
				CASE
					WHEN SOFT_TOKEN.USCODE IS NOT NULL THEN 'SOFT TOKEN'
					WHEN DW_BEL_IBPNUS.USPNST = 1 AND DW_BEL_IBPNUS.USBPST = 1 THEN 'PUSH NOTIFICATIONS'
					WHEN RTRIM(LTRIM(DW_BEL_IBUSER.USTRCA)) <> '' THEN 'TOKEN SMS'
					WHEN DW_BEL_IBUSER.USSTOK <> 0 THEN 'TOKEN FISICO'
				ELSE 'NO TIENE TOKEN'
				END TIPO_TOKEN,
				DW_BEL_IBCLIE.CLNOCL NombreCliente,
				DW_BEL_IBCLIE.PECODE AS PERFIL_CONVENIO,
				DW_BEL_IBUSER.PEUSUA AS PERFIL_USUARIO,
				DW_BEL_IBUSER.USTIUS AS TIPO_USUARIO,
				TR_BEL_IBPERF.PEDESC AS Perfil_UsuarioDesc,
				CLSTAT,
				Clientes_consorcio,
				UsuarioActivo=ISNULL(cu.UsuarioActivo, 0),
				UsuarioInactivo=ISNULL(cu.UsuarioInactivo, 0),
				CantidadUsuario=ISNULL(cu.CantidadUsuario, 0),
				RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI)) ORDER BY DW_BEL_IBCLIE.CLCCLI,CLSTAT,USSTAT ASC,DW_BEL_IBUSER.dw_fecha_creacion asc) 
			FROM
				DW_BEL_IBCLIE 
				LEFT JOIN (
				SELECT 
					e1.CLCCLI ,
					STUFF((
						SELECT ',' + e2.CLCCLC + ' - ' + e2.CLNOCL

						FROM dw_bel_IBCLIC AS e2
						WHERE e2.CLCCLI = e1.CLCCLI
						FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS Clientes_consorcio,
					RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(e1.CLCCLI)) ORDER BY e1.CLCCLI asc) 
				FROM dw_bel_IBCLIC AS e1
				GROUP BY CLCCLI
				) Consorcio ON Consorcio.CLCCLI = DW_BEL_IBCLIE.CLCCLI AND Consorcio.RN=1
					INNER JOIN 
					(
					SELECT DW_BEL_IBUSER.CLCCLI, --5
						   DW_BEL_IBUSER.PEUSUA,
						   DW_BEL_IBUSER.USTIUS,
						   DW_BEL_IBUSER.USSTOK,
						   DW_BEL_IBUSER.USTRCA,
						   DW_BEL_IBUSER.USCODE, --10
						   --UsuarioActivo=CASE WHEN USSTAT='A' THEN 1 ELSE 0 END,
						   --UsuarioInactivo=CASE WHEN USSTAT='I' THEN 1 ELSE 0 END,
						   DW_BEL_IBUSER.USSTAT,
						   DW_BEL_IBUSER.dw_fecha_creacion
					FROM DW_BEL_IBUSER
				
					)
					DW_BEL_IBUSER ON (DW_BEL_IBCLIE.CLCCLI=DW_BEL_IBUSER.CLCCLI) 
					 LEFT JOIN TR_BEL_IBPERF ON (DW_BEL_IBUSER.PEUSUA=TR_BEL_IBPERF.PECODE)
  

						LEFT JOIN 
							(
								SELECT
								DW_BEL_IBAUTUSR.USCODE,
								DW_BEL_IBAUTUSR.IBASST,
								DW_BEL_IBAUTUSR.IBESST
							FROM
								DW_BEL_IBAUTUSR
							WHERE
								( DW_BEL_IBAUTUSR.IBASST  =  'S'   AND   DW_BEL_IBAUTUSR.IBESST  =  'A' )
							) SOFT_TOKEN ON (DW_BEL_IBUSER.USCODE = SOFT_TOKEN.USCODE)
						LEFT JOIN DW_BEL_IBPNUS  WITH (NOLOCK) ON (DW_BEL_IBUSER.USCODE = DW_BEL_IBPNUS.USECODE)
						LEFT JOIN conteo_usuarios cu ON cu.CLCCLI = LTRIM(RTRIM(DW_BEL_IBCLIE.CLCCLI))
				) Datos
			WHERE 
				RN=1 
	) datosBel ON Transacciones.Codigo_Cliente = datosBel.CLCCLI 

LEFT JOIN
(
	SELECT
		TR_CIF_CLTIEJ.CLTJDE Segmento,
		tr_cif_clejne.CLEJDE Responsable,
		DW_DEP_DEPOSITOS.CLRESP,
		LTRIM(RTRIM(DW_DEP_DEPOSITOS.CLDOC)) CLDOC,
		DW_DEP_DEPOSITOS.DW_CUENTA_CORPORATIVA,
		DW_DEP_DEPOSITOS.DW_FEHA_APERTURA,
		RN=ROW_Number()OVER(PARTITION BY DW_DEP_DEPOSITOS.CLDOC ORDER BY DW_DEP_DEPOSITOS.DW_FEHA_APERTURA DESC)
	FROM
		DWHBP..TR_CIF_CLTIEJ 
			INNER JOIN DWHBP..tr_cif_clejne ON (TR_CIF_CLTIEJ.CLTJCO=tr_cif_clejne.CLTJCO)
			RIGHT OUTER JOIN DWHBP..DW_DEP_DEPOSITOS ON (DW_DEP_DEPOSITOS.CLRESP=tr_cif_clejne.CLEJCO and tr_cif_clejne.EMPCOD=1)

	WHERE 
		TR_CIF_CLTIEJ.CLTJDE IN 
			(
			'GTE CTA INTER COMERCIAL',
			'GERENTE DE CUENTA CORPORATIVO',
			'GTE DE CUENTA CASH MANAGEMENT',
			'GERENTE DE CUENTA PYME'
			)
) Cuentas ON Cuentas.CLDOC = Transacciones.Codigo_Cliente and Cuentas.RN=1
	 INNER JOIN (
	    SELECT DW_CIF_CLIENTES.DW_SECTOR_DESCRIPCION,
	         LTRIM(RTRIM(DW_CIF_CLIENTES.CLDOC)) AS CIF,
         DW_CIF_CLIENTES.CLTIPE AS Tipo_Cliente,
		 DW_CIF_DIRECCIONES.DW_NIVEL_GEO1 Zona,
		 DW_CIF_DIRECCIONES.DW_NIVEL_GEO2 Depto,
		 DW_CIF_CLIENTES.dw_usuarios_bel_cnt BancaE,
		 DW_CIF_DIRECCIONES.CLDICO,
		 RN=ROW_Number()OVER(PARTITION BY LTRIM(RTRIM(DW_CIF_DIRECCIONES.CLDOC)) ORDER BY DW_CIF_DIRECCIONES.dw_fecha DESC)
  FROM
        DW_CIF_CLIENTES 
		LEFT JOIN  DW_CIF_DIRECCIONES on DW_CIF_CLIENTES.CLDOC=DW_CIF_DIRECCIONES.CLDOC
	WHERE DW_CIF_DIRECCIONES.CLDICO=1
) [Tipo de cliente] ON ([Transacciones].[Codigo_Cliente] = [Tipo de cliente].[CIF] and [Tipo de cliente].RN=1)
WHERE
Tipo_Cliente = 'J'
AND
BancaE = 1
