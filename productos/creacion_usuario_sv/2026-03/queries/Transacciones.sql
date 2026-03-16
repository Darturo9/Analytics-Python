SELECT  
   journal.dw_fecha_journal as fecha_transaccion,
   journal.CLCCLI as codigo_cliente_transaccion,
   RIGHT('00000000' +RTRIM(LTRIM(journal.CLCCLI)),8) as padded_codigo_cliente_transaccion,
   SUBSTRING(journal.jotrae,22,4) as codigo_transaccion,
   descripcion.SEDESC + ' - ' + journal.SECODE as codigo_desc_transaccion,
   journal.JOMONT as moneda,
   journal.JOVAOR as valor,	
   datos_cliente.CLTIPE as tipo_cliente,
   descripcion.SEDESC as descripcion,
   journal.CACODE  as medio,
   ROW_NUMBER() OVER (PARTITION BY journal.CLCCLI ORDER BY journal.dw_fecha_journal)	AS no_trx
FROM
	dw_BEL_IBJOUR journal
LEFT JOIN
	DW_CIF_CLIENTES datos_cliente
ON   RIGHT('00000000'+ RTRIM(LTRIM(datos_cliente.CLDOC)),8) = RIGHT('00000000'+ RTRIM(LTRIM(journal.CLCCLI)),8)
LEFT JOIN
	DW_BEL_IBSERV descripcion
ON journal.SECODE = descripcion.SECODE
WHERE
   journal.dw_fecha_journal  >=  '2025-01-01'
   AND journal.JOSTAT  =  1
   AND journal.JOSECU  =  1
   AND journal.CACODE  IN  ('IB', 'AP') 
   AND datos_cliente.CLTIPE = 'N'
   AND SUBSTRING(journal.jotrae,22,4) IN ( '5661',
		'5682',
		'5761',
		'5785',
		'5755',
		'5679',
		'5645',
		'365 ',
		'3714',
		'5405',
		'5685',
		'5686',
		'5769',
		'5687',  
		'5688',  
		'5774',
		'5677',
		'692',
		'1687',
		'1789',
		'4709',
		'5609',
		'793 ') 
   AND RTRIM(LTRIM(journal.CLCCLI)) NOT IN ('1651457','2380531')