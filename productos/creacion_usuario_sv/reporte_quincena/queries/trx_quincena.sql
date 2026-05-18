SELECT
    journal.dw_fecha_journal AS fecha_transaccion,
    RIGHT('00000000' + RTRIM(LTRIM(journal.CLCCLI)), 8) AS codigo_cliente_transaccion,
    COALESCE(descripcion.SEDESC + ' - ' + journal.SECODE, journal.SECODE) AS transaccion,
    journal.SECODE AS secode,
    CAST(journal.JOVAOR AS DECIMAL(18, 2)) AS monto_transaccion
FROM dw_BEL_IBJOUR journal
LEFT JOIN DW_CIF_CLIENTES datos_cliente
    ON RIGHT('00000000' + RTRIM(LTRIM(datos_cliente.CLDOC)), 8) = RIGHT('00000000' + RTRIM(LTRIM(journal.CLCCLI)), 8)
LEFT JOIN DW_BEL_IBSERV descripcion
    ON journal.SECODE = descripcion.SECODE
WHERE journal.dw_fecha_journal >= :fecha_inicio
  AND journal.dw_fecha_journal <  :fecha_fin_exclusiva
  AND journal.JOSTAT = 1
  AND journal.JOSECU = 1
  AND journal.CACODE IN ('IB', 'AP')
  AND datos_cliente.CLTIPE = 'N';
