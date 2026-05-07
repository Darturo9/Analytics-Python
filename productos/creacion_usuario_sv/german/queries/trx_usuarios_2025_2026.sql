SELECT
    journal.dw_fecha_journal AS fecha_transaccion,
    RIGHT('00000000' + RTRIM(LTRIM(journal.CLCCLI)), 8) AS codigo_cliente_transaccion,
    SUBSTRING(journal.jotrae, 22, 4) AS codigo_transaccion,
    journal.SECODE AS secode,
    descripcion.SEDESC AS descripcion_transaccion,
    journal.CACODE AS canal,
    journal.JOMONT AS moneda,
    journal.JOVAOR AS valor
FROM
    dw_BEL_IBJOUR journal
    LEFT JOIN DW_BEL_IBSERV descripcion ON journal.SECODE = descripcion.SECODE
    LEFT JOIN DW_CIF_CLIENTES datos_cliente
        ON RIGHT('00000000' + RTRIM(LTRIM(datos_cliente.CLDOC)), 8) = RIGHT('00000000' + RTRIM(LTRIM(journal.CLCCLI)), 8)
WHERE
    journal.dw_fecha_journal >= '2025-01-01 00:00:00'
    AND journal.dw_fecha_journal < '2027-01-01 00:00:00'
    AND journal.JOSTAT = 1
    AND journal.JOSECU = 1
    AND journal.CACODE IN ('IB', 'AP')
    AND datos_cliente.CLTIPE = 'N';
