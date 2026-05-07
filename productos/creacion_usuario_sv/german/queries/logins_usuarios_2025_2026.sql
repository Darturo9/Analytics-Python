SELECT
    RIGHT('00000000' + RTRIM(LTRIM(B.LOGCLI)), 8) AS codigo_cliente_login,
    B.LOGSER AS tipo_login,
    B.LOGANO AS anio_login,
    B.LOGMES AS mes_login
FROM
    DW_BEL_IBCLIE A
    INNER JOIN DW_BEL_IBLOGDQUSR B ON (A.CLCCLI = B.LOGCLI)
WHERE
    B.LOGANO >= 2025
    AND B.LOGANO <= 2026
    AND B.LOGTIP IN ('I')
    AND B.LOGSER IN ('app-login')
    AND A.CLTIPE = 'N';
