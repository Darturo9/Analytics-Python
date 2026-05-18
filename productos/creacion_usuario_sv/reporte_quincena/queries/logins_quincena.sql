SELECT
    RIGHT('00000000' + RTRIM(LTRIM(login_view.clccli)), 8) AS codigo_cliente_login,
    login_view.uscode                                      AS nombre_usuario_login,
    login_view.secode                                      AS tipo_login,
    login_view.dw_fecha_trx                                AS fecha_login
FROM dw_bel_IBSTTRA_VIEW login_view
LEFT JOIN DW_CIF_CLIENTES clientes
    ON RIGHT('00000000' + RTRIM(LTRIM(clientes.CLDOC)), 8) = RIGHT('00000000' + RTRIM(LTRIM(login_view.clccli)), 8)
WHERE login_view.dw_fecha_trx >= :fecha_inicio
  AND login_view.dw_fecha_trx <  :fecha_fin_exclusiva
  AND login_view.secode IN ('app-login', 'web-login', 'login')
  AND login_view.clccli IS NOT NULL
  AND clientes.CLTIPE = 'N';
