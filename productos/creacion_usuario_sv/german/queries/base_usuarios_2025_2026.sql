SELECT
    usuarios.dw_fecha_creacion AS fecha_creacion_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8) AS codigo_cliente_usuario_creado,
    usuarios.uscode AS nombre_usuario,
    info_clientes.CLISEX AS genero_cliente,
    info_clientes.DW_FECHA_NACIMIENTO AS fecha_nacimiento_usuario
FROM
    dw_bel_ibuser usuarios
    INNER JOIN dw_bel_ibclie clientes_bel ON usuarios.CLCCLI = clientes_bel.CLCCLI
    INNER JOIN DW_CIF_CLIENTES info_clientes
        ON RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8) = RIGHT('00000000' + RTRIM(LTRIM(info_clientes.CLDOC)), 8)
WHERE
    usuarios.dw_fecha_creacion >= '2025-01-01 00:00:00'
    AND usuarios.dw_fecha_creacion < '2027-01-01 00:00:00'
    AND clientes_bel.cltipe = 'N';
