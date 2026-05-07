SELECT
    usuarios.dw_fecha_creacion AS fecha_creacion_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8) AS codigo_cliente_usuario_creado,
    usuarios.uscode AS nombre_usuario
FROM
    dw_bel_ibuser usuarios
    INNER JOIN dw_bel_ibclie clientes_bel ON usuarios.CLCCLI = clientes_bel.CLCCLI
WHERE
    usuarios.dw_fecha_creacion >= '2026-01-01 00:00:00'
    AND usuarios.dw_fecha_creacion < '2026-05-01 00:00:00'
    AND clientes_bel.cltipe = 'N';
