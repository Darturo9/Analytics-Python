SELECT
    usuarios.dw_fecha_creacion                                      AS fecha_creacion_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8)           AS codigo_cliente_usuario_creado,
    usuarios.uscode                                                 AS nombre_usuario,
    info_clientes.CLISEX                                            AS genero_cliente,
    info_clientes.DW_FECHA_NACIMIENTO                               AS fecha_nacimiento_usuario,
    usuarios.USSTAT                                                 AS estado_usuario,
    clientes_bel.CLSTAT                                             AS estado_cliente,
    direcciones.dw_nivel_geo1                                       AS direccion_lvl_1,
    direcciones.dw_nivel_geo2                                       AS direccion_lvl_2,
    direcciones.dw_nivel_geo3                                       AS direccion_lvl_3
FROM dw_bel_ibuser usuarios
INNER JOIN dw_bel_ibclie clientes_bel
    ON usuarios.CLCCLI = clientes_bel.CLCCLI
INNER JOIN DW_CIF_CLIENTES info_clientes
    ON RIGHT('00000000' + RTRIM(LTRIM(usuarios.CLCCLI)), 8) = RIGHT('00000000' + RTRIM(LTRIM(info_clientes.CLDOC)), 8)
LEFT JOIN dw_cif_direcciones_principal direcciones
    ON RTRIM(LTRIM(info_clientes.cltdoc)) = RTRIM(LTRIM(direcciones.cldoc))
WHERE usuarios.dw_fecha_creacion >= :fecha_inicio
  AND usuarios.dw_fecha_creacion <  :fecha_fin_exclusiva
  AND clientes_bel.cltipe = 'N';
