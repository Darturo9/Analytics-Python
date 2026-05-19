-- =====================================================================
-- Superpack Claro — Compras por quincena (detalle transaccional + demografia)
-- Criterios:
--   - Codigo superpack: 498
--   - Solo canales banca electronica: SPCPCO IN (1, 7)
--   - Solo lempiras: CLMOCO IN ('001', 'L')
--   - Excluye juridicos: CLTIPE = 'J'
--   - Incluye reversas para calcular neto en Python (sppafr)
--   - Demografía incluida en la misma consulta (1 solo round trip)
--
-- Parametros esperados (desde Python):
--   fecha_inicio        (DATE) obligatorio
--   fecha_fin_exclusiva (DATE) obligatorio
-- =====================================================================

WITH trx_superpack AS (
    SELECT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL                      THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1 THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )),
            8
        )                                                                             AS padded_codigo_cliente,
        CONVERT(DATE, p.dw_fecha_operacion_sp)                                        AS fecha_operacion,
        TRY_CONVERT(INT, p.spcodc)                                                    AS codigo_superpack,
        COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.spcpde AS VARCHAR(60)))), ''), 'SIN_DATO') AS canal_operacion_raw,
        TRY_CONVERT(INT, p.spcpco)                                                    AS canal_operacion_codigo,
        CAST(p.sppava AS DECIMAL(18, 2))                                              AS monto_operacion,
        p.sppafr                                                                      AS es_reversa,
        CASE
            WHEN TRY_CONVERT(INT, p.SPPAHR) IS NULL THEN NULL
            WHEN TRY_CONVERT(INT, p.SPPAHR) < 24    THEN TRY_CONVERT(INT, p.SPPAHR)
            WHEN TRY_CONVERT(INT, p.SPPAHR) < 2400  THEN TRY_CONVERT(INT, p.SPPAHR) / 100
            ELSE                                          TRY_CONVERT(INT, p.SPPAHR) / 10000
        END                                                                           AS hora_operacion
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m ON m.spcodc = p.spcodc
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND TRY_CONVERT(INT, p.spcodc) = 498
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
),
clientes_unicos AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM trx_superpack
    WHERE padded_codigo_cliente IS NOT NULL
),
cif AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente,
        MAX(c.CLISEX)                                  AS genero_raw,
        MAX(CAST(c.DW_FECHA_NACIMIENTO AS DATE))        AS fecha_nacimiento,
        MAX(c.CLTIPE)                                   AS tipo_cliente
    FROM DW_CIF_CLIENTES c
    INNER JOIN clientes_unicos cu
        ON RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) = cu.padded_codigo_cliente
    GROUP BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
),
deptos AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)                              AS padded_codigo_cliente,
        MAX(COALESCE(NULLIF(LTRIM(RTRIM(d.dw_nivel_geo2)), ''), 'SIN DATO'))       AS departamento
    FROM DW_CIF_DIRECCIONES_PRINCIPAL d
    INNER JOIN clientes_unicos cu
        ON RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) = cu.padded_codigo_cliente
    GROUP BY RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
)
SELECT
    t.padded_codigo_cliente,
    t.fecha_operacion,
    t.canal_operacion_raw,
    t.canal_operacion_codigo,
    t.monto_operacion,
    t.es_reversa,
    t.hora_operacion,
    c.genero_raw,
    c.fecha_nacimiento,
    COALESCE(dep.departamento, 'SIN DATO') AS departamento
FROM trx_superpack t
LEFT JOIN cif c  ON c.padded_codigo_cliente  = t.padded_codigo_cliente
LEFT JOIN deptos dep ON dep.padded_codigo_cliente = t.padded_codigo_cliente
WHERE t.padded_codigo_cliente IS NOT NULL
  AND COALESCE(c.tipo_cliente, 'N') <> 'J'
ORDER BY t.fecha_operacion, t.padded_codigo_cliente;
