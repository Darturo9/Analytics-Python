-- =====================================================================
-- Superpack Claro — Compras por quincena (detalle transaccional)
-- Criterios:
--   - Codigo superpack: 498
--   - Solo canales banca electronica: SPCPCO IN (1, 7)
--   - Solo lempiras: CLMOCO IN ('001', 'L')
--   - Excluye juridicos: CLTIPE <> 'J'
--   - Incluye reversas para calcular neto en Python (sppafr)
--
-- Nota de rendimiento:
--   El codigo cliente se extrae directamente de SPINUS con funciones de
--   string (sin JOIN a DW_BEL_IBUSER) para mayor velocidad.
--
-- Parametros esperados (desde Python):
--   fecha_inicio        (DATE) obligatorio  — inicio de quincena (inclusivo)
--   fecha_fin_exclusiva (DATE) obligatorio  — dia siguiente al fin de quincena
-- =====================================================================

WITH trx_superpack AS (
    SELECT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL                          THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1     THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1     THEN NULL
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
    INNER JOIN dw_mul_spmaco m
        ON m.spcodc = p.spcodc
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND TRY_CONVERT(INT, p.spcodc) = 498
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
)
SELECT
    t.padded_codigo_cliente,
    t.fecha_operacion,
    t.codigo_superpack,
    t.canal_operacion_raw,
    t.canal_operacion_codigo,
    t.monto_operacion,
    t.es_reversa,
    t.hora_operacion
FROM trx_superpack t
WHERE t.padded_codigo_cliente IS NOT NULL
ORDER BY t.fecha_operacion, t.padded_codigo_cliente;
