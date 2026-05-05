/*
Compradores de Superpack Claro >= L.120 - Marzo 2026 (universo completo)

Universo: todos los compradores de Superpack en marzo 2026, sin filtrar
por lista de contactados.

Filtros alineados con dashboard:
  - Codigo Superpack: 498 (Claro)
  - Canales banca electronica: SPCPCO IN (1, 7)
  - Solo lempiras: CLMOCO IN ('001', 'L')
  - Excluye juridicos: CLTIPE <> 'J'
  - Excluye reversas para el umbral de L.120 (sppafr = 'N')

Salida:
  - total_compradores      : clientes unicos con al menos 1 compra efectiva
  - compradores_120_o_mas  : clientes con al menos 1 trx >= L.120 (no reversa)
  - compradores_menos_120  : resto
  - pct_120_o_mas          : porcentaje sobre el total de compradores
*/

WITH trx_superpack AS (
    SELECT
        ClientesBel.CLCCLI                   AS padded_codigo_cliente,
        CAST(p.sppava AS DECIMAL(18, 2))      AS monto_operacion,
        p.sppafr                              AS es_reversa
    FROM dw_mul_sppadat p
    LEFT JOIN dw_mul_spmaco m
        ON m.spcodc = p.spcodc
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
            LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
    ) ClientesBel
        ON LTRIM(RTRIM(p.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(CLDOC)) AS CLDOC,
            CLTIPE,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(CLDOC))
                ORDER BY CASE WHEN CLTIPE = 'N' THEN 1 WHEN CLTIPE IS NULL THEN 2 ELSE 3 END
            ) AS RN
        FROM DW_CIF_CLIENTES
    ) CIF
        ON CIF.CLDOC = ClientesBel.CLCCLI AND CIF.RN = 1
    WHERE p.dw_fecha_operacion_sp >= '2026-03-01'
      AND p.dw_fecha_operacion_sp <  '2026-04-01'
      AND TRY_CONVERT(INT, p.spcodc) = 498
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
      AND ClientesBel.CLCCLI IS NOT NULL
      AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
),
compradores_efectivos AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM trx_superpack
    WHERE es_reversa = 'N'
),
compradores_120 AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM trx_superpack
    WHERE es_reversa = 'N'
      AND monto_operacion >= 120
)
SELECT
    COUNT(DISTINCT e.padded_codigo_cliente)                         AS total_compradores,
    COUNT(DISTINCT c.padded_codigo_cliente)                         AS compradores_120_o_mas,
    COUNT(DISTINCT e.padded_codigo_cliente)
        - COUNT(DISTINCT c.padded_codigo_cliente)                   AS compradores_menos_120,
    CAST(
        COUNT(DISTINCT c.padded_codigo_cliente) * 100.0
        / NULLIF(COUNT(DISTINCT e.padded_codigo_cliente), 0)
        AS DECIMAL(5, 2)
    )                                                               AS pct_120_o_mas
FROM compradores_efectivos e
LEFT JOIN compradores_120 c
    ON c.padded_codigo_cliente = e.padded_codigo_cliente;
