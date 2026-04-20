/*
==============================================================================
Proyecto: Superpack Claro
Descripcion:
- Construye el universo de clientes unicos que compraron Superpack Claro
  en abril 2026 via Multipagos (spcodc = 498).
- Permite validar una lista de clientes para saber cuantos compraron.
==============================================================================
*/

USE DWHBP;
SET NOCOUNT ON;

DECLARE @fecha_inicio DATE = '2026-04-01';
DECLARE @fecha_fin_exclusiva DATE = '2026-05-01';
DECLARE @codigo_superpack INT = 498;

IF OBJECT_ID('tempdb..#compradores_superpack_abril_2026') IS NOT NULL
    DROP TABLE #compradores_superpack_abril_2026;

IF OBJECT_ID('tempdb..#clientes_lista_raw') IS NOT NULL
    DROP TABLE #clientes_lista_raw;

IF OBJECT_ID('tempdb..#clientes_lista_norm') IS NOT NULL
    DROP TABLE #clientes_lista_norm;

/*
1) Universo de compradores de Superpack (Multipagos)
*/
WITH trx_superpack AS (
    SELECT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )),
            8
        ) AS padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= @fecha_inicio
      AND p.dw_fecha_operacion_sp <  @fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = @codigo_superpack
)
SELECT
    t.padded_codigo_cliente,
    COUNT(*) AS total_tx,
    CAST(SUM(t.monto_operacion) AS DECIMAL(18, 2)) AS monto_total,
    MIN(t.fecha_operacion) AS primera_fecha_operacion,
    MAX(t.fecha_operacion) AS ultima_fecha_operacion
INTO #compradores_superpack_abril_2026
FROM trx_superpack t
WHERE t.padded_codigo_cliente IS NOT NULL
GROUP BY t.padded_codigo_cliente;

/*
2) Resultado base: clientes unicos que compraron en abril 2026
*/
SELECT
    padded_codigo_cliente,
    total_tx,
    monto_total,
    primera_fecha_operacion,
    ultima_fecha_operacion
FROM #compradores_superpack_abril_2026
ORDER BY total_tx DESC, monto_total DESC, padded_codigo_cliente;

/*
3) TOP 200 clientes (opcional)
*/
SELECT TOP (200)
    padded_codigo_cliente,
    total_tx,
    monto_total,
    primera_fecha_operacion,
    ultima_fecha_operacion
FROM #compradores_superpack_abril_2026
ORDER BY total_tx DESC, monto_total DESC, padded_codigo_cliente;

/*
4) Validacion contra lista de clientes
   - Pega aqui tu lista y ejecuta este bloque.
*/
CREATE TABLE #clientes_lista_raw (
    codigo_cliente_raw VARCHAR(50) NOT NULL
);

CREATE TABLE #clientes_lista_norm (
    padded_codigo_cliente VARCHAR(8) NOT NULL
);

-- Ejemplo (borra o reemplaza):
-- INSERT INTO #clientes_lista_raw (codigo_cliente_raw)
-- VALUES
-- ('02783401'),
-- ('00012345');

INSERT INTO #clientes_lista_norm (padded_codigo_cliente)
SELECT DISTINCT
    RIGHT(
        '00000000' + LTRIM(RTRIM(
            CASE
                WHEN r.codigo_cliente_raw IS NULL THEN NULL
                WHEN PATINDEX('%[A-Za-z]%', r.codigo_cliente_raw) > 1
                    THEN LEFT(r.codigo_cliente_raw, PATINDEX('%[A-Za-z]%', r.codigo_cliente_raw) - 1)
                WHEN PATINDEX('%[A-Za-z]%', r.codigo_cliente_raw) = 1 THEN NULL
                ELSE r.codigo_cliente_raw
            END
        )),
        8
    ) AS padded_codigo_cliente
FROM #clientes_lista_raw r
WHERE NULLIF(LTRIM(RTRIM(r.codigo_cliente_raw)), '') IS NOT NULL;

/*
5) Detalle por cliente de la lista
*/
SELECT
    l.padded_codigo_cliente,
    CASE WHEN c.padded_codigo_cliente IS NULL THEN 0 ELSE 1 END AS compro_superpack_abril_2026,
    c.total_tx,
    c.monto_total,
    c.primera_fecha_operacion,
    c.ultima_fecha_operacion
FROM #clientes_lista_norm l
LEFT JOIN #compradores_superpack_abril_2026 c
    ON c.padded_codigo_cliente = l.padded_codigo_cliente
ORDER BY compro_superpack_abril_2026 DESC, c.total_tx DESC, l.padded_codigo_cliente;

/*
6) Resumen de validacion de la lista
*/
SELECT
    COUNT(*) AS total_clientes_lista,
    SUM(CASE WHEN c.padded_codigo_cliente IS NOT NULL THEN 1 ELSE 0 END) AS clientes_que_compraron,
    SUM(CASE WHEN c.padded_codigo_cliente IS NULL THEN 1 ELSE 0 END) AS clientes_que_no_compraron,
    CAST(
        100.0 * SUM(CASE WHEN c.padded_codigo_cliente IS NOT NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5, 2)
    ) AS pct_que_compraron
FROM #clientes_lista_norm l
LEFT JOIN #compradores_superpack_abril_2026 c
    ON c.padded_codigo_cliente = l.padded_codigo_cliente;
