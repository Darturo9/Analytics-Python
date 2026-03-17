/*
query_clientes_sin_pago_servicios.sql
-------------------------------------
Objetivo:
  Obtener clientes naturales y activos, con banca y al menos un usuario BEL activo,
  que NO hayan realizado pagos de servicios en el rango de fechas indicado.

Notas:
  - "Sin importar canal": no se filtra por canal.
  - Fuentes de pago usadas:
      1) Multipagos (DW_MUL_SPPADAT)
      2) Journal con códigos de pago de servicios (DW_BEL_IBJOUR)
*/

DECLARE @fecha_inicio DATE = '2026-03-01';
DECLARE @fecha_fin    DATE = '2026-03-31';

WITH universo_clientes AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(cif.CLDOC)), 8) AS padded_codigo_cliente,
        LTRIM(RTRIM(cif.CLDOC)) AS codigo_cliente,
        cif.CLTIPE AS tipo_cliente,
        bel.CLSTAT AS estado_cliente_bel,
        ISNULL(cif.dw_usuarios_bel_cnt, 0) AS banca_e,
        (
            SELECT COUNT(1)
            FROM DW_BEL_IBUSER u
            WHERE u.CLCCLI = bel.CLCCLI
              AND u.USSTAT = 'A'
        ) AS usuarios_bel_activos
    FROM DW_CIF_CLIENTES cif
    INNER JOIN DW_BEL_IBCLIE bel
        ON RIGHT('00000000' + LTRIM(RTRIM(cif.CLDOC)), 8) =
           RIGHT('00000000' + LTRIM(RTRIM(bel.CLCCLI)), 8)
    WHERE cif.CLTIPE = 'N'                 -- Clientes naturales
      AND bel.CLSTAT = 'A'                 -- Cliente activo
      AND ISNULL(cif.dw_usuarios_bel_cnt, 0) > 0   -- Tiene banca
      AND EXISTS (
          SELECT 1
          FROM DW_BEL_IBUSER u
          WHERE u.CLCCLI = bel.CLCCLI
            AND u.USSTAT = 'A'             -- Usuario BEL activo
      )
),
pagos_multipagos AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(clientes_bel.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_MUL_SPPADAT mp
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(u.CLCCLI)) AS CLCCLI,
            LTRIM(RTRIM(u.USCODE)) AS USCODE
        FROM DW_BEL_IBUSER u
    ) clientes_bel
        ON LTRIM(RTRIM(mp.SPINUS)) = (clientes_bel.CLCCLI + clientes_bel.USCODE)
    WHERE mp.DW_FECHA_OPERACION_SP >= @fecha_inicio
      AND mp.DW_FECHA_OPERACION_SP < DATEADD(DAY, 1, @fecha_fin)
      AND clientes_bel.CLCCLI IS NOT NULL
),
pagos_journal AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(j.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBJOUR j
    WHERE j.dw_fecha_journal >= @fecha_inicio
      AND j.dw_fecha_journal < DATEADD(DAY, 1, @fecha_fin)
      AND j.JOSTAT = 1
      AND j.JOSECU = 1
      AND j.SECODE IN ('mpg-cpago', 'app-cpago')
),
pagos_servicios AS (
    SELECT padded_codigo_cliente FROM pagos_multipagos
    UNION
    SELECT padded_codigo_cliente FROM pagos_journal
)
SELECT
    uc.padded_codigo_cliente,
    uc.codigo_cliente,
    uc.tipo_cliente,
    uc.estado_cliente_bel,
    uc.banca_e,
    uc.usuarios_bel_activos
FROM universo_clientes uc
LEFT JOIN pagos_servicios ps
    ON uc.padded_codigo_cliente = ps.padded_codigo_cliente
WHERE ps.padded_codigo_cliente IS NULL
ORDER BY uc.padded_codigo_cliente;

