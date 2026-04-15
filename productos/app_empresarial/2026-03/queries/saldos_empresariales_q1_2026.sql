/*
Resumen:
- Universo empresarial: clientes con actividad en canal AP/APP desde 2025-12-01,
  tipo cliente juridico (J) y con indicador BancaE = 1.
- Saldo Q1: del 2026-01-01 al 2026-03-31 usando HIS_DEP_DEPOSITOS_VIEW.
*/

WITH clientes_empresariales AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(t.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBSTTRA_VIEW t
    INNER JOIN DW_CIF_CLIENTES c
        ON LTRIM(RTRIM(t.CLCCLI)) = LTRIM(RTRIM(c.CLDOC))
    WHERE
        t.DW_FECHA_TRX >= '2025-12-01'
        AND (
            t.CACODE IN ('AP', 'APP')
            OR t.SECODE = 'app-login'
        )
        AND c.CLTIPE = 'J'
        AND c.dw_usuarios_bel_cnt = 1
),
saldo_q1 AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(h.CLDOC)), 8) AS padded_codigo_cliente,
        CAST(h.DW_FECHA_INFORMACION AS DATE) AS fecha_informacion,
        CAST(COALESCE(h.CTT001, 0) AS DECIMAL(18, 2)) AS saldo_cierre_dia,
        CAST(COALESCE(h.DW_SALDO_PROMEDIO, 0) AS DECIMAL(18, 2)) AS saldo_promedio_dia
    FROM HIS_DEP_DEPOSITOS_VIEW h
    WHERE
        h.PRCODP = 1
        AND h.PRSUBP = 51
        AND h.DW_PRODUCTO = 'CUENTA DIGITAL'
        AND h.DW_FECHA_INFORMACION >= '2026-01-01'
        AND h.DW_FECHA_INFORMACION < '2026-04-01'
),
saldo_por_cliente AS (
    SELECT
        s.padded_codigo_cliente,
        AVG(s.saldo_cierre_dia) AS saldo_promedio_q1,
        AVG(s.saldo_promedio_dia) AS saldo_promedio_contable_q1,
        MAX(s.saldo_cierre_dia) AS saldo_maximo_q1,
        SUM(s.saldo_cierre_dia) AS saldo_acumulado_dias_q1,
        SUM(
            CASE
                WHEN s.fecha_informacion = '2026-03-31' THEN s.saldo_cierre_dia
                ELSE 0
            END
        ) AS saldo_cierre_31_mar,
        COUNT(*) AS dias_observados_q1,
        SUM(CASE WHEN s.saldo_cierre_dia > 0 THEN 1 ELSE 0 END) AS dias_con_saldo_q1
    FROM saldo_q1 s
    INNER JOIN clientes_empresariales ce
        ON ce.padded_codigo_cliente = s.padded_codigo_cliente
    GROUP BY
        s.padded_codigo_cliente
),
universo AS (
    SELECT COUNT(*) AS clientes_empresariales_universo
    FROM clientes_empresariales
)
SELECT
    u.clientes_empresariales_universo,
    COUNT(spc.padded_codigo_cliente) AS clientes_empresariales_con_saldo_q1,
    u.clientes_empresariales_universo - COUNT(spc.padded_codigo_cliente) AS clientes_sin_saldo_q1,
    CAST(
        CASE
            WHEN u.clientes_empresariales_universo = 0 THEN 0
            ELSE (COUNT(spc.padded_codigo_cliente) * 100.0 / u.clientes_empresariales_universo)
        END
        AS DECIMAL(10, 2)
    ) AS pct_clientes_con_saldo_q1,
    SUM(spc.saldo_cierre_31_mar) AS saldo_total_cierre_31_mar,
    AVG(spc.saldo_cierre_31_mar) AS saldo_promedio_cierre_31_mar_por_cliente,
    AVG(spc.saldo_promedio_q1) AS promedio_q1_saldo_cierre_por_cliente,
    AVG(spc.saldo_promedio_contable_q1) AS promedio_q1_saldo_promedio_contable_por_cliente,
    AVG(spc.saldo_maximo_q1) AS promedio_q1_saldo_maximo_por_cliente,
    SUM(spc.saldo_acumulado_dias_q1) AS saldo_acumulado_q1_todos_los_dias,
    SUM(spc.dias_con_saldo_q1) AS total_dias_con_saldo_en_q1
FROM universo u
LEFT JOIN saldo_por_cliente spc
    ON 1 = 1
GROUP BY
    u.clientes_empresariales_universo;
