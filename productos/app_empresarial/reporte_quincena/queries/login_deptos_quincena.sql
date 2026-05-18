/*
Login App Empresarial por rango parametrizable con depto nivel 2.

Parametros esperados:
- fecha_inicio (YYYY-MM-DD, inclusiva)
- fecha_fin_exclusiva (YYYY-MM-DD, exclusiva)
*/

WITH login_base AS (
    SELECT
        l.dw_fecha_trx AS fecha_login,
        RIGHT('00000000' + LTRIM(RTRIM(l.CLCCLI)), 8) AS padded_codigo_cliente
    FROM dw_bel_IBSTTRA_VIEW l
    WHERE l.dw_fecha_trx >= :fecha_inicio
      AND l.dw_fecha_trx < :fecha_fin_exclusiva
      AND l.SECODE IN ('login', 'web-login', 'app-login')
),
clientes_periodo AS (
    SELECT DISTINCT
        lb.padded_codigo_cliente
    FROM login_base lb
),
clientes_empresariales AS (
    SELECT
        x.padded_codigo_cliente,
        x.cldoc
    FROM (
        SELECT
            RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente,
            LTRIM(RTRIM(c.CLDOC)) AS cldoc,
            c.CLTIPE AS tipo_cliente,
            c.dw_usuarios_bel_cnt AS banca_e,
            ROW_NUMBER() OVER (
                PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
                ORDER BY c.dw_fecha_informacion DESC
            ) AS rn
        FROM DW_CIF_CLIENTES c
        INNER JOIN clientes_periodo cp
            ON cp.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
    ) x
    WHERE x.rn = 1
      AND x.tipo_cliente = 'J'
      AND x.banca_e = 1
),
direccion_cliente AS (
    SELECT
        y.padded_codigo_cliente,
        COALESCE(NULLIF(LTRIM(RTRIM(y.depto)), ''), 'SIN DATO') AS depto
    FROM (
        SELECT
            ce.padded_codigo_cliente,
            d.DW_NIVEL_GEO2 AS depto,
            ROW_NUMBER() OVER (
                PARTITION BY ce.padded_codigo_cliente
                ORDER BY d.dw_fecha DESC
            ) AS rn
        FROM clientes_empresariales ce
        LEFT JOIN DW_CIF_DIRECCIONES d
            ON ce.cldoc = d.CLDOC
           AND d.CLDICO = 1
    ) y
    WHERE y.rn = 1
),
login_final AS (
    SELECT
        lb.padded_codigo_cliente,
        COALESCE(dc.depto, 'SIN DATO') AS depto
    FROM login_base lb
    INNER JOIN clientes_empresariales ce
        ON ce.padded_codigo_cliente = lb.padded_codigo_cliente
    LEFT JOIN direccion_cliente dc
        ON dc.padded_codigo_cliente = lb.padded_codigo_cliente
),
agg_depto AS (
    SELECT
        lf.depto,
        COUNT(*) AS total_logins_depto,
        COUNT(DISTINCT lf.padded_codigo_cliente) AS clientes_unicos_depto
    FROM login_final lf
    GROUP BY lf.depto
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY ad.clientes_unicos_depto DESC, ad.total_logins_depto DESC, ad.depto
    ) AS ranking_depto,
    ad.depto,
    ad.clientes_unicos_depto,
    ad.total_logins_depto,
    SUM(ad.total_logins_depto) OVER () AS total_logins_global,
    SUM(ad.clientes_unicos_depto) OVER () AS clientes_unicos_global
FROM agg_depto ad
ORDER BY ranking_depto;
