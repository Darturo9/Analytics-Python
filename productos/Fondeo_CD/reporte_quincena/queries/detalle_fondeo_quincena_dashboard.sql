WITH cuentas_quincena AS (
    SELECT DISTINCT
        d.DW_CUENTA_CORPORATIVA AS numero_cuenta,
        RTRIM(LTRIM(d.cldoc)) AS codigo_cliente,
        CAST(d.dw_feha_apertura AS DATE) AS fecha_apertura
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= :fecha_inicio_quincena
      AND d.dw_feha_apertura <  :fecha_fin_quincena_exclusiva
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
fondeo_periodo AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA AS numero_cuenta,
        MAX(COALESCE(h.ctt001, 0)) AS saldo_max_periodo
    FROM HIS_DEP_DEPOSITOS_VIEW h
    WHERE h.dw_fecha_informacion >= :fecha_inicio_fondeo
      AND h.dw_fecha_informacion <  :fecha_fin_fondeo_exclusiva
    GROUP BY h.DW_CUENTA_CORPORATIVA
),
base AS (
    SELECT
        q.numero_cuenta,
        q.codigo_cliente,
        q.fecha_apertura,
        COALESCE(f.saldo_max_periodo, 0) AS saldo_max_periodo,
        CASE
            WHEN COALESCE(f.saldo_max_periodo, 0) > 0 THEN 1
            ELSE 0
        END AS fondeada,
        COALESCE(NULLIF(LTRIM(RTRIM(cli.clisex)), ''), 'SIN DATO') AS genero,
        COALESCE(NULLIF(LTRIM(RTRIM(dir.dw_nivel_geo1)), ''), 'SIN DATO') AS departamento,
        CAST(cli.DW_FECHA_NACIMIENTO AS DATE) AS fecha_nac,
        ROW_NUMBER() OVER (
            PARTITION BY q.numero_cuenta
            ORDER BY
                q.codigo_cliente,
                dir.dw_nivel_geo1,
                dir.dw_nivel_geo2,
                dir.dw_nivel_geo3
        ) AS rn
    FROM cuentas_quincena q
    LEFT JOIN fondeo_periodo f
        ON q.numero_cuenta = f.numero_cuenta
    LEFT JOIN dw_cif_clientes cli
        ON q.codigo_cliente = RTRIM(LTRIM(cli.cldoc))
    LEFT JOIN dw_cif_direcciones_principal dir
        ON q.codigo_cliente = RTRIM(LTRIM(dir.cldoc))
)
SELECT
    numero_cuenta,
    codigo_cliente,
    fecha_apertura,
    saldo_max_periodo,
    fondeada,
    genero,
    departamento,
    fecha_nac
FROM base
WHERE rn = 1;
