/*
==============================================================================
Autor: Equipo Analytics
Fecha creación / Modificación: 2026-03-26
Descripción:
  Base de clientes de "Base clientes.sql" + datos de contacto (correo/celular).
  Reglas:
    - Universo base exacto de Base clientes.sql
    - 1 fila por cliente (ROW_NUMBER)
    - Prioridad celular: SMS válido > cltel1 válido > cltel2 válido
    - Correo obligatorio (validación tipo ejemploDatosContacto.sql)
==============================================================================
*/

WITH base_clientes AS (
    SELECT
        RTRIM(LTRIM(c.CLDOC)) AS codigo_cliente,
        RIGHT('00000000' + RTRIM(LTRIM(c.CLDOC)), 8) AS codigo_cliente_padded,
        RTRIM(LTRIM(c.CLNOMB)) AS nombre_cliente
    FROM DW_CIF_CLIENTES c
    WHERE
        c.dw_usuarios_bel_cnt > 0
        AND c.ESTATU IN ('A')
        AND c.DW_FECHA_NACIMIENTO <= DATEADD(YEAR, -18, GETDATE())
        AND c.CLTIPE IN ('N')
),
contactos_candidatos AS (
    SELECT
        base.codigo_cliente,
        base.nombre_cliente,
        CASE
            WHEN COALESCE(LEN(sms.telefono_sms), 0) = 8
                 AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3', '8', '9')
                THEN RTRIM(LTRIM(sms.telefono_sms))
            WHEN COALESCE(LEN(tel.cltel1), 0) = 8
                 AND SUBSTRING(tel.cltel1, 1, 1) IN ('3', '8', '9')
                THEN RTRIM(LTRIM(tel.cltel1))
            WHEN COALESCE(LEN(tel.cltel2), 0) = 8
                 AND SUBSTRING(tel.cltel2, 1, 1) IN ('3', '8', '9')
                THEN RTRIM(LTRIM(tel.cltel2))
        END AS celular,
        LOWER(RTRIM(LTRIM(cor.cldire))) AS correo,
        CASE
            WHEN COALESCE(LEN(sms.telefono_sms), 0) = 8
                 AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3', '8', '9')
                THEN 1
            WHEN COALESCE(LEN(tel.cltel1), 0) = 8
                 AND SUBSTRING(tel.cltel1, 1, 1) IN ('3', '8', '9')
                THEN 2
            WHEN COALESCE(LEN(tel.cltel2), 0) = 8
                 AND SUBSTRING(tel.cltel2, 1, 1) IN ('3', '8', '9')
                THEN 3
            ELSE 4
        END AS prioridad_celular,
        CASE
            WHEN
                COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), '') LIKE '%@%.com'
                AND LEN(COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), ''))
                    - LEN(REPLACE(COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), ''), '@', '')) = 1
                AND PATINDEX('%[^a-zA-Z0-9@._-]%', COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), '')) = 0
                AND COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), '') NOT LIKE '%www.%'
                AND CHARINDEX(' ', COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), '')) = 0
                AND CHARINDEX('@', COALESCE(LOWER(RTRIM(LTRIM(cor.cldire))), '')) > 1
            THEN 1
            ELSE 0
        END AS correo_valido
    FROM base_clientes base
    LEFT JOIN dwhbi.dbo.dw_sms_perfil_usuario sms
        ON RTRIM(LTRIM(base.codigo_cliente_padded)) = RTRIM(LTRIM(sms.cif))
        AND sms.dw_descripcion_status = 'ACTIVO'
        AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H')
    LEFT JOIN dw_cif_direcciones_principal tel
        ON RTRIM(LTRIM(base.codigo_cliente)) = RTRIM(LTRIM(tel.cldoc))
    LEFT JOIN dw_cif_direcciones cor
        ON RTRIM(LTRIM(base.codigo_cliente)) = RTRIM(LTRIM(cor.cldoc))
        AND cor.cldico = 4
),
contactos_deduplicados AS (
    SELECT
        candidatos.codigo_cliente,
        candidatos.nombre_cliente,
        candidatos.correo,
        candidatos.celular,
        candidatos.correo_valido,
        ROW_NUMBER() OVER (
            PARTITION BY candidatos.codigo_cliente
            ORDER BY
                candidatos.correo_valido DESC,
                candidatos.prioridad_celular ASC,
                candidatos.correo ASC
        ) AS rn
    FROM contactos_candidatos candidatos
)
SELECT
    contactos.codigo_cliente,
    contactos.nombre_cliente,
    contactos.correo,
    contactos.celular
FROM contactos_deduplicados contactos
WHERE
    contactos.rn = 1
    AND contactos.correo_valido = 1
ORDER BY contactos.codigo_cliente;
