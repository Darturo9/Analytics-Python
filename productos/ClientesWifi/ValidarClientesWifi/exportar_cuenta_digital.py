"""
exportar_cuenta_digital.py
==========================
Ejecuta la query de cuentas digitales abiertas desde 2026
y exporta el resultado a Excel con correo y teléfonos.
"""

import sys
import os

# --- Apuntar al módulo de conexión en sql_project ---
sys.path.append(r"C:\Users\72404\Documents\sql_project")

from consultas_sql import ejecutar_consulta, exportar_excel

# ---------------------------------------------------------------------------
# Consulta
# ---------------------------------------------------------------------------
QUERY = """
SELECT
	c_digitales.*
FROM (
	SELECT
		depositos.dw_cuenta_corporativa					AS numero_cuenta,
		clientes_bel.CLEJEV							    AS Ejecutivo_de_Cuentas,
		usuarios_bel.USCODE							    AS Usuario_Banca,
	    usuarios_bel.dw_fecha_creacion				    AS Fecha_Creacion_Usuario,
        clientes.dw_fecha_alta                          AS fecha_ingreso_banco,
		DATEDIFF(DAY, CAST(clientes.dw_fecha_alta AS DATE), depositos.dw_feha_apertura) AS dif,
		clientes.DW_CLASE_CLIENTE_DESCRIPCION,
        clientes.DW_ESTADO_CIVIL_DESCRIPCION            AS estado_civil,
		agencia.clagno,
		CAST(depositos.dw_feha_apertura AS DATE)		AS fecha_apertura,
        COALESCE(clientes.dw_sms_cnt, 0)                AS bp_movil, 
		CASE
			WHEN DAY(depositos.dw_feha_apertura) <= 7	THEN 1
			WHEN DAY(depositos.dw_feha_apertura) <= 14	THEN 2
			WHEN DAY(depositos.dw_feha_apertura) <= 21	THEN 3
			ELSE 4
		END												AS semana_apertura,
		CASE
			WHEN MONTH(depositos.dw_feha_apertura) <= 3 	THEN 'Q1'
			WHEN MONTH(depositos.dw_feha_apertura) <= 6	THEN 'Q2'
			WHEN MONTH(depositos.dw_feha_apertura) <= 9	THEN 'Q3'
			WHEN MONTH(depositos.dw_feha_apertura) <= 12	THEN 'Q4'
		END												AS trimestre,
		depositos.dw_estado_cuenta						AS estado_cuenta,
		depositos.dw_aplicacion							AS aplicacion,
		depositos.dw_producto							AS producto,
		depositos.dpagen								AS codigo_agencia,
		depositos.dw_agencia_cta						AS nombre_agencia,
		depositos.dw_moneda								AS moneda,
		COALESCE(depositos.ctt001, 0)					AS saldo_cuenta,
		COALESCE(depositos.ctctrx, 0)					AS cant_transacciones,
		RTRIM(LTRIM(depositos.cldoc))					AS codigo_cliente,
        RIGHT('00000000' + RTRIM(LTRIM(depositos.cldoc)),8)	AS padded_codigo_cliente,
		CAST(clientes.dw_fecha_alta AS DATE)			AS fecha_ingreso,
		clientes.cltipe									AS tipo_cliente,
		clientes.dw_clase_cliente_descripcion			AS descripcion_cliente,
		clientes.clclco									AS codigo_descripcion_cliente,
        clientes.clisex                                 AS genero,
        clientes.DW_FECHA_NACIMIENTO                    AS fecha_nac,
		CASE
			WHEN clientes.clclco = 10 THEN 1
			ELSE 0
		END												AS empleado_banpais,
		direcciones.dw_nivel_geo1						AS direccion_lvl_1,
		direcciones.dw_nivel_geo2						AS direccion_lvl_2,
		direcciones.dw_nivel_geo3						AS direccion_lvl_3,
		direcciones.dw_nivel_geo1						AS direccion_1,
		direcciones.dw_nivel_geo2						AS direccion_2,
		direcciones.dw_nivel_geo3						AS direccion_3,
		depositos.dw_ultima_transaccion                 AS ultima_transaccion,
		direcciones.cltel1								AS telefono_1,
		direcciones.cltel2								AS telefono_2,
		LOWER(RTRIM(LTRIM(correos.cldire)))				AS correo,
		ROW_NUMBER() OVER(PARTITION BY depositos.dw_cuenta_corporativa
				ORDER BY clientes.cldoc, 
				direcciones.dw_nivel_geo1, direcciones.dw_nivel_geo2, direcciones.dw_nivel_geo3) AS rn
	FROM
		dw_dep_depositos depositos
	LEFT JOIN
		dw_cif_clientes clientes
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes.cldoc))
	LEFT JOIN
		dw_cif_direcciones_principal direcciones
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(direcciones.cldoc))
	LEFT JOIN
		dw_cif_direcciones correos
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(correos.cldoc))
		AND correos.cldico = 4
		AND correos.cldire LIKE '%_@_%.__%'
	LEFT JOIN
		dw_bel_ibclie clientes_bel
		ON RTRIM(LTRIM(depositos.cldoc)) = RTRIM(LTRIM(clientes_bel.clccli)) 
	LEFT JOIN
		dw_bel_ibuser usuarios_bel
		ON (clientes_bel.CLCCLI = usuarios_bel.CLCCLI
		AND usuarios_bel.dw_fecha_creacion = depositos.dw_feha_apertura)
	LEFT JOIN
		tr_cif_clagen agencia
		ON agencia.clagcd = clientes_bel.clucra
		AND agencia.empcod = 1
		AND agencia.clagno LIKE '%servicios electronicos%'
	WHERE
		depositos.dw_feha_apertura >= '2026-02-01'
		AND depositos.dw_feha_apertura < '2026-04-05'
		AND depositos.dw_producto = 'CUENTA DIGITAL'
) AS c_digitales
WHERE
	c_digitales.rn = 1
"""

# ---------------------------------------------------------------------------
# Ruta de salida del Excel
# ---------------------------------------------------------------------------
RUTA_SALIDA = r"C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\cuenta_digital_2026.xlsx"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("🔄 Ejecutando consulta de cuentas digitales 2026...")

    df = ejecutar_consulta(QUERY)

    print(f"✅ Registros obtenidos: {len(df)}")

    # Forzar como texto las columnas de código y teléfonos
    for col in ["codigo_cliente", "padded_codigo_cliente", "telefono_1", "telefono_2"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "")

    exportar_excel(
        df,
        ruta_completa=RUTA_SALIDA,
        columnas_texto=["codigo_cliente", "padded_codigo_cliente", "telefono_1", "telefono_2"]
    )

    print(f"📁 Archivo guardado en: {RUTA_SALIDA}")
