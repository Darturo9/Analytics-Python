Esta carpeta almacena los archivos Excel generados por:

- export_fondeo_acumulado.py

Salida por defecto:
- BBDD_CD_BP_FONDEO_ACUMULADO_YYYYMMDD_YYYYMMDD.xlsx

Columnas principales:
- fecha_informacion
- cuentas_creadas_periodo
- cuentas_reportadas_dia
- cuentas_con_fondos_dia
- cuentas_acumuladas_con_fondos

Regla de fondeo:
- Universo: solo cuentas de Cuenta Digital abiertas dentro del rango evaluado.
- Una cuenta se considera con fondos en el dia si ctt001 > 0 o dw_saldo_promedio > 0.
- El acumulado mensual cuenta las cuentas que tuvieron fondos al menos una vez hasta ese dia.

Ejemplos:
- python3 "productos/Generacion de BBDD/CD BP/Fondeo Acumulado/export_fondeo_acumulado.py"
- python3 "productos/Generacion de BBDD/CD BP/Fondeo Acumulado/export_fondeo_acumulado.py" --fecha-inicio 2026-03-01 --fecha-fin 2026-03-31
