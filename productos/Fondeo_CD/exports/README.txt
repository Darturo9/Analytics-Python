Esta carpeta almacena los archivos Excel generados por:

- Analysis/export_fondeo_cd.py

Archivo de salida por defecto:
- BBDD_FONDEO_CD_YYYYMMDD_YYYYMMDD.xlsx

Hojas:
- KPIs
- ResumenCuentas
- HistoricoDiario

Regla principal:
- Universo = cuentas Cuenta Digital abiertas en el periodo.
- Se considera fondeo si ctt001 > 0 o dw_saldo_promedio > 0 en al menos un dia del periodo.
