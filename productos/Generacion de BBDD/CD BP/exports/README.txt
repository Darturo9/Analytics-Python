Esta carpeta almacena los archivos Excel generados por:

- export_cd_bp.py

Archivos de salida por defecto:
- BBDD_CD_BP_COMPLETA.xlsx
- BBDD_CD_BP_LIMITADA.xlsx

Regla de la base limitada:
- Maximo 4,000 clientes por segmentacion_generacional.
- Muestreo aleatorio reproducible con semilla 42 (cambiable con --seed).
- Se excluye la categoria "OTRA GENERACION".

Ejemplo:
- python3 "productos/Generacion de BBDD/CD BP/export_cd_bp.py"
