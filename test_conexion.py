"""
test_conexion.py
----------------
Script para verificar que la conexión a SQL Server funciona correctamente.
Ejecutar desde la raíz del proyecto:

    python test_conexion.py
"""

from core.db import run_query

print("Conectando a SQL Server...")

df = run_query("SELECT  top 10 CLDOC FROM DW_CIF_Clientes")

print(df.to_string())
print(f"\n[OK] {len(df)} filas, {len(df.columns)} columnas")
