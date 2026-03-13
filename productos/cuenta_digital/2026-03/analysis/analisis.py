from core.db import run_query_file
from core.utils import exportar_excel

df = run_query_file("productos/cuenta_digital/2026-03/queries/analisis.sql")

print(df.to_string())
print(f"\n{len(df)} filas, {len(df.columns)} columnas")

# exportar_excel(df, "productos/cuenta_digital/2026-03/exports/analisis.xlsx")
