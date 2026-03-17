import pandas as pd

df1 = pd.read_excel(r'C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\baselimpiaPotenciales.xlsx', nrows=2)
print("=== Potenciales ===")
for i, col in enumerate(df1.columns):
    print(f"  [{i}] '{col}'")

df2 = pd.read_excel(r'C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\cuenta_digital_2026.xlsx', nrows=2)
print("\n=== Cuenta Digital ===")
for i, col in enumerate(df2.columns):
    print(f"  [{i}] '{col}'")
