"""
validar_wifi.py
===============
Compara los clientes potenciales contra la base de cuenta digital 2026.
Busca coincidencias por correo y/o teléfono.
Imprime resultados en consola y exporta coincidencias a Excel.
"""

import unicodedata
import pandas as pd

# ---------------------------------------------------------------------------
# Rutas de archivos
# ---------------------------------------------------------------------------
RUTA_POTENCIALES    = r"C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\baselimpiaPotenciales.xlsx"
RUTA_CUENTA_DIGITAL = r"C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\cuenta_digital_2026.xlsx"
RUTA_SALIDA         = r"C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\coincidencias_cuenta_digital.xlsx"
RUTA_DESCARTADOS    = r"C:\Users\72404\Documents\Danilo\Carpeta Python\ValidarClientesWifi\descartando_convertidos.xlsx"

# ---------------------------------------------------------------------------
# Función para quitar tildes y caracteres especiales
# ---------------------------------------------------------------------------
def quitar_tildes(texto: str) -> str:
    """Convierte caracteres con tilde a su equivalente sin tilde."""
    if not isinstance(texto, str):
        return ""
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

# ---------------------------------------------------------------------------
# Función para normalizar correos
# ---------------------------------------------------------------------------
def normalizar_correo(serie: pd.Series) -> pd.Series:
    """Convierte a minúsculas, quita espacios y tildes."""
    resultado = (
        serie.astype(str)
        .str.strip()
        .str.lower()
        .apply(quitar_tildes)
    )
    # Reemplazar "nan", cadenas vacías y valores sin @ por NaN real
    resultado = resultado.where(
        resultado.str.contains("@", na=False) & (resultado != "nan"),
        other=pd.NA
    )
    return resultado

# ---------------------------------------------------------------------------
# Función para normalizar teléfonos
# ---------------------------------------------------------------------------
def normalizar_telefono(serie: pd.Series) -> pd.Series:
    """Quita espacios, guiones, paréntesis, decimales .0 y convierte a string limpio."""
    return (
        serie.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)           # quita el .0 de floats
        .str.replace(r"[\s\-\(\)\+]", "", regex=True)  # quita espacios, -, (, ), +
        .str.replace(r"^504", "", regex=True)           # quita prefijo Honduras 504
        .str.lower()
        .replace("nan", "")
        .replace("", pd.NA)
    )

# ---------------------------------------------------------------------------
# Cargar archivos
# ---------------------------------------------------------------------------
print("📂 Cargando archivos...")

df_potenciales    = pd.read_excel(RUTA_POTENCIALES)
df_cuenta_digital = pd.read_excel(RUTA_CUENTA_DIGITAL, dtype=str)

print(f"   ✅ Potenciales cargados:     {len(df_potenciales):,} registros")
print(f"   ✅ Cuenta digital cargados:  {len(df_cuenta_digital):,} registros")

# ---------------------------------------------------------------------------
# Normalizar columnas
# ---------------------------------------------------------------------------
print("\n🔧 Normalizando datos...")

# Potenciales
df_potenciales["_correo_norm"]   = normalizar_correo(df_potenciales["Correo"])
df_potenciales["_telefono_norm"] = normalizar_telefono(df_potenciales["Telefono Formateado"])

# Cuenta digital
df_cuenta_digital["_correo_norm"]     = normalizar_correo(df_cuenta_digital["direccion_3"])
df_cuenta_digital["_telefono_1_norm"] = normalizar_telefono(df_cuenta_digital["telefono_1"])
df_cuenta_digital["_telefono_2_norm"] = normalizar_telefono(df_cuenta_digital["telefono_2"])

# ---------------------------------------------------------------------------
# Crear sets para búsqueda rápida
# ---------------------------------------------------------------------------
set_correos    = set(df_cuenta_digital["_correo_norm"].dropna().loc[lambda x: x != ""])
set_telefonos  = set(df_cuenta_digital["_telefono_1_norm"].dropna()) | set(df_cuenta_digital["_telefono_2_norm"].dropna())
set_telefonos.discard("")

# ---------------------------------------------------------------------------
# Comparar
# ---------------------------------------------------------------------------
print("\n🔍 Comparando...")

def tiene_coincidencia(row):
    correo   = row["_correo_norm"]
    telefono = row["_telefono_norm"]
    correo_match   = pd.notna(correo)   and correo   in set_correos
    telefono_match = pd.notna(telefono) and telefono in set_telefonos
    return correo_match or telefono_match

def tipo_coincidencia(row):
    correo   = row["_correo_norm"]
    telefono = row["_telefono_norm"]
    correo_match   = pd.notna(correo)   and correo   in set_correos
    telefono_match = pd.notna(telefono) and telefono in set_telefonos
    if correo_match and telefono_match:
        return "Correo y Teléfono"
    elif correo_match:
        return "Solo Correo"
    elif telefono_match:
        return "Solo Teléfono"
    return "Sin coincidencia"

df_potenciales["coincide"]        = df_potenciales.apply(tiene_coincidencia, axis=1)
df_potenciales["tipo_coincidencia"] = df_potenciales.apply(tipo_coincidencia, axis=1)

# ---------------------------------------------------------------------------
# Resultados en consola
# ---------------------------------------------------------------------------
total         = len(df_potenciales)
coinciden     = df_potenciales["coincide"].sum()
no_coinciden  = total - coinciden

print("\n" + "=" * 45)
print("       RESULTADOS DE COMPARACIÓN")
print("=" * 45)
print(f"  Total potenciales:         {total:>8,}")
print(f"  Con cuenta digital (2026): {coinciden:>8,}")
print(f"  Sin cuenta digital:        {no_coinciden:>8,}")
print(f"  Porcentaje con cuenta:     {(coinciden/total*100):>7.1f}%")
print("=" * 45)
print("\n  Desglose por tipo de coincidencia:")
print("-" * 45)
for tipo, cant in df_potenciales["tipo_coincidencia"].value_counts().items():
    print(f"  {tipo:<25} {cant:>8,}")
print("=" * 45)

# ---------------------------------------------------------------------------
# Detalle de coincidencias
# ---------------------------------------------------------------------------
df_coinciden = df_potenciales[df_potenciales["coincide"] == True].copy()

print(f"\n📋 Detalle de las {len(df_coinciden)} coincidencias:")
print("-" * 65)
print(f"  {'Correo potencial':<35} {'Teléfono potencial':<20} {'Tipo'}")
print("-" * 65)
for _, row in df_coinciden.iterrows():
    correo   = row["Correo"] if str(row["Correo"]) != "nan" else "(sin correo)"
    telefono = row["Telefono Formateado"] if str(row["Telefono Formateado"]) != "nan" else "(sin teléfono)"
    tipo     = row["tipo_coincidencia"]
    print(f"  {str(correo):<35} {str(telefono):<20} {tipo}")
print("-" * 65)

# ---------------------------------------------------------------------------
# Exportar coincidencias a Excel
# ---------------------------------------------------------------------------
print("\n💾 Exportando coincidencias a Excel...")

# Limpiar columnas internas antes de exportar
cols_exportar = [c for c in df_coinciden.columns if not c.startswith("_")]
df_export = df_coinciden[cols_exportar].copy()

# Limpiar el .0 de teléfonos para el Excel también
df_export["Telefono Formateado"] = (
    df_export["Telefono Formateado"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .replace("nan", "")
)

df_export.to_excel(RUTA_SALIDA, index=False)
print(f"✅ Archivo guardado en: {RUTA_SALIDA}")

# ---------------------------------------------------------------------------
# Exportar clientes SIN coincidencia (descartando convertidos)
# ---------------------------------------------------------------------------
print("\n💾 Exportando clientes sin cuenta digital...")

df_no_coinciden = df_potenciales[df_potenciales["coincide"] == False].copy()
cols_exportar   = [c for c in df_no_coinciden.columns if not c.startswith("_")]
df_no_coinciden = df_no_coinciden[cols_exportar].copy()

df_no_coinciden["Telefono Formateado"] = (
    df_no_coinciden["Telefono Formateado"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .replace("nan", "")
)

df_no_coinciden.to_excel(RUTA_DESCARTADOS, index=False)

print(f"✅ Archivo guardado en: {RUTA_DESCARTADOS}")
print(f"   📊 Clientes descartados (convertidos):  {len(df_coinciden):>8,}")
print(f"   📊 Clientes restantes (sin cuenta):     {len(df_no_coinciden):>8,}")
