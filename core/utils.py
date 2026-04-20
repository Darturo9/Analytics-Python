"""
utils.py
--------
Funciones utilitarias reutilizables en todos los productos.

Funciones disponibles:
    exportar_excel(df, path, hoja)         → Exporta un DataFrame a Excel
    exportar_excel_multi(sheets, path)     → Exporta múltiples hojas a un Excel
    exportar_csv(df, path)                 → Exporta un DataFrame a CSV
    formatear_cif(df, columna)             → Normaliza códigos CIF a 8 dígitos con ceros

Uso:
    from core.utils import exportar_excel, formatear_cif
"""

import pandas as pd
from pathlib import Path


def exportar_excel(df: pd.DataFrame, path: str, hoja: str = "Datos") -> None:
    """
    Exporta un DataFrame a un archivo Excel.

    Crea la carpeta destino si no existe.
    Preserva los ceros a la izquierda guardando como texto.

    Args:
        df    : DataFrame a exportar.
        path  : Ruta del archivo de salida (ej: "productos/app_empresarial/2026-03/exports/reporte.xlsx")
        hoja  : Nombre de la hoja. Por defecto "Datos".

    Ejemplo:
        exportar_excel(df, "productos/cuenta_digital/2026-03/exports/clientes.xlsx")
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=hoja, index=False)
        workbook  = writer.book
        worksheet = writer.sheets[hoja]
        fmt_text  = workbook.add_format({"num_format": "@"})  # formato texto
        for i, col in enumerate(df.columns):
            # Usar iloc por posicion evita problemas con columnas duplicadas.
            serie = df.iloc[:, i].astype(str)
            ancho = max(serie.map(len).max(), len(str(col))) + 2
            worksheet.set_column(i, i, ancho, fmt_text)
    print(f"[OK] Excel exportado: {path}")


def exportar_excel_multi(sheets: dict, path: str) -> None:
    """
    Exporta múltiples DataFrames a un Excel con una hoja por cada uno.

    Args:
        sheets : Diccionario {nombre_hoja: DataFrame}.
        path   : Ruta del archivo de salida.

    Ejemplo:
        exportar_excel_multi({"Enero": df1, "Febrero": df2}, "exports/resumen.xlsx")
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        for nombre, df in sheets.items():
            df.to_excel(writer, sheet_name=nombre, index=False)
            worksheet = writer.sheets[nombre]
            for i, col in enumerate(df.columns):
                # Usar iloc por posicion evita problemas con columnas duplicadas.
                serie = df.iloc[:, i].astype(str)
                ancho = max(serie.map(len).max(), len(str(col))) + 2
                worksheet.set_column(i, i, ancho)
    print(f"[OK] Excel multi-hoja exportado: {path}")


def exportar_csv(df: pd.DataFrame, path: str) -> None:
    """
    Exporta un DataFrame a CSV UTF-8.

    Args:
        df   : DataFrame a exportar.
        path : Ruta del archivo de salida.

    Ejemplo:
        exportar_csv(df, "productos/cuenta_digital/2026-03/exports/clientes.csv")
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV exportado: {path}")


def formatear_cif(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    """
    Normaliza una columna de códigos CIF a 8 dígitos con ceros a la izquierda.

    Args:
        df      : DataFrame que contiene la columna.
        columna : Nombre de la columna a formatear.

    Returns:
        DataFrame con la columna formateada.

    Ejemplo:
        df = formatear_cif(df, "CIF_CLIENTE")
    """
    df[columna] = df[columna].astype(str).str.strip().str.zfill(8)
    return df
