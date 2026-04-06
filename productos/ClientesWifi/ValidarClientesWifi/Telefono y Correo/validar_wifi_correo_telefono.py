"""
validar_wifi_correo_telefono.py
===============================
Valida si contactos (correo/telefono) aparecen en la base de cuentas creadas.

Entradas fijas (misma carpeta del script):
- Correo.xlsx (columna: Email)
- Telefono.xlsx (columna: Telefono)

Fuente de cuentas:
- cuenta_digital_2026.xlsx (primero en esta carpeta, fallback a carpeta padre)

Salidas:
- Coinciden_CuentasCreadas.xlsx
- NoCoinciden_CuentasCreadas.xlsx
"""

from pathlib import Path
import unicodedata
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent

RUTA_CORREO = BASE_DIR / "Correo.xlsx"
RUTA_TELEFONO = BASE_DIR / "Telefono.xlsx"
RUTA_CUENTA_LOCAL = BASE_DIR / "cuenta_digital_2026.xlsx"
RUTA_CUENTA_PARENT = PARENT_DIR / "cuenta_digital_2026.xlsx"

RUTA_SALIDA_COINCIDEN = BASE_DIR / "Coinciden_CuentasCreadas.xlsx"
RUTA_SALIDA_NO_COINCIDEN = BASE_DIR / "NoCoinciden_CuentasCreadas.xlsx"


def quitar_tildes(texto: str) -> str:
    """Convierte texto con tildes a su equivalente sin tildes."""
    if not isinstance(texto, str):
        return ""
    return "".join(
        c
        for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def normalizar_correo(serie: pd.Series) -> pd.Series:
    """Normaliza correos: trim, lower, sin tildes, y con arroba obligatoria."""
    resultado = (
        serie.astype(str)
        .str.strip()
        .str.lower()
        .apply(quitar_tildes)
    )
    return resultado.where(
        resultado.str.contains("@", na=False) & (resultado != "nan"),
        other=pd.NA,
    )


def normalizar_telefono(serie: pd.Series) -> pd.Series:
    """Normaliza telefonos quitando simbolos, prefijo 504 y sufijo .0."""
    return (
        serie.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[\s\-\(\)\+]", "", regex=True)
        .str.replace(r"^504", "", regex=True)
        .str.lower()
        .replace("nan", "")
        .replace("", pd.NA)
    )


def resolver_ruta_cuenta_digital() -> Path:
    """Resuelve la ruta de cuenta_digital_2026.xlsx con fallback a carpeta padre."""
    if RUTA_CUENTA_LOCAL.exists():
        return RUTA_CUENTA_LOCAL
    if RUTA_CUENTA_PARENT.exists():
        return RUTA_CUENTA_PARENT
    raise FileNotFoundError(
        "No se encontro cuenta_digital_2026.xlsx en:\n"
        f"- {RUTA_CUENTA_LOCAL}\n"
        f"- {RUTA_CUENTA_PARENT}"
    )


def validar_archivo_columna(path: Path, columna: str) -> pd.DataFrame:
    """Carga un Excel y valida que exista la columna esperada."""
    if not path.exists():
        raise FileNotFoundError(f"No se encontro el archivo requerido: {path}")

    df = pd.read_excel(path)
    if columna not in df.columns:
        columnas = ", ".join(map(str, df.columns))
        raise ValueError(
            f"El archivo {path.name} debe contener la columna '{columna}'. "
            f"Columnas detectadas: [{columnas}]"
        )
    return df[[columna]].copy()


def construir_base_entrada() -> pd.DataFrame:
    """Construye un dataframe unico a partir de Correo.xlsx y Telefono.xlsx."""
    df_correo = validar_archivo_columna(RUTA_CORREO, "Email")
    df_telefono = validar_archivo_columna(RUTA_TELEFONO, "Telefono")

    max_len = max(len(df_correo), len(df_telefono))
    df_correo = df_correo.reindex(range(max_len))
    df_telefono = df_telefono.reindex(range(max_len))

    df = pd.DataFrame(
        {
            "Email": df_correo["Email"],
            "Telefono": df_telefono["Telefono"],
        }
    )
    return df


def obtener_contactos_cuentas(df_cuentas: pd.DataFrame) -> tuple[set[str], set[str]]:
    """Obtiene sets normalizados de correos y telefonos desde la base de cuentas."""
    if "correo" in df_cuentas.columns:
        col_correo = "correo"
    elif "direccion_3" in df_cuentas.columns:
        col_correo = "direccion_3"
    else:
        raise ValueError(
            "La base de cuentas debe tener columna 'correo' o 'direccion_3' para validar emails."
        )

    if "telefono_1" not in df_cuentas.columns and "telefono_2" not in df_cuentas.columns:
        raise ValueError(
            "La base de cuentas debe tener al menos una columna de telefono: "
            "'telefono_1' o 'telefono_2'."
        )

    df_cuentas["_correo_norm"] = normalizar_correo(df_cuentas[col_correo])
    if "telefono_1" in df_cuentas.columns:
        df_cuentas["_telefono_1_norm"] = normalizar_telefono(df_cuentas["telefono_1"])
    else:
        df_cuentas["_telefono_1_norm"] = pd.NA

    if "telefono_2" in df_cuentas.columns:
        df_cuentas["_telefono_2_norm"] = normalizar_telefono(df_cuentas["telefono_2"])
    else:
        df_cuentas["_telefono_2_norm"] = pd.NA

    set_correos = set(df_cuentas["_correo_norm"].dropna().loc[lambda x: x != ""])
    set_telefonos = set(df_cuentas["_telefono_1_norm"].dropna()) | set(
        df_cuentas["_telefono_2_norm"].dropna()
    )
    set_telefonos.discard("")

    return set_correos, set_telefonos


def marcar_coincidencias(df_entrada: pd.DataFrame, set_correos: set[str], set_telefonos: set[str]) -> pd.DataFrame:
    """Marca coincidencias por Correo O Telefono y clasifica el tipo."""
    df = df_entrada.copy()
    df["_correo_norm"] = normalizar_correo(df["Email"])
    df["_telefono_norm"] = normalizar_telefono(df["Telefono"])

    def tiene_coincidencia(row: pd.Series) -> bool:
        correo = row["_correo_norm"]
        telefono = row["_telefono_norm"]
        correo_match = pd.notna(correo) and correo in set_correos
        telefono_match = pd.notna(telefono) and telefono in set_telefonos
        return correo_match or telefono_match

    def tipo_coincidencia(row: pd.Series) -> str:
        correo = row["_correo_norm"]
        telefono = row["_telefono_norm"]
        correo_match = pd.notna(correo) and correo in set_correos
        telefono_match = pd.notna(telefono) and telefono in set_telefonos
        if correo_match and telefono_match:
            return "Correo y Telefono"
        if correo_match:
            return "Solo Correo"
        if telefono_match:
            return "Solo Telefono"
        return "Sin coincidencia"

    df["coincide"] = df.apply(tiene_coincidencia, axis=1)
    df["tipo_coincidencia"] = df.apply(tipo_coincidencia, axis=1)
    return df


def exportar_resultados(df_resultado: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Exporta dos archivos: coinciden y no coinciden."""
    df_coinciden = df_resultado[df_resultado["coincide"]].copy()
    df_no_coinciden = df_resultado[~df_resultado["coincide"]].copy()

    columnas_salida = [c for c in df_resultado.columns if not c.startswith("_")]
    df_coinciden = df_coinciden[columnas_salida]
    df_no_coinciden = df_no_coinciden[columnas_salida]

    df_coinciden.to_excel(RUTA_SALIDA_COINCIDEN, index=False)
    df_no_coinciden.to_excel(RUTA_SALIDA_NO_COINCIDEN, index=False)
    return df_coinciden, df_no_coinciden


def imprimir_resumen(df_resultado: pd.DataFrame) -> None:
    """Imprime resumen general y desglose por tipo de coincidencia."""
    total = len(df_resultado)
    coinciden = int(df_resultado["coincide"].sum())
    no_coinciden = total - coinciden

    print("\n============================================")
    print("RESULTADOS DE VALIDACION")
    print("============================================")
    print(f"Total evaluado:       {total:,}")
    print(f"Coinciden:            {coinciden:,}")
    print(f"No coinciden:         {no_coinciden:,}")
    if total > 0:
        print(f"Porcentaje coincide:  {(coinciden / total) * 100:,.1f}%")
    else:
        print("Porcentaje coincide:  0.0%")
    print("============================================")
    print("Desglose por tipo:")
    for tipo, cant in df_resultado["tipo_coincidencia"].value_counts().items():
        print(f"- {tipo}: {cant:,}")


def main() -> None:
    """Punto de entrada."""
    try:
        print("Cargando archivos de entrada...")
        df_entrada = construir_base_entrada()

        ruta_cuentas = resolver_ruta_cuenta_digital()
        print(f"Cargando cuentas creadas: {ruta_cuentas}")
        df_cuentas = pd.read_excel(ruta_cuentas, dtype=str)

        set_correos, set_telefonos = obtener_contactos_cuentas(df_cuentas)
        df_resultado = marcar_coincidencias(df_entrada, set_correos, set_telefonos)
        df_coinciden, df_no_coinciden = exportar_resultados(df_resultado)

        imprimir_resumen(df_resultado)
        print(f"\n[OK] Archivo generado: {RUTA_SALIDA_COINCIDEN}")
        print(f"[OK] Archivo generado: {RUTA_SALIDA_NO_COINCIDEN}")
        print(f"[OK] Coinciden: {len(df_coinciden):,}")
        print(f"[OK] No coinciden: {len(df_no_coinciden):,}")

    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
