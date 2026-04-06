"""
validar_wifi_correo_telefono.py
===============================
Valida contactos por separado (correo y telefono) contra cuentas creadas.

Entradas fijas (misma carpeta del script):
- Correo.xlsx (columna: Email)
- Telefono.xlsx (columna: Telefono)

Fuente de cuentas:
- cuenta_digital_2026.xlsx (primero en esta carpeta, fallback a carpeta padre)

Salidas:
- Coinciden_Correo.xlsx
- NoCoinciden_Correo.xlsx
- Coinciden_Telefono.xlsx
- NoCoinciden_Telefono.xlsx
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

RUTA_SALIDA_COINCIDEN_CORREO = BASE_DIR / "Coinciden_Correo.xlsx"
RUTA_SALIDA_NO_COINCIDEN_CORREO = BASE_DIR / "NoCoinciden_Correo.xlsx"
RUTA_SALIDA_COINCIDEN_TELEFONO = BASE_DIR / "Coinciden_Telefono.xlsx"
RUTA_SALIDA_NO_COINCIDEN_TELEFONO = BASE_DIR / "NoCoinciden_Telefono.xlsx"


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


def evaluar_correos(df_correo: pd.DataFrame, set_correos: set[str]) -> pd.DataFrame:
    """Valida solamente correos, sin relacionarlo con telefonos."""
    df = df_correo.copy()
    df["_correo_norm"] = normalizar_correo(df["Email"])
    df["coincide"] = df["_correo_norm"].apply(lambda x: pd.notna(x) and x in set_correos)
    df["tipo_coincidencia"] = df["coincide"].map(
        {True: "Solo Correo", False: "Sin coincidencia"}
    )
    return df


def evaluar_telefonos(df_telefono: pd.DataFrame, set_telefonos: set[str]) -> pd.DataFrame:
    """Valida solamente telefonos, sin relacionarlo con correos."""
    df = df_telefono.copy()
    df["_telefono_norm"] = normalizar_telefono(df["Telefono"])
    df["coincide"] = df["_telefono_norm"].apply(
        lambda x: pd.notna(x) and x in set_telefonos
    )
    df["tipo_coincidencia"] = df["coincide"].map(
        {True: "Solo Telefono", False: "Sin coincidencia"}
    )
    return df


def exportar_corte(
    df_resultado: pd.DataFrame, ruta_coinciden: Path, ruta_no_coinciden: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Exporta coincidencias y no coincidencias para un solo tipo de validacion."""
    df_coinciden = df_resultado[df_resultado["coincide"]].copy()
    df_no_coinciden = df_resultado[~df_resultado["coincide"]].copy()

    columnas_salida = [c for c in df_resultado.columns if not c.startswith("_")]
    df_coinciden = df_coinciden[columnas_salida]
    df_no_coinciden = df_no_coinciden[columnas_salida]

    df_coinciden.to_excel(ruta_coinciden, index=False)
    df_no_coinciden.to_excel(ruta_no_coinciden, index=False)
    return df_coinciden, df_no_coinciden


def imprimir_resumen(nombre: str, df_resultado: pd.DataFrame) -> None:
    """Imprime resumen para una validacion especifica."""
    total = len(df_resultado)
    coinciden = int(df_resultado["coincide"].sum())
    no_coinciden = total - coinciden

    print("\n============================================")
    print(f"RESULTADOS DE VALIDACION - {nombre}")
    print("============================================")
    print(f"Total evaluado:       {total:,}")
    print(f"Coinciden:            {coinciden:,}")
    print(f"No coinciden:         {no_coinciden:,}")
    if total > 0:
        print(f"Porcentaje coincide:  {(coinciden / total) * 100:,.1f}%")
    else:
        print("Porcentaje coincide:  0.0%")


def main() -> None:
    """Punto de entrada."""
    try:
        print("Cargando archivos de entrada...")
        df_correo = validar_archivo_columna(RUTA_CORREO, "Email")
        df_telefono = validar_archivo_columna(RUTA_TELEFONO, "Telefono")

        ruta_cuentas = resolver_ruta_cuenta_digital()
        print(f"Cargando cuentas creadas: {ruta_cuentas}")
        df_cuentas = pd.read_excel(ruta_cuentas, dtype=str)

        set_correos, set_telefonos = obtener_contactos_cuentas(df_cuentas)
        df_resultado_correo = evaluar_correos(df_correo, set_correos)
        df_resultado_telefono = evaluar_telefonos(df_telefono, set_telefonos)

        df_coinciden_correo, df_no_coinciden_correo = exportar_corte(
            df_resultado_correo,
            RUTA_SALIDA_COINCIDEN_CORREO,
            RUTA_SALIDA_NO_COINCIDEN_CORREO,
        )
        df_coinciden_telefono, df_no_coinciden_telefono = exportar_corte(
            df_resultado_telefono,
            RUTA_SALIDA_COINCIDEN_TELEFONO,
            RUTA_SALIDA_NO_COINCIDEN_TELEFONO,
        )

        imprimir_resumen("Correo", df_resultado_correo)
        imprimir_resumen("Telefono", df_resultado_telefono)

        print(f"\n[OK] Archivo generado: {RUTA_SALIDA_COINCIDEN_CORREO}")
        print(f"[OK] Archivo generado: {RUTA_SALIDA_NO_COINCIDEN_CORREO}")
        print(f"[OK] Archivo generado: {RUTA_SALIDA_COINCIDEN_TELEFONO}")
        print(f"[OK] Archivo generado: {RUTA_SALIDA_NO_COINCIDEN_TELEFONO}")
        print(
            "[OK] Correo - Coinciden/No coinciden: "
            f"{len(df_coinciden_correo):,}/{len(df_no_coinciden_correo):,}"
        )
        print(
            "[OK] Telefono - Coinciden/No coinciden: "
            f"{len(df_coinciden_telefono):,}/{len(df_no_coinciden_telefono):,}"
        )

    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
