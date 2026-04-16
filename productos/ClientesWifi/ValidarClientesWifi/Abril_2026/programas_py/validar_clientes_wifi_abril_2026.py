"""
validar_clientes_wifi_abril_2026.py
===================================
Valida clientes Wifi (archivo de abril) contra cuentas digitales creadas en abril 2026.
Coincidencias evaluadas por correo y/o telefono.

Entradas:
- ValidarClientesWifi/archivosExcel/Clientes Wifi para abril.* (o nombre similar)
- query SQL: Abril_2026/queries/query_cuenta_digital_abril_2026.sql

Salidas:
- Abril_2026/exports/Coinciden_ClientesWifi_Abril2026.xlsx
- Abril_2026/exports/NoCoinciden_ClientesWifi_Abril2026.xlsx
- Abril_2026/exports/Resumen_ClientesWifi_Abril2026.xlsx
"""

from __future__ import annotations

import sys
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


BASE_DIR = Path(__file__).resolve().parent
ABRIL_DIR = BASE_DIR.parent
VALIDAR_DIR = ABRIL_DIR.parent

RUTA_ARCHIVOS_EXCEL = VALIDAR_DIR / "archivosExcel"
RUTA_QUERY = ABRIL_DIR / "queries" / "query_cuenta_digital_abril_2026.sql"
RUTA_EXPORTS = ABRIL_DIR / "exports"

RUTA_SALIDA_COINCIDEN = RUTA_EXPORTS / "Coinciden_ClientesWifi_Abril2026.xlsx"
RUTA_SALIDA_NO_COINCIDEN = RUTA_EXPORTS / "NoCoinciden_ClientesWifi_Abril2026.xlsx"
RUTA_SALIDA_RESUMEN = RUTA_EXPORTS / "Resumen_ClientesWifi_Abril2026.xlsx"


def quitar_tildes(texto: str) -> str:
    """Convierte texto con tildes a su equivalente sin tildes."""
    if not isinstance(texto, str):
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def normalizar_nombre_columna(nombre: str) -> str:
    return (
        str(nombre)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace(" ", "_")
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


def buscar_archivo_clientes_wifi_abril() -> Path:
    preferidos = [
        "Clientes Wifi para abril.xlsx",
        "Clientes Wifi para abril.xls",
        "Clientes Wifi para abril.csv",
        "Clientes_Wifi_para_abril.xlsx",
        "Clientes_Wifi_para_abril.xls",
        "Clientes_Wifi_para_abril.csv",
        "clientes_wifi_para_abril.xlsx",
        "clientes_wifi_para_abril.xls",
        "clientes_wifi_para_abril.csv",
        "ClientesWifiAbril.xlsx",
        "ClientesWifiAbril.xls",
        "ClientesWifiAbril.csv",
    ]

    for nombre in preferidos:
        ruta = RUTA_ARCHIVOS_EXCEL / nombre
        if ruta.exists():
            return ruta

    if RUTA_ARCHIVOS_EXCEL.exists():
        candidatos = [
            p for p in RUTA_ARCHIVOS_EXCEL.iterdir()
            if p.is_file()
            and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
            and "cliente" in p.stem.lower()
            and "wifi" in p.stem.lower()
            and "abril" in p.stem.lower()
        ]
        if candidatos:
            prioridad_ext = {".xlsx": 0, ".csv": 1, ".xls": 2}
            candidatos.sort(key=lambda p: (prioridad_ext.get(p.suffix.lower(), 99), p.name.lower()))
            return candidatos[0]

    raise FileNotFoundError(
        "No se encontro archivo de clientes Wifi de abril en:\n"
        f"{RUTA_ARCHIVOS_EXCEL}\n"
        "Esperado: nombre similar a 'Clientes Wifi para abril.xlsx'."
    )


def detectar_columna(df: pd.DataFrame, objetivo_normalizado: str) -> str | None:
    mapa = {col: normalizar_nombre_columna(col) for col in df.columns}
    for original, normalizada in mapa.items():
        if normalizada == objetivo_normalizado:
            return original
    return None


def cargar_clientes_wifi() -> tuple[pd.DataFrame, Path, str, str]:
    ruta_excel = buscar_archivo_clientes_wifi_abril()
    try:
        if ruta_excel.suffix.lower() == ".xlsx":
            df = pd.read_excel(ruta_excel, engine="openpyxl")
        elif ruta_excel.suffix.lower() == ".xls":
            df = pd.read_excel(ruta_excel, engine="xlrd")
        else:
            df = pd.read_csv(ruta_excel)
    except ImportError as exc:
        if ruta_excel.suffix.lower() == ".xls":
            raise RuntimeError(
                "El archivo detectado es .xls y falta la libreria xlrd.\n"
                "Opciones:\n"
                "1) Instalar xlrd: pip install xlrd>=2.0.1\n"
                "2) Guardar el archivo como .xlsx en la misma carpeta (recomendado)."
            ) from exc
        raise

    col_correo = detectar_columna(df, "correo")
    col_telefono = detectar_columna(df, "telefono_formateado")

    if col_correo is None or col_telefono is None:
        raise ValueError(
            "El archivo debe contener columnas 'Correo' y 'Telefono Formateado'. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    return df.copy(), ruta_excel, col_correo, col_telefono


def cargar_cuentas_digitales_abril() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    if df.empty:
        return df

    df.columns = [str(c) for c in df.columns]
    return df


def obtener_sets_contacto_cuentas(df_cuentas: pd.DataFrame) -> tuple[set[str], set[str]]:
    if df_cuentas.empty:
        return set(), set()

    col_correo = None
    for candidato in ["correo", "direccion_3"]:
        if candidato in df_cuentas.columns:
            col_correo = candidato
            break
    if col_correo is None:
        raise ValueError("La query debe devolver columna 'correo' o 'direccion_3'.")

    if "telefono_1" not in df_cuentas.columns and "telefono_2" not in df_cuentas.columns:
        raise ValueError("La query debe devolver al menos 'telefono_1' o 'telefono_2'.")

    df = df_cuentas.copy()
    df["_correo_norm"] = normalizar_correo(df[col_correo])

    if "telefono_1" in df.columns:
        df["_telefono_1_norm"] = normalizar_telefono(df["telefono_1"])
    else:
        df["_telefono_1_norm"] = pd.NA

    if "telefono_2" in df.columns:
        df["_telefono_2_norm"] = normalizar_telefono(df["telefono_2"])
    else:
        df["_telefono_2_norm"] = pd.NA

    set_correos = set(df["_correo_norm"].dropna().loc[lambda x: x != ""])
    set_telefonos = set(df["_telefono_1_norm"].dropna()) | set(df["_telefono_2_norm"].dropna())
    set_telefonos.discard("")
    return set_correos, set_telefonos


def evaluar_coincidencias(
    df_clientes: pd.DataFrame,
    col_correo: str,
    col_telefono: str,
    set_correos: set[str],
    set_telefonos: set[str],
) -> pd.DataFrame:
    df = df_clientes.copy()
    df["_correo_norm"] = normalizar_correo(df[col_correo])
    df["_telefono_norm"] = normalizar_telefono(df[col_telefono])

    df["_correo_match"] = df["_correo_norm"].apply(lambda x: pd.notna(x) and x in set_correos)
    df["_telefono_match"] = df["_telefono_norm"].apply(lambda x: pd.notna(x) and x in set_telefonos)

    df["coincide"] = df["_correo_match"] | df["_telefono_match"]
    df["tipo_coincidencia"] = "Sin coincidencia"
    df.loc[df["_correo_match"] & df["_telefono_match"], "tipo_coincidencia"] = "Correo y Telefono"
    df.loc[df["_correo_match"] & ~df["_telefono_match"], "tipo_coincidencia"] = "Solo Correo"
    df.loc[~df["_correo_match"] & df["_telefono_match"], "tipo_coincidencia"] = "Solo Telefono"
    return df


def exportar_resultados(df_resultado: pd.DataFrame) -> tuple[Path, Path, Path]:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)

    columnas_salida = [c for c in df_resultado.columns if not c.startswith("_")]
    df_coinciden = df_resultado[df_resultado["coincide"]].copy()[columnas_salida]
    df_no_coinciden = df_resultado[~df_resultado["coincide"]].copy()[columnas_salida]

    df_coinciden.to_excel(RUTA_SALIDA_COINCIDEN, index=False)
    df_no_coinciden.to_excel(RUTA_SALIDA_NO_COINCIDEN, index=False)

    total = len(df_resultado)
    orden_tipos = ["Correo y Telefono", "Solo Correo", "Solo Telefono", "Sin coincidencia"]
    resumen = (
        df_resultado["tipo_coincidencia"]
        .value_counts()
        .reindex(orden_tipos, fill_value=0)
        .rename_axis("tipo_coincidencia")
        .reset_index(name="cantidad")
    )
    resumen["porcentaje"] = resumen["cantidad"].apply(
        lambda x: round((x / total) * 100, 1) if total > 0 else 0.0
    )
    resumen.to_excel(RUTA_SALIDA_RESUMEN, index=False)

    return RUTA_SALIDA_COINCIDEN, RUTA_SALIDA_NO_COINCIDEN, RUTA_SALIDA_RESUMEN


def imprimir_resumen(df_resultado: pd.DataFrame, ruta_excel: Path) -> None:
    total = len(df_resultado)
    coinciden = int(df_resultado["coincide"].sum())
    no_coinciden = total - coinciden

    print("=" * 52)
    print("RESULTADOS VALIDACION CLIENTES WIFI - ABRIL 2026")
    print("=" * 52)
    print(f"Archivo fuente:              {ruta_excel}")
    print(f"Total registros evaluados:   {total:,}")
    print(f"Coinciden (correo/telefono): {coinciden:,}")
    print(f"No coinciden:                {no_coinciden:,}")
    if total > 0:
        print(f"Porcentaje coincide:         {(coinciden / total) * 100:,.1f}%")
    else:
        print("Porcentaje coincide:         0.0%")
    print("-" * 52)
    print("Desglose por tipo:")
    for tipo, cantidad in df_resultado["tipo_coincidencia"].value_counts().items():
        print(f"  {tipo:<22} {cantidad:>8,}")
    print("=" * 52)


def main() -> None:
    print(f"Cargando archivo clientes Wifi desde: {RUTA_ARCHIVOS_EXCEL}")
    print(f"Ejecutando query de cuentas abril 2026: {RUTA_QUERY}")

    try:
        df_clientes, ruta_excel, col_correo, col_telefono = cargar_clientes_wifi()
        df_cuentas = cargar_cuentas_digitales_abril()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc

    print(f"Clientes Wifi cargados: {len(df_clientes):,}")
    print(f"Cuentas digitales abril 2026 cargadas: {len(df_cuentas):,}")

    try:
        set_correos, set_telefonos = obtener_sets_contacto_cuentas(df_cuentas)
    except Exception as exc:
        print(f"[ERROR] No se pudieron construir sets de contacto desde cuentas: {exc}")
        raise SystemExit(1) from exc

    df_resultado = evaluar_coincidencias(
        df_clientes=df_clientes,
        col_correo=col_correo,
        col_telefono=col_telefono,
        set_correos=set_correos,
        set_telefonos=set_telefonos,
    )

    ruta_coinciden, ruta_no_coinciden, ruta_resumen = exportar_resultados(df_resultado)
    imprimir_resumen(df_resultado, ruta_excel)

    print(f"[OK] Archivo generado: {ruta_coinciden}")
    print(f"[OK] Archivo generado: {ruta_no_coinciden}")
    print(f"[OK] Archivo generado: {ruta_resumen}")


if __name__ == "__main__":
    main()
