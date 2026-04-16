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
from zipfile import BadZipFile

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
    return buscar_archivos_clientes_wifi_abril()[0]


def buscar_archivos_clientes_wifi_abril() -> list[Path]:
    preferidos = [
        "Clientes Wifi Abril.xlsx",
        "Clientes Wifi Abril.xls",
        "Clientes Wifi Abril.csv",
        "Clientes_Wifi_Abril.xlsx",
        "Clientes_Wifi_Abril.xls",
        "Clientes_Wifi_Abril.csv",
        "clientes_wifi_abril.xlsx",
        "clientes_wifi_abril.xls",
        "clientes_wifi_abril.csv",
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

    candidatos: list[Path] = []
    for nombre in preferidos:
        ruta = RUTA_ARCHIVOS_EXCEL / nombre
        if ruta.exists():
            candidatos.append(ruta)

    if RUTA_ARCHIVOS_EXCEL.exists():
        encontrados = [
            p for p in RUTA_ARCHIVOS_EXCEL.iterdir()
            if p.is_file()
            and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
            and "cliente" in p.stem.lower()
            and "wifi" in p.stem.lower()
            and "abril" in p.stem.lower()
        ]
        prioridad_ext = {".xlsx": 0, ".csv": 1, ".xls": 2}
        encontrados.sort(key=lambda p: (prioridad_ext.get(p.suffix.lower(), 99), p.name.lower()))
        candidatos.extend(encontrados)

    # Deduplicar manteniendo orden
    vistos: set[str] = set()
    unicos: list[Path] = []
    for c in candidatos:
        key = str(c.resolve())
        if key not in vistos:
            vistos.add(key)
            unicos.append(c)

    if unicos:
        return unicos

    raise FileNotFoundError(
        "No se encontro archivo de clientes Wifi de abril en:\n"
        f"{RUTA_ARCHIVOS_EXCEL}\n"
        "Esperado: nombre similar a 'Clientes Wifi para abril.xlsx'."
    )


def _es_xls_binario(ruta: Path) -> bool:
    # Firma OLE2 (xls clásico)
    firma_xls = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"
    try:
        with open(ruta, "rb") as f:
            head = f.read(8)
        return head.startswith(firma_xls)
    except Exception:
        return False


def _leer_csv_robusto(ruta: Path) -> pd.DataFrame:
    errores: list[str] = []
    for encoding in ["utf-8-sig", "latin-1"]:
        try:
            return pd.read_csv(ruta, sep=None, engine="python", encoding=encoding)
        except Exception as exc:
            errores.append(f"{encoding}: {exc}")
    raise RuntimeError("No se pudo leer como CSV. " + " | ".join(errores))


def _leer_excel_robusto(ruta: Path) -> pd.DataFrame:
    errores: list[str] = []
    ext = ruta.suffix.lower()
    es_xls_bin = _es_xls_binario(ruta)

    intentos: list[tuple[str, dict]] = []
    if ext == ".xlsx":
        intentos.append(("openpyxl", {"engine": "openpyxl"}))
        if es_xls_bin:
            intentos.append(("xlrd", {"engine": "xlrd"}))
    elif ext == ".xls":
        intentos.append(("xlrd", {"engine": "xlrd"}))
        intentos.append(("openpyxl", {"engine": "openpyxl"}))
    else:
        intentos.append(("openpyxl", {"engine": "openpyxl"}))
        intentos.append(("xlrd", {"engine": "xlrd"}))

    # fallback auto de pandas
    intentos.append(("auto", {}))

    for etiqueta, kwargs in intentos:
        try:
            return pd.read_excel(ruta, **kwargs)
        except ImportError as exc:
            errores.append(f"{etiqueta}: libreria faltante ({exc})")
        except BadZipFile:
            errores.append(f"{etiqueta}: archivo no es zip valido para xlsx")
        except ValueError as exc:
            errores.append(f"{etiqueta}: {exc}")
        except Exception as exc:
            errores.append(f"{etiqueta}: {exc}")

    raise RuntimeError("No se pudo leer como Excel. " + " | ".join(errores))


def leer_archivo_clientes_robusto(ruta: Path) -> pd.DataFrame:
    ext = ruta.suffix.lower()
    errores: list[str] = []

    if ext in {".xlsx", ".xls"}:
        try:
            return _leer_excel_robusto(ruta)
        except Exception as exc_excel:
            errores.append(str(exc_excel))
            # fallback por si el archivo excel realmente es texto/csv mal renombrado
            try:
                return _leer_csv_robusto(ruta)
            except Exception as exc_csv:
                errores.append(str(exc_csv))
    elif ext == ".csv":
        try:
            return _leer_csv_robusto(ruta)
        except Exception as exc_csv:
            errores.append(str(exc_csv))
    else:
        raise RuntimeError(f"Extension no soportada: {ext}")

    detalle = " | ".join(errores)
    if ext in {".xlsx", ".xls"}:
        raise RuntimeError(
            "No se pudo abrir el archivo Excel. "
            "Si el archivo es .xls, instala xlrd (pip install xlrd>=2.0.1) "
            "o guardalo como .xlsx. "
            f"Detalle: {detalle}"
        )
    raise RuntimeError(f"No se pudo abrir el archivo. Detalle: {detalle}")


def detectar_columna(df: pd.DataFrame, objetivo_normalizado: str) -> str | None:
    mapa = {col: normalizar_nombre_columna(col) for col in df.columns}
    for original, normalizada in mapa.items():
        if normalizada == objetivo_normalizado:
            return original
    return None


def cargar_clientes_wifi() -> tuple[pd.DataFrame, Path, str, str]:
    candidatos = buscar_archivos_clientes_wifi_abril()
    errores: list[str] = []

    for ruta_excel in candidatos:
        try:
            df = leer_archivo_clientes_robusto(ruta_excel)
        except Exception as exc:
            errores.append(f"{ruta_excel.name}: {exc}")
            continue

        col_correo = detectar_columna(df, "correo")
        col_telefono = detectar_columna(df, "telefono_formateado")

        if col_correo is None or col_telefono is None:
            errores.append(
                f"{ruta_excel.name}: faltan columnas requeridas "
                f"(detectadas: {list(df.columns)})"
            )
            continue

        return df.copy(), ruta_excel, col_correo, col_telefono

    raise RuntimeError(
        "No se pudo cargar ningun archivo valido de clientes Wifi para abril.\n"
        "Revisa formato y columnas requeridas ('Correo', 'Telefono Formateado').\n"
        "Detalle intentos:\n- " + "\n- ".join(errores)
    )


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


def imprimir_resumen(
    df_resultado: pd.DataFrame,
    ruta_excel: Path,
    col_correo: str,
    col_telefono: str,
) -> None:
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

    print("\nLISTADO DE CLIENTES QUE COINCIDEN")
    print("-" * 52)
    df_coinciden = df_resultado[df_resultado["coincide"]].copy()
    if df_coinciden.empty:
        print("No hay clientes con coincidencia.")
        print("=" * 52)
        return

    listado = df_coinciden[[col_correo, col_telefono, "tipo_coincidencia"]].copy()
    listado = listado.rename(
        columns={
            col_correo: "Correo",
            col_telefono: "Telefono Formateado",
            "tipo_coincidencia": "Coincidencia",
        }
    )
    listado["Correo"] = listado["Correo"].astype(str).replace("nan", "")
    listado["Telefono Formateado"] = (
        listado["Telefono Formateado"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .replace("nan", "")
    )

    print(listado.to_string(index=False))
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
    imprimir_resumen(
        df_resultado=df_resultado,
        ruta_excel=ruta_excel,
        col_correo=col_correo,
        col_telefono=col_telefono,
    )

    print(f"[OK] Archivo generado: {ruta_coinciden}")
    print(f"[OK] Archivo generado: {ruta_no_coinciden}")
    print(f"[OK] Archivo generado: {ruta_resumen}")


if __name__ == "__main__":
    main()
