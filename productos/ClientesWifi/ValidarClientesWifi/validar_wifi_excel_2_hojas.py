"""
validar_wifi_excel_2_hojas.py
=============================
Valida un archivo Excel con 2 hojas (correos y telefonos) contra cuentas digitales.

Objetivo:
- Saber cuantos clientes del archivo abrieron cuenta digital.
- Obtener quienes son (listados de coincidencias y no coincidencias).

Configuracion:
- Todo se modifica en las constantes CONFIG_* de este archivo.
- No requiere argumentos en terminal.
"""

from __future__ import annotations

import sys
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


BASE_DIR = Path(__file__).resolve().parent
RUTA_ARCHIVOS_EXCEL = BASE_DIR / "archivosExcel"
RUTA_QUERY_CUENTAS = BASE_DIR / "query_cuenta_digital.sql"
RUTA_EXPORTS = BASE_DIR / "exports"

# ============================================================================
# CONFIGURACION EDITABLE
# ============================================================================
# Nombre base o nombre completo (ej: "BDD_DEPURADA_CLIENTES_WIFI" o "BDD_DEPURADA_CLIENTES_WIFI.xlsx")
CONFIG_NOMBRE_ARCHIVO_ENTRADA = "BDD_DEPURADA_CLIENTES_WIFI"
CONFIG_HOJA_CORREOS = "Hoja1"
CONFIG_HOJA_TELEFONOS = "Hoja2"
CONFIG_COL_CORREO = "Email"
CONFIG_COL_TELEFONO = "Telefono"

# Si True: usa query SQL para cargar cuentas digitales.
# Si False: usa archivo Excel local.
CONFIG_USAR_SQL = True
CONFIG_ARCHIVO_CUENTAS_LOCAL = BASE_DIR / "cuenta_digital_2026.xlsx"

CONFIG_ARCHIVO_SALIDA = "Resultado_Validacion_ClientesWifi_2Hojas.xlsx"
# ============================================================================


def quitar_tildes(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def normalizar_correo(serie: pd.Series) -> pd.Series:
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


def cargar_excel_2_hojas(ruta_excel: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not ruta_excel.exists():
        raise FileNotFoundError(f"No se encontro el archivo de entrada: {ruta_excel}")

    df_correos = pd.read_excel(ruta_excel, sheet_name=CONFIG_HOJA_CORREOS)
    df_telefonos = pd.read_excel(ruta_excel, sheet_name=CONFIG_HOJA_TELEFONOS)

    if CONFIG_COL_CORREO not in df_correos.columns:
        raise ValueError(
            f"La hoja '{CONFIG_HOJA_CORREOS}' no contiene la columna '{CONFIG_COL_CORREO}'. "
            f"Columnas detectadas: {list(df_correos.columns)}"
        )
    if CONFIG_COL_TELEFONO not in df_telefonos.columns:
        raise ValueError(
            f"La hoja '{CONFIG_HOJA_TELEFONOS}' no contiene la columna '{CONFIG_COL_TELEFONO}'. "
            f"Columnas detectadas: {list(df_telefonos.columns)}"
        )

    return df_correos.copy(), df_telefonos.copy()


def resolver_archivo_entrada() -> Path:
    if not RUTA_ARCHIVOS_EXCEL.exists():
        raise FileNotFoundError(f"No existe la carpeta de entrada: {RUTA_ARCHIVOS_EXCEL}")

    candidatos = [
        p for p in RUTA_ARCHIVOS_EXCEL.iterdir()
        if p.is_file() and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
    ]
    if not candidatos:
        raise FileNotFoundError(
            "No hay archivos Excel/CSV en la carpeta de entrada: "
            f"{RUTA_ARCHIVOS_EXCEL}"
        )

    objetivo = CONFIG_NOMBRE_ARCHIVO_ENTRADA.strip().lower()

    exactos = [p for p in candidatos if p.name.lower() == objetivo]
    if exactos:
        return exactos[0]

    exactos_base = [p for p in candidatos if p.stem.lower() == objetivo]
    if exactos_base:
        return exactos_base[0]

    prefijos = [p for p in candidatos if p.stem.lower().startswith(objetivo)]
    if prefijos:
        return sorted(prefijos, key=lambda x: x.name.lower())[0]

    lista = ", ".join(sorted(p.name for p in candidatos))
    raise FileNotFoundError(
        "No se encontro el archivo de entrada segun CONFIG_NOMBRE_ARCHIVO_ENTRADA='"
        f"{CONFIG_NOMBRE_ARCHIVO_ENTRADA}'. "
        f"Archivos detectados: [{lista}]"
    )


def cargar_cuentas_digitales() -> pd.DataFrame:
    if CONFIG_USAR_SQL:
        if not RUTA_QUERY_CUENTAS.exists():
            raise FileNotFoundError(f"No se encontro la query de cuentas: {RUTA_QUERY_CUENTAS}")
        df = run_query_file(str(RUTA_QUERY_CUENTAS))
        df.columns = [str(c) for c in df.columns]
        return df

    if not CONFIG_ARCHIVO_CUENTAS_LOCAL.exists():
        raise FileNotFoundError(
            "No se encontro el archivo de cuentas local en "
            f"{CONFIG_ARCHIVO_CUENTAS_LOCAL}"
        )
    df = pd.read_excel(CONFIG_ARCHIVO_CUENTAS_LOCAL, dtype=str)
    df.columns = [str(c) for c in df.columns]
    return df


def construir_indices_contacto(
    df_cuentas: pd.DataFrame,
) -> tuple[set[str], set[str], dict[str, pd.Timestamp], dict[str, pd.Timestamp]]:
    if df_cuentas.empty:
        return set(), set(), {}, {}

    col_correo = "correo" if "correo" in df_cuentas.columns else "direccion_3"
    if col_correo not in df_cuentas.columns:
        raise ValueError("La base de cuentas debe incluir 'correo' o 'direccion_3'.")

    if "telefono_1" not in df_cuentas.columns and "telefono_2" not in df_cuentas.columns:
        raise ValueError("La base de cuentas debe incluir 'telefono_1' o 'telefono_2'.")

    if "fecha_apertura" not in df_cuentas.columns:
        raise ValueError("La base de cuentas debe incluir 'fecha_apertura'.")

    df = df_cuentas.copy()
    df["_fecha_apertura"] = pd.to_datetime(df["fecha_apertura"], errors="coerce")
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

    mapa_fecha_correo = (
        df[df["_correo_norm"].notna() & (df["_correo_norm"] != "") & df["_fecha_apertura"].notna()]
        .groupby("_correo_norm")["_fecha_apertura"]
        .min()
        .to_dict()
    )

    tel1 = df[["_telefono_1_norm", "_fecha_apertura"]].rename(columns={"_telefono_1_norm": "_telefono_norm"})
    tel2 = df[["_telefono_2_norm", "_fecha_apertura"]].rename(columns={"_telefono_2_norm": "_telefono_norm"})
    df_tels = pd.concat([tel1, tel2], ignore_index=True)

    mapa_fecha_telefono = (
        df_tels[df_tels["_telefono_norm"].notna() & (df_tels["_telefono_norm"] != "") & df_tels["_fecha_apertura"].notna()]
        .groupby("_telefono_norm")["_fecha_apertura"]
        .min()
        .to_dict()
    )

    return set_correos, set_telefonos, mapa_fecha_correo, mapa_fecha_telefono


def evaluar_correos(
    df_correos: pd.DataFrame,
    set_correos: set[str],
    mapa_fecha_correo: dict[str, pd.Timestamp],
) -> pd.DataFrame:
    df = df_correos.copy()
    df["_correo_norm"] = normalizar_correo(df[CONFIG_COL_CORREO])
    df["coincide"] = df["_correo_norm"].apply(lambda x: pd.notna(x) and x in set_correos)
    df["tipo_coincidencia"] = df["coincide"].map({True: "Solo Correo", False: "Sin coincidencia"})
    df["fecha_apertura_cuenta_digital"] = (
        pd.to_datetime(df["_correo_norm"].map(mapa_fecha_correo), errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("")
    )
    return df


def evaluar_telefonos(
    df_telefonos: pd.DataFrame,
    set_telefonos: set[str],
    mapa_fecha_telefono: dict[str, pd.Timestamp],
) -> pd.DataFrame:
    df = df_telefonos.copy()
    df["_telefono_norm"] = normalizar_telefono(df[CONFIG_COL_TELEFONO])
    df["coincide"] = df["_telefono_norm"].apply(lambda x: pd.notna(x) and x in set_telefonos)
    df["tipo_coincidencia"] = df["coincide"].map({True: "Solo Telefono", False: "Sin coincidencia"})
    df["fecha_apertura_cuenta_digital"] = (
        pd.to_datetime(df["_telefono_norm"].map(mapa_fecha_telefono), errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("")
    )
    return df


def resumen_categoria(nombre: str, df: pd.DataFrame) -> dict[str, object]:
    total = len(df)
    coinciden = int(df["coincide"].sum()) if total > 0 else 0
    no_coinciden = total - coinciden
    pct = round((coinciden / total) * 100, 1) if total > 0 else 0.0
    return {
        "categoria": nombre,
        "total": total,
        "coinciden": coinciden,
        "no_coinciden": no_coinciden,
        "porcentaje_coincide": pct,
    }


def exportar_resultados(df_res_correo: pd.DataFrame, df_res_telefono: pd.DataFrame) -> Path:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)
    ruta_salida = RUTA_EXPORTS / CONFIG_ARCHIVO_SALIDA

    cols_correo = [c for c in df_res_correo.columns if not c.startswith("_")]
    cols_telefono = [c for c in df_res_telefono.columns if not c.startswith("_")]

    df_coinciden_correo = df_res_correo[df_res_correo["coincide"]].copy()[cols_correo]
    df_no_coinciden_correo = df_res_correo[~df_res_correo["coincide"]].copy()[cols_correo]

    df_coinciden_telefono = df_res_telefono[df_res_telefono["coincide"]].copy()[cols_telefono]
    df_no_coinciden_telefono = df_res_telefono[~df_res_telefono["coincide"]].copy()[cols_telefono]

    resumen = pd.DataFrame(
        [
            resumen_categoria("Correos", df_res_correo),
            resumen_categoria("Telefonos", df_res_telefono),
        ]
    )

    with pd.ExcelWriter(ruta_salida, engine="xlsxwriter") as writer:
        resumen.to_excel(writer, sheet_name="Resumen", index=False)
        df_coinciden_correo.to_excel(writer, sheet_name="Coinciden_Correos", index=False)
        df_no_coinciden_correo.to_excel(writer, sheet_name="NoCoinciden_Correos", index=False)
        df_coinciden_telefono.to_excel(writer, sheet_name="Coinciden_Telefonos", index=False)
        df_no_coinciden_telefono.to_excel(writer, sheet_name="NoCoinciden_Telefonos", index=False)

    return ruta_salida


def imprimir_resumen(df_res_correo: pd.DataFrame, df_res_telefono: pd.DataFrame, ruta_salida: Path) -> None:
    res_correo = resumen_categoria("Correos", df_res_correo)
    res_telefono = resumen_categoria("Telefonos", df_res_telefono)

    print("\n====================================================")
    print("VALIDACION CLIENTES WIFI - EXCEL 2 HOJAS")
    print("====================================================")
    try:
        ruta_entrada = resolver_archivo_entrada()
    except Exception:
        ruta_entrada = RUTA_ARCHIVOS_EXCEL / CONFIG_NOMBRE_ARCHIVO_ENTRADA
    print(f"Archivo entrada: {ruta_entrada}")
    print(f"Fuente cuentas:  {'SQL query' if CONFIG_USAR_SQL else CONFIG_ARCHIVO_CUENTAS_LOCAL}")
    print("----------------------------------------------------")
    print(
        f"Correos   -> Total: {res_correo['total']:,} | "
        f"Coinciden: {res_correo['coinciden']:,} | "
        f"No coinciden: {res_correo['no_coinciden']:,} | "
        f"%: {res_correo['porcentaje_coincide']}%"
    )
    print(
        f"Telefonos -> Total: {res_telefono['total']:,} | "
        f"Coinciden: {res_telefono['coinciden']:,} | "
        f"No coinciden: {res_telefono['no_coinciden']:,} | "
        f"%: {res_telefono['porcentaje_coincide']}%"
    )
    print("====================================================")
    print(f"[OK] Archivo generado: {ruta_salida}")


def main() -> None:
    try:
        ruta_entrada = resolver_archivo_entrada()
        df_correos, df_telefonos = cargar_excel_2_hojas(ruta_entrada)
        df_cuentas = cargar_cuentas_digitales()

        set_correos, set_telefonos, mapa_fecha_correo, mapa_fecha_telefono = construir_indices_contacto(df_cuentas)
        df_res_correo = evaluar_correos(df_correos, set_correos, mapa_fecha_correo)
        df_res_telefono = evaluar_telefonos(df_telefonos, set_telefonos, mapa_fecha_telefono)

        ruta_salida = exportar_resultados(df_res_correo, df_res_telefono)
        imprimir_resumen(df_res_correo, df_res_telefono, ruta_salida)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo consultar SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
