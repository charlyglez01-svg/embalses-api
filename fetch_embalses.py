"""
fetch_embalses.py
-----------------
Descarga el fichero ZIP del MITECO con los datos de embalses,
lo parsea y guarda los datos en embalses.db (SQLite).

Fuente oficial:
  https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico.html

El MITECO actualiza el fichero BD-Embalses_1988-XXXX.zip cada martes.
"""

import os
import io
import zipfile
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
MITECO_URL = (
    "https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/"
    "boletin-hidrologico.html"
)
# URL directa al ZIP (puede cambiar de año; el script la detecta automáticamente)
ZIP_DIRECT_URL = (
    "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/"
    "evaluacion-de-los-recursos-hidricos/BD-Embalses_1988-2022.zip"
)

DB_PATH = os.path.join(os.path.dirname(__file__), "embalses.db")
LOG_PATH = os.path.join(os.path.dirname(__file__), "fetch.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; EmbalsesScraper/1.0; "
        "+https://github.com/tu-usuario/embalses)"
    )
}


# ─────────────────────────────────────────────
# 1. Detectar URL del ZIP desde la página del MITECO
# ─────────────────────────────────────────────
def detectar_url_zip() -> str:
    """Intenta encontrar la URL del ZIP en la página oficial del MITECO."""
    try:
        log.info("Buscando URL del ZIP en la página del MITECO...")
        r = requests.get(MITECO_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "BD-Embalses" in href and href.endswith(".zip"):
                url = href if href.startswith("http") else "https://www.miteco.gob.es" + href
                log.info(f"URL del ZIP detectada: {url}")
                return url
    except Exception as e:
        log.warning(f"No se pudo detectar la URL automáticamente: {e}")
    log.info(f"Usando URL por defecto: {ZIP_DIRECT_URL}")
    return ZIP_DIRECT_URL


# ─────────────────────────────────────────────
# 2. Descargar el ZIP
# ─────────────────────────────────────────────
def descargar_zip(url: str) -> bytes:
    log.info(f"Descargando ZIP desde: {url}")
    r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    data = b""
    for chunk in r.iter_content(chunk_size=65536):
        data += chunk
    log.info(f"ZIP descargado: {len(data)/1024/1024:.1f} MB")
    return data


# ─────────────────────────────────────────────
# 3. Extraer fichero del ZIP y parsear
# ─────────────────────────────────────────────
def parsear_excel(zip_bytes: bytes) -> pd.DataFrame:
    log.info("Extrayendo fichero del ZIP...")
    import tempfile

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        nombres = zf.namelist()
        log.info(f"Ficheros en el ZIP: {nombres}")

        # ── Caso 1: Excel ──────────────────────────────────────────────────
        excel_files = [n for n in nombres if n.lower().endswith((".xlsx", ".xls"))]
        if excel_files:
            excel_name = excel_files[0]
            log.info(f"Leyendo Excel: {excel_name}")
            with zf.open(excel_name) as f:
                df = pd.read_excel(f, sheet_name=0, engine="openpyxl")
            log.info(f"DataFrame cargado: {len(df)} filas, columnas: {list(df.columns)}")
            return df

        # ── Caso 2: MDB (Access) ────────────────────────────────────────────
        mdb_files = [n for n in nombres if n.lower().endswith(".mdb")]
        if mdb_files:
            mdb_name = mdb_files[0]
            log.info(f"Encontrado fichero MDB: {mdb_name} — usando pyodbc")

            # Extraer el .mdb a un fichero temporal
            with tempfile.NamedTemporaryFile(suffix=".mdb", delete=False) as tmp:
                tmp.write(zf.read(mdb_name))
                tmp_path = tmp.name
            log.info(f"MDB extraído en: {tmp_path}")

            return parsear_mdb(tmp_path)

        raise ValueError(
            f"No se encontró Excel ni MDB en el ZIP. Ficheros: {nombres}"
        )


def parsear_mdb(mdb_path: str) -> pd.DataFrame:
    """Lee un fichero .mdb de Access usando pyodbc (solo Windows)."""
    try:
        import pyodbc
    except ImportError:
        raise ImportError(
            "pyodbc no está instalado. Ejecuta: pip install pyodbc"
        )

    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_path};"
    )
    try:
        conn = pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        raise RuntimeError(
            f"No se pudo conectar al MDB. Asegúrate de tener instalado "
            f"el 'Microsoft Access Database Engine'.\n"
            f"Descárgalo gratis en: https://www.microsoft.com/en-us/download/details.aspx?id=54920\n"
            f"Error original: {e}"
        )

    # Ver tablas disponibles
    cursor = conn.cursor()
    tablas = [row.table_name for row in cursor.tables(tableType="TABLE")]
    log.info(f"Tablas en el MDB: {tablas}")

    # Buscar la tabla principal de embalses
    tabla_embalses = None
    for candidata in tablas:
        nombre_lower = candidata.lower()
        if "embalse" in nombre_lower or "presa" in nombre_lower or "pantano" in nombre_lower:
            tabla_embalses = candidata
            break

    if not tabla_embalses:
        # Si no encontramos por nombre, cogemos la tabla más grande
        tabla_embalses = tablas[0]
        log.warning(f"No se encontró tabla de embalses por nombre. Usando: {tabla_embalses}")

    log.info(f"Leyendo tabla: {tabla_embalses}")
    df = pd.read_sql(f"SELECT * FROM [{tabla_embalses}]", conn)
    conn.close()

    # Limpiar fichero temporal
    try:
        os.unlink(mdb_path)
    except Exception:
        pass

    log.info(f"DataFrame cargado: {len(df)} filas, columnas: {list(df.columns)}")
    return df


# ─────────────────────────────────────────────
# 4. Normalizar columnas
# ─────────────────────────────────────────────
COLUMN_MAP = {
    # Nombres que suele usar el MITECO → nombre normalizado
    "EMBALSE":         "nombre",
    "NOMBRE_EMBALSE":  "nombre",
    "NOMBRE":          "nombre",
    "CUENCA":          "cuenca",
    "AMBITO":          "cuenca",
    "COMUNIDAD":       "comunidad",
    "CAPACIDAD":       "capacidad_hm3",
    "CAPACIDAD_TOTAL": "capacidad_hm3",
    "CAP_TOTAL":       "capacidad_hm3",
    "VOLUMEN":         "volumen_hm3",
    "AGUA_TOTAL":      "volumen_hm3",
    "ELEC_CAPACIDAD":  "energia_mwh",
    "FECHA":           "fecha",
    "ANO":             "anio",
    "AÑO":             "anio",
    "SEMANA":          "semana",
    "PORCENTAJE":      "porcentaje",
    "PORC":            "porcentaje",
}


def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    # Pasar columnas a mayúsculas para mapear sin problema de case
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns}, inplace=True)

    # Calcular porcentaje si no existe
    if "porcentaje" not in df.columns and "volumen_hm3" in df.columns and "capacidad_hm3" in df.columns:
        df["porcentaje"] = (df["volumen_hm3"] / df["capacidad_hm3"] * 100).round(2)

    # Fecha como string ISO si existe
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

    log.info(f"Columnas normalizadas: {list(df.columns)}")
    return df


# ─────────────────────────────────────────────
# 5. Guardar en SQLite
# ─────────────────────────────────────────────
def guardar_db(df: pd.DataFrame, db_path: str = DB_PATH):
    log.info(f"Guardando en base de datos: {db_path}")
    con = sqlite3.connect(db_path)
    # Guardamos la tabla completa (histórico) reemplazando la anterior
    df.to_sql("embalses", con, if_exists="replace", index=False)

    # Vista con los datos más recientes por embalse
    con.execute("DROP VIEW IF EXISTS embalses_ultimo")
    if "fecha" in df.columns and "nombre" in df.columns:
        con.execute("""
            CREATE VIEW embalses_ultimo AS
            SELECT e.*
            FROM embalses e
            INNER JOIN (
                SELECT nombre, MAX(fecha) AS max_fecha
                FROM embalses
                GROUP BY nombre
            ) latest ON e.nombre = latest.nombre AND e.fecha = latest.max_fecha
        """)

    # Tabla de metadatos
    con.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            clave TEXT PRIMARY KEY,
            valor TEXT
        )
    """)
    con.execute(
        "INSERT OR REPLACE INTO meta VALUES (?, ?)",
        ("ultima_actualizacion", datetime.now().isoformat())
    )
    con.execute(
        "INSERT OR REPLACE INTO meta VALUES (?, ?)",
        ("total_registros", str(len(df)))
    )
    con.commit()
    con.close()
    log.info(f"✅ Base de datos actualizada con {len(df)} registros.")


# ─────────────────────────────────────────────
# 6. Función principal
# ─────────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("Iniciando descarga de datos de embalses (MITECO)")
    log.info("=" * 50)

    try:
        url = detectar_url_zip()
        zip_bytes = descargar_zip(url)
        df = parsear_excel(zip_bytes)
        df = normalizar(df)
        guardar_db(df)
        log.info("✅ Proceso completado con éxito.")
    except Exception as e:
        log.error(f"❌ Error durante el proceso: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
