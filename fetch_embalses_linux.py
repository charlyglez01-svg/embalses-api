"""
fetch_embalses_linux.py
-----------------------
Versión para GitHub Actions (Linux).
Usa mdbtools en lugar de pyodbc para leer el .mdb del MITECO.

mdbtools se instala con: sudo apt-get install mdbtools
"""

import os
import io
import zipfile
import sqlite3
import logging
import subprocess
import tempfile
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
ZIP_DIRECT_URL = (
    "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/"
    "evaluacion-de-los-recursos-hidricos/boletin-hidrologico/"
    "Historico-de-embalses/BD-Embalses.zip"
)

DB_PATH = os.path.join(os.path.dirname(__file__), "embalses.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EmbalsesScraper/1.0)"
}


# ─────────────────────────────────────────────
# 1. Detectar URL del ZIP
# ─────────────────────────────────────────────
def detectar_url_zip() -> str:
    try:
        log.info("Buscando URL del ZIP en la pagina del MITECO...")
        r = requests.get(MITECO_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "BD-Embalses" in href and href.endswith(".zip"):
                url = href if href.startswith("http") else "https://www.miteco.gob.es" + href
                log.info(f"URL detectada: {url}")
                return url
    except Exception as e:
        log.warning(f"No se pudo detectar URL: {e}")
    log.info(f"Usando URL por defecto: {ZIP_DIRECT_URL}")
    return ZIP_DIRECT_URL


# ─────────────────────────────────────────────
# 2. Descargar el ZIP
# ─────────────────────────────────────────────
def descargar_zip(url: str) -> bytes:
    log.info(f"Descargando ZIP desde: {url}")
    r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
    r.raise_for_status()
    data = b""
    for chunk in r.iter_content(chunk_size=65536):
        data += chunk
    log.info(f"ZIP descargado: {len(data)/1024/1024:.1f} MB")
    return data


# ─────────────────────────────────────────────
# 3. Extraer y parsear (Excel o MDB)
# ─────────────────────────────────────────────
def parsear(zip_bytes: bytes) -> pd.DataFrame:
    log.info("Extrayendo fichero del ZIP...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        nombres = zf.namelist()
        log.info(f"Ficheros en el ZIP: {nombres}")

        # ── Excel ──────────────────────────────────────────────────────────
        excel_files = [n for n in nombres if n.lower().endswith((".xlsx", ".xls"))]
        if excel_files:
            log.info(f"Leyendo Excel: {excel_files[0]}")
            with zf.open(excel_files[0]) as f:
                df = pd.read_excel(f, sheet_name=0, engine="openpyxl")
            return df

        # ── MDB con mdbtools ───────────────────────────────────────────────
        mdb_files = [n for n in nombres if n.lower().endswith(".mdb")]
        if mdb_files:
            with tempfile.NamedTemporaryFile(suffix=".mdb", delete=False) as tmp:
                tmp.write(zf.read(mdb_files[0]))
                tmp_path = tmp.name
            log.info(f"MDB extraido en: {tmp_path}")
            return parsear_mdb_linux(tmp_path)

        raise ValueError(f"No se encontro Excel ni MDB en el ZIP. Ficheros: {nombres}")


def parsear_mdb_linux(mdb_path: str) -> pd.DataFrame:
    """Lee un .mdb usando mdbtools (disponible en Linux)."""
    # Listar tablas
    result = subprocess.run(
        ["mdb-tables", "-1", mdb_path],
        capture_output=True, text=True
    )
    tablas = [t.strip() for t in result.stdout.strip().split("\n") if t.strip()]
    log.info(f"Tablas en el MDB: {tablas}")

    # Buscar tabla de embalses
    tabla = next(
        (t for t in tablas if any(k in t.lower() for k in ["embalse", "presa", "pantano"])),
        tablas[0] if tablas else None
    )
    if not tabla:
        raise ValueError("No se encontro ninguna tabla en el MDB")

    log.info(f"Exportando tabla: {tabla}")

    # Exportar a CSV con mdb-export
    result = subprocess.run(
        ["mdb-export", mdb_path, tabla],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error exportando MDB: {result.stderr}")

    df = pd.read_csv(io.StringIO(result.stdout))
    log.info(f"DataFrame cargado: {len(df)} filas, columnas: {list(df.columns)}")

    try:
        os.unlink(mdb_path)
    except Exception:
        pass

    return df


# ─────────────────────────────────────────────
# 4. Guardar en SQLite
# ─────────────────────────────────────────────
def guardar_db(df: pd.DataFrame):
    log.info(f"Guardando en: {DB_PATH}")

    # Normalizar fecha si existe
    for col in df.columns:
        if "fecha" in col.lower() or "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df.rename(columns={col: "fecha"}, inplace=True)
            break

    con = sqlite3.connect(DB_PATH)
    df.to_sql("embalses", con, if_exists="replace", index=False)

    con.execute("CREATE TABLE IF NOT EXISTS meta (clave TEXT PRIMARY KEY, valor TEXT)")
    con.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                ("ultima_actualizacion", datetime.now().isoformat()))
    con.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                ("total_registros", str(len(df))))
    con.commit()
    con.close()
    log.info(f"Base de datos actualizada con {len(df)} registros.")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("Actualizacion de datos de embalses (GitHub Actions)")
    log.info("=" * 50)
    url = detectar_url_zip()
    zip_bytes = descargar_zip(url)
    df = parsear(zip_bytes)
    guardar_db(df)
    log.info("Proceso completado con exito.")


if __name__ == "__main__":
    main()
