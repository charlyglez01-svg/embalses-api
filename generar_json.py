"""
generar_json.py
---------------
Lee embalses.db y genera ficheros JSON estáticos para el frontend.
Columnas reales del MITECO:
  AMBITO_NOMBRE  → cuenca
  EMBALSE_NOMBRE → nombre
  fecha          → fecha
  AGUA_TOTAL     → capacidad total hm³
  AGUA_ACTUAL    → agua actual hm³
  ELECTRICO_FLAG → uso eléctrico
"""

import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "embalses.db")
OUT_DIR = os.path.join(os.path.dirname(__file__), "datos")

def get_con():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def query(sql, args=()):
    con = get_con()
    rows = [dict(r) for r in con.execute(sql, args).fetchall()]
    con.close()
    return rows

def limpiar_numero(valor):
    if valor is None:
        return None
    try:
        return float(str(valor).replace(",", "."))
    except:
        return None

def formato_embalse(row):
    total  = limpiar_numero(row.get("AGUA_TOTAL"))
    actual = limpiar_numero(row.get("AGUA_ACTUAL"))
    pct    = round((actual / total * 100), 1) if total and actual and total > 0 else None
    return {
        "nombre":        row.get("EMBALSE_NOMBRE"),
        "cuenca":        row.get("AMBITO_NOMBRE"),
        "fecha":         row.get("fecha"),
        "capacidad_hm3": total,
        "volumen_hm3":   actual,
        "porcentaje":    pct,
        "electrico":     bool(row.get("ELECTRICO_FLAG")),
    }

def guardar(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"  OK: {path}")

def main():
    print("Generando JSON estáticos...")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "embalses"), exist_ok=True)

    ultima_fecha = query("SELECT MAX(fecha) as f FROM embalses")[0]["f"]
    print(f"  Fecha: {ultima_fecha}")

    # resumen.json
    r = query(
        "SELECT COUNT(DISTINCT EMBALSE_NOMBRE) AS total_embalses, "
        "COUNT(DISTINCT AMBITO_NOMBRE) AS total_cuencas "
        "FROM embalses WHERE fecha = ?", [ultima_fecha]
    )[0]
    guardar(f"{OUT_DIR}/resumen.json", {
        "ultima_fecha":   ultima_fecha,
        "total_embalses": r["total_embalses"],
        "total_cuencas":  r["total_cuencas"],
    })

    # cuencas.json
    cuencas = [r["AMBITO_NOMBRE"] for r in query(
        "SELECT DISTINCT AMBITO_NOMBRE FROM embalses "
        "WHERE AMBITO_NOMBRE IS NOT NULL ORDER BY AMBITO_NOMBRE"
    )]
    guardar(f"{OUT_DIR}/cuencas.json", cuencas)

    # embalses.json
    rows = query("SELECT * FROM embalses WHERE fecha = ? ORDER BY EMBALSE_NOMBRE", [ultima_fecha])
    embalses = [formato_embalse(r) for r in rows]
    guardar(f"{OUT_DIR}/embalses.json", embalses)
    print(f"  Total embalses: {len(embalses)}")

    # embalses/<nombre>.json (histórico)
    nombres = [r["EMBALSE_NOMBRE"] for r in query(
        "SELECT DISTINCT EMBALSE_NOMBRE FROM embalses WHERE EMBALSE_NOMBRE IS NOT NULL"
    )]
    print(f"  Generando históricos de {len(nombres)} embalses...")
    for nombre in nombres:
        historico = query(
            "SELECT * FROM embalses WHERE EMBALSE_NOMBRE = ? ORDER BY fecha DESC", [nombre]
        )
        data = {
            "nombre":      nombre,
            "cuenca":      historico[0].get("AMBITO_NOMBRE") if historico else None,
            "ultimo_dato": formato_embalse(historico[0]) if historico else {},
            "historico":   [formato_embalse(r) for r in historico],
        }
        nombre_safe = nombre.replace("/", "_").replace("\\", "_").replace(" ", "_")
        guardar(f"{OUT_DIR}/embalses/{nombre_safe}.json", data)

    print("\nCompletado.")

if __name__ == "__main__":
    main()
