import sqlite3
import json
import os

DB_PATH = "embalses.db"

# ─────────────────────────────────────────────
# Helpers copiados de tu api.py
# ─────────────────────────────────────────────
def limpiar_numero(valor):
    if valor is None:
        return None
    try:
        return float(str(valor).replace(",", "."))
    except Exception:
        return None

def formato_embalse(row):
    volumen = limpiar_numero(row["volumen_hm3"])
    agua    = limpiar_numero(row["AGUA_ACTUAL"])
    return {
        "nombre":          row["EMBALSE_NOMBRE"],
        "cuenca":          row["AMBITO_NOMBRE"],
        "fecha":           row["fecha"],
        "volumen_hm3":     volumen,
        "agua_actual_hm3": agua,
        "electrico":       bool(row["ELECTRICO_FLAG"]),
    }

# ─────────────────────────────────────────────
# Proceso de exportación
# ─────────────────────────────────────────────
def main():
    if not os.path.exists(DB_PATH):
        print(f"Error: No se encuentra {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Exportar Cuencas (cuencas.json)
    cur.execute("SELECT DISTINCT AMBITO_NOMBRE FROM embalses WHERE AMBITO_NOMBRE IS NOT NULL ORDER BY AMBITO_NOMBRE")
    cuencas = [r["AMBITO_NOMBRE"] for r in cur.fetchall()]
    with open("cuencas.json", "w", encoding="utf-8") as f:
        json.dump(cuencas, f, ensure_ascii=False, indent=2)
    print("✅ cuencas.json generado.")

    # 2. Exportar Resumen (resumen.json)
    cur.execute("SELECT MAX(fecha) as f FROM embalses")
    ultima_fecha = cur.fetchone()["f"]

    cur.execute("SELECT COUNT(DISTINCT EMBALSE_NOMBRE) AS total_embalses, COUNT(DISTINCT AMBITO_NOMBRE) AS total_cuencas FROM embalses WHERE fecha = ?", (ultima_fecha,))
    r_resumen = cur.fetchone()
    
    resumen_data = {
        "ultima_fecha":   ultima_fecha,
        "total_embalses": r_resumen["total_embalses"],
        "total_cuencas":  r_resumen["total_cuencas"]
    }
    with open("resumen.json", "w", encoding="utf-8") as f:
        json.dump(resumen_data, f, ensure_ascii=False, indent=2)
    print("✅ resumen.json generado.")

    # 3. Exportar Lista Principal (embalses.json)
    cur.execute("SELECT * FROM embalses WHERE fecha = ? ORDER BY EMBALSE_NOMBRE", (ultima_fecha,))
    embalses_ultimos = [formato_embalse(r) for r in cur.fetchall()]
    
    with open("embalses.json", "w", encoding="utf-8") as f:
        # Lo metemos en un objeto "data" para que tu front-end no note la diferencia
        json.dump({"data": embalses_ultimos}, f, ensure_ascii=False, indent=2)
    print("✅ embalses.json generado.")

    # 4. Exportar Detalles e Históricos para el Modal (carpeta /embalses/)
    os.makedirs("embalses", exist_ok=True) # Crea la carpeta si no existe
    cur.execute("SELECT DISTINCT EMBALSE_NOMBRE FROM embalses WHERE EMBALSE_NOMBRE IS NOT NULL")
    nombres_embalses = [r["EMBALSE_NOMBRE"] for r in cur.fetchall()]

    for nombre in nombres_embalses:
        cur.execute("SELECT * FROM embalses WHERE EMBALSE_NOMBRE = ? ORDER BY fecha DESC", (nombre,))
        historico = cur.fetchall()
        
        if not historico:
            continue
            
        ultimo = historico[0]
        detalle_data = {
            "nombre":      ultimo["EMBALSE_NOMBRE"],
            "cuenca":      ultimo["AMBITO_NOMBRE"],
            "ultimo_dato": formato_embalse(ultimo),
            "historico":   [formato_embalse(r) for r in historico]
        }
        
        # Reemplazamos barras por si algún embalse tiene un "/" en el nombre
        safe_nombre = nombre.replace("/", "-").replace("\\", "-")
        filepath = os.path.join("embalses", f"{safe_nombre}.json")
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(detalle_data, f, ensure_ascii=False, indent=2)
            
    print(f"✅ Creados los detalles de {len(nombres_embalses)} embalses en la carpeta /embalses/")
    
    conn.close()

if __name__ == "__main__":
    main()