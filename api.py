"""
api.py
------
API REST para datos de embalses España (fuente: MITECO).

Columnas reales en la BD:
  AMBITO_NOMBRE   → cuenca hidrográfica
  EMBALSE_NOMBRE  → nombre del embalse
  fecha           → fecha del dato (YYYY-MM-DD)
  volumen_hm3     → volumen almacenado en hm³ (texto con coma decimal)
  AGUA_ACTUAL     → agua actual (numérico)
  ELECTRICO_FLAG  → 1 si tiene uso eléctrico

Endpoints:
  GET /api/meta
  GET /api/cuencas
  GET /api/resumen
  GET /api/embalses
  GET /api/embalses?cuenca=Tajo
  GET /api/embalses/<nombre>
  GET /api/embalses/<nombre>?desde=2020-01-01&hasta=2023-12-31

Iniciar:
  python api.py
"""

import sqlite3
import os
from flask import Flask, jsonify, request, g
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), "embalses.db")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def query_db(sql, args=(), one=False):
    cur = get_db().execute(sql, args)
    rv = cur.fetchall()
    cur.close()
    return ([dict(r) for r in rv] if not one else (dict(rv[0]) if rv else None))


def db_existe():
    return os.path.exists(DB_PATH)


def limpiar_numero(valor):
    """Convierte '91,00' o '91.00' o None a float."""
    if valor is None:
        return None
    try:
        return float(str(valor).replace(",", "."))
    except Exception:
        return None


def formato_embalse(row: dict) -> dict:
    """Convierte una fila raw de la BD a un dict limpio para la API."""
    total  = limpiar_numero(row.get("AGUA_TOTAL"))
    actual = limpiar_numero(row.get("AGUA_ACTUAL"))
    pct    = round((actual / total * 100), 1) if total and actual and total > 0 else None
    return {
        "nombre":          row.get("EMBALSE_NOMBRE"),
        "cuenca":          row.get("AMBITO_NOMBRE"),
        "fecha":           row.get("fecha"),
        "capacidad_hm3":   total,
        "volumen_hm3":     actual,
        "porcentaje":      pct,
        "electrico":       bool(row.get("ELECTRICO_FLAG")),
    }


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.route("/api/meta")
def meta():
    """Info de la ultima actualizacion."""
    if not db_existe():
        return jsonify({"error": "Base de datos no encontrada. Ejecuta fetch_embalses.py"}), 503
    rows = query_db("SELECT clave, valor FROM meta")
    return jsonify({r["clave"]: r["valor"] for r in rows})


@app.route("/api/cuencas")
def cuencas():
    """Lista de cuencas hidrograficas."""
    if not db_existe():
        return jsonify({"error": "Base de datos no encontrada"}), 503
    rows = query_db(
        "SELECT DISTINCT AMBITO_NOMBRE FROM embalses "
        "WHERE AMBITO_NOMBRE IS NOT NULL ORDER BY AMBITO_NOMBRE"
    )
    return jsonify([r["AMBITO_NOMBRE"] for r in rows])


@app.route("/api/resumen")
def resumen():
    """Estadisticas nacionales con los datos mas recientes."""
    if not db_existe():
        return jsonify({"error": "Base de datos no encontrada"}), 503

    ultima_fecha = query_db(
        "SELECT MAX(fecha) as f FROM embalses", one=True
    )["f"]

    r = query_db(
        "SELECT COUNT(DISTINCT EMBALSE_NOMBRE) AS total_embalses, "
        "COUNT(DISTINCT AMBITO_NOMBRE) AS total_cuencas "
        "FROM embalses WHERE fecha = ?",
        [ultima_fecha], one=True
    )

    return jsonify({
        "ultima_fecha":   ultima_fecha,
        "total_embalses": r["total_embalses"],
        "total_cuencas":  r["total_cuencas"],
    })


@app.route("/api/embalses")
def embalses_lista():
    """
    Embalses con el dato mas reciente disponible.

    Query params:
      cuenca   -> filtrar por cuenca (ej: ?cuenca=Tajo)
      page     -> pagina (default: 1)
      per_page -> resultados por pagina (default: 50, max: 200)
    """
    if not db_existe():
        return jsonify({"error": "Base de datos no encontrada"}), 503

    cuenca   = request.args.get("cuenca")
    page     = max(1, request.args.get("page", 1, type=int))
    per_page = min(200, max(1, request.args.get("per_page", 50, type=int)))

    ultima_fecha = query_db("SELECT MAX(fecha) as f FROM embalses", one=True)["f"]

    conditions = ["fecha = ?"]
    params     = [ultima_fecha]

    if cuenca:
        conditions.append("AMBITO_NOMBRE LIKE ?")
        params.append(f"%{cuenca}%")

    where = "WHERE " + " AND ".join(conditions)

    total = query_db(
        f"SELECT COUNT(*) as n FROM embalses {where}", params, one=True
    )["n"]

    offset = (page - 1) * per_page
    rows = query_db(
        f"SELECT * FROM embalses {where} ORDER BY EMBALSE_NOMBRE LIMIT ? OFFSET ?",
        params + [per_page, offset],
    )

    return jsonify({
        "ultima_fecha": ultima_fecha,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        (total + per_page - 1) // per_page,
        "data":         [formato_embalse(r) for r in rows],
    })


@app.route("/api/embalses/<string:nombre>")
def embalse_detalle(nombre):
    """
    Historico completo de un embalse.

    Query params:
      desde -> fecha inicio (ej: ?desde=2020-01-01)
      hasta -> fecha fin    (ej: ?hasta=2023-12-31)
    """
    if not db_existe():
        return jsonify({"error": "Base de datos no encontrada"}), 503

    desde = request.args.get("desde")
    hasta = request.args.get("hasta")

    conditions = ["EMBALSE_NOMBRE LIKE ?"]
    params     = [f"%{nombre}%"]

    if desde:
        conditions.append("fecha >= ?")
        params.append(desde)
    if hasta:
        conditions.append("fecha <= ?")
        params.append(hasta)

    where     = "WHERE " + " AND ".join(conditions)
    historico = query_db(
        f"SELECT * FROM embalses {where} ORDER BY fecha DESC", params
    )

    if not historico:
        return jsonify({"error": f"Embalse '{nombre}' no encontrado"}), 404

    ultimo = historico[0]
    return jsonify({
        "nombre":      ultimo.get("EMBALSE_NOMBRE"),
        "cuenca":      ultimo.get("AMBITO_NOMBRE"),
        "ultimo_dato": formato_embalse(ultimo),
        "historico":   [formato_embalse(r) for r in historico],
    })


# ─────────────────────────────────────────────
# Arranque
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\nEmbalses Espana - API")
    print("--------------------------")
    print("Endpoints:")
    print("  http://localhost:5000/api/meta")
    print("  http://localhost:5000/api/cuencas")
    print("  http://localhost:5000/api/resumen")
    print("  http://localhost:5000/api/embalses")
    print("  http://localhost:5000/api/embalses?cuenca=Tajo")
    print("  http://localhost:5000/api/embalses/Albarellos")
    print("--------------------------\n")
    app.run(debug=True, port=5000)
