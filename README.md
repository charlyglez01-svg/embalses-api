# 🌊 Sistema de Datos de Embalses España

Backend que descarga automáticamente los datos oficiales del MITECO
y los expone como una API REST para tu web.

---

## 📁 Estructura del proyecto

```
embalses/
├── fetch_embalses.py   # Script de descarga y parseo
├── api.py              # API REST con Flask
├── embalses.db         # Base de datos SQLite (se genera automáticamente)
├── fetch.log           # Log de ejecuciones
├── requirements.txt    # Dependencias Python
└── README.md
```

---

## ⚙️ Instalación

### 1. Instalar dependencias

```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 2. Primera descarga de datos

```bash
python fetch_embalses.py
```

Esto descargará el ZIP del MITECO (~50 MB) y creará `embalses.db`.

### 3. Arrancar la API

```bash
# Desarrollo
python api.py

# Producción (con gunicorn)
gunicorn -w 4 -b 0.0.0.0:5000 "api:app"
```

---

## ⏰ Cron Job (actualización automática cada martes)

El MITECO publica datos nuevos cada martes. Para automatizar la descarga:

### Linux/Mac

```bash
# Abrir el editor de cron
crontab -e

# Añadir esta línea (ejecuta cada martes a las 10:00 AM):
0 10 * * 2 /ruta/a/venv/bin/python /ruta/a/embalses/fetch_embalses.py >> /ruta/a/embalses/fetch.log 2>&1
```

**Ejemplo con rutas reales:**
```
0 10 * * 2 /home/usuario/embalses/venv/bin/python /home/usuario/embalses/fetch_embalses.py >> /home/usuario/embalses/fetch.log 2>&1
```

### Windows (Task Scheduler)

```powershell
# Crear tarea programada en PowerShell (ejecutar como admin):
$action = New-ScheduledTaskAction -Execute "C:\ruta\venv\Scripts\python.exe" -Argument "C:\ruta\embalses\fetch_embalses.py"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tuesday -At 10:00AM
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "EmbalsesMITECO" -RunLevel Highest
```

---

## 🔌 Endpoints de la API

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/meta` | Info última actualización |
| GET | `/api/resumen` | Estadísticas nacionales |
| GET | `/api/cuencas` | Lista de cuencas |
| GET | `/api/embalses` | Todos los embalses (última semana) |
| GET | `/api/embalses?cuenca=Tajo` | Filtrar por cuenca |
| GET | `/api/embalses?min_porc=50` | Por % mínimo de llenado |
| GET | `/api/embalses/<nombre>` | Detalle + histórico |
| GET | `/api/embalses/<nombre>?desde=2020-01-01` | Histórico con fechas |

### Ejemplo de respuesta `/api/embalses`

```json
{
  "total": 412,
  "page": 1,
  "per_page": 50,
  "pages": 9,
  "data": [
    {
      "nombre": "Alcántara",
      "cuenca": "Tajo",
      "comunidad": "Extremadura",
      "capacidad_hm3": 3162.0,
      "volumen_hm3": 1890.4,
      "porcentaje": 59.8,
      "fecha": "2026-02-18"
    },
    ...
  ]
}
```

---

## 🌐 Uso desde tu web frontend

```javascript
// Obtener todos los embalses de la última semana
const resp = await fetch('http://localhost:5000/api/embalses');
const { data } = await resp.json();

// Filtrar por cuenca
const tajo = await fetch('http://localhost:5000/api/embalses?cuenca=Tajo');

// Resumen nacional
const resumen = await fetch('http://localhost:5000/api/resumen');
// → { total_embalses: 412, porcentaje_medio: 58.3, capacidad_total_hm3: 52847.0, ... }
```

---

## ☁️ Despliegue en producción

### Opción A: VPS propio (Nginx + Gunicorn)

```nginx
# /etc/nginx/sites-available/embalses
server {
    listen 80;
    server_name tu-dominio.com;

    location /api/ {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Opción B: Railway / Render / Fly.io (gratis)

Sube el proyecto a GitHub y despliégalo directamente. Añade un `Procfile`:

```
web: gunicorn -w 2 -b 0.0.0.0:$PORT "api:app"
```

Y configura el cron job con el scheduler integrado de cada plataforma.

---

## 📝 Notas

- Los datos del MITECO tienen un retardo de ~1 semana (datos provisionales).
- El fichero ZIP puede pesar ~50 MB; el script solo necesita ejecutarse una vez por semana.
- La base de datos SQLite resultante ocupa ~20-40 MB con datos desde 1988.
- **Fuente oficial:** https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico.html
