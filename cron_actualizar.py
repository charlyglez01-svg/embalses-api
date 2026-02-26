"""
cron_actualizar.py
------------------
Script que Railway ejecuta cada martes para actualizar los datos del MITECO.
No necesitas tocarlo — Railway lo llama automáticamente.
"""
import subprocess
import sys
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info(f"=== Actualización automática: {datetime.now().isoformat()} ===")
    try:
        result = subprocess.run(
            [sys.executable, "fetch_embalses.py"],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos máximo
        )
        if result.returncode == 0:
            log.info("OK - Datos actualizados correctamente")
            log.info(result.stdout)
        else:
            log.error("ERROR durante la actualizacion")
            log.error(result.stderr)
            sys.exit(1)
    except subprocess.TimeoutExpired:
        log.error("TIMEOUT - El script tardó más de 10 minutos")
        sys.exit(1)
    except Exception as e:
        log.error(f"Excepción inesperada: {e}")
        sys.exit(1)
