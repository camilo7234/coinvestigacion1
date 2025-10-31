import asyncio
import logging
import sys
import os

# Asegurar que 'src' está en sys.path para imports locales cuando el test se ejecuta desde la raíz
PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
REPO_ROOT = os.path.abspath(os.path.join(PROJECT_SRC, '..'))
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

from device_events import event_manager, DeviceEvent
from pstrace_connection import simulate_stream_from_pssession, configurar_sdk_palmsens, cargar_sesion_pssession

logging.basicConfig(level=logging.DEBUG)

async def on_data(event: DeviceEvent):
    print(f"[TEST] Data received: {event.data}")

async def main():
    await event_manager.start()
    event_manager.subscribe('cv_data_point', on_data)

    # intentar localizar una psession en data/ (usar la que exista)
    import os
    test_ps = os.path.abspath(os.path.join(REPO_ROOT, 'data', 'ultima_medicion.pssession'))
    if not os.path.exists(test_ps):
        print("No hay archivo .pssession de prueba; finalizando test")
        return

    # cargar dll para construir metodo_load (si es necesario)
    dll = configurar_sdk_palmsens()
    # Ejecutar simulación en hilo para no bloquear y capturar excepciones
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, simulate_stream_from_pssession, None, test_ps, 5.0, 'TEST_SIM')
    except Exception:
        import traceback
        traceback.print_exc()
        print("simulate_stream_from_pssession raised an exception; continuing for cleanup")
    finally:
        # dar tiempo para procesar últimos eventos y apagar el manager
        await asyncio.sleep(1)
        await event_manager.stop()

if __name__ == '__main__':
    asyncio.run(main())
