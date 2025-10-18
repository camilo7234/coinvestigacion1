import socket
import json
import os
import argparse
import asyncio
from tqdm import tqdm
from device_events import DeviceEventManager, DeviceEvent

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config_cliente.json")
event_manager = DeviceEventManager()

# ...existing code for cargar_config(), guardar_config() and configurar()...

async def enviar_archivo(ruta_archivo):
    """Envía un archivo al servidor usando la configuración actual."""
    config = cargar_config()
    if not config:
        print("❌ No se pudo cargar configuración.")
        return

    host = config["HOST"]
    port = config["PORT"]
    buffer_size = config["BUFFER_SIZE"]

    if not os.path.exists(ruta_archivo):
        print("❌ Archivo no encontrado:", ruta_archivo)
        return

    print(f"📡 Conectando a {host}:{port}...")
    
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"✅ Conectado a {host}:{port}")

        # Iniciar gestor de eventos
        await event_manager.start()
        
        # Enviar nombre del archivo
        writer.write(os.path.basename(ruta_archivo).encode() + b"\n")
        await writer.drain()

        # Registrar conexión exitosa
        await event_manager.emit_event(DeviceEvent(
            type="connection_established",
            timestamp=datetime.datetime.now(),
            data={"host": host, "port": port},
            device_id="client"
        ))

        with open(ruta_archivo, "rb") as f, tqdm(
            total=os.path.getsize(ruta_archivo),
            unit="B", unit_scale=True, desc="Enviando"
        ) as barra:
            while chunk := f.read(buffer_size):
                writer.write(chunk)
                await writer.drain()
                barra.update(len(chunk))
                
                # Emitir evento de progreso
                await event_manager.emit_event(DeviceEvent(
                    type="transfer_progress",
                    timestamp=datetime.datetime.now(),
                    data={"bytes_sent": len(chunk)},
                    device_id="client"
                ))

        print("✅ Archivo enviado correctamente.")
        
        # Emitir evento de finalización
        await event_manager.emit_event(DeviceEvent(
            type="transfer_complete",
            timestamp=datetime.datetime.now(),
            data={"file": ruta_archivo},
            device_id="client"
        ))

    except Exception as e:
        print(f"❌ Error durante la transferencia: {e}")
        await event_manager.emit_event(DeviceEvent(
            type="transfer_error",
            timestamp=datetime.datetime.now(),
            data={"error": str(e)},
            device_id="client"
        ))
    finally:
        if 'writer' in locals():
            writer.close()
            await writer.wait_closed()
        await event_manager.stop()

async def iniciar_streaming(instrumento_id: str):
    """Inicia streaming de datos del instrumento"""
    config = cargar_config()
    if not config:
        print("❌ No se pudo cargar configuración.")
        return

    try:
        reader, writer = await asyncio.open_connection(
            config["HOST"], 
            config["PORT"]
        )
        
        await event_manager.start()
        print(f"✅ Streaming iniciado para instrumento {instrumento_id}")

        while True:
            try:
                # Enviar heartbeat cada 5 segundos
                writer.write(b"heartbeat\n")
                await writer.drain()
                
                # Registrar actividad
                await event_manager.register_heartbeat(instrumento_id)
                
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"❌ Error en streaming: {e}")
                # Intentar reconexión
                await asyncio.sleep(1)
                continue

    except Exception as e:
        print(f"❌ Error de conexión: {e}")
    finally:
        if 'writer' in locals():
            writer.close()
            await writer.wait_closed()
        await event_manager.stop()

async def main_async():
    """Versión asíncrona del main"""
    parser = argparse.ArgumentParser(description="Cliente IoT mejorado con configuración amigable.")
    parser.add_argument("--config", action="store_true", help="Abrir configuración interactiva")
    parser.add_argument("--send", type=str, help="Enviar archivo al servidor")
    parser.add_argument("--stream", type=str, help="Iniciar streaming para instrumento")
    args = parser.parse_args()

    if args.config:
        configurar()
    elif args.send:
        await enviar_archivo(args.send)
    elif args.stream:
        await iniciar_streaming(args.stream)
    else:
        print("Uso:")
        print("  python cliente_iot.py --config            # Modificar IP/puerto")
        print("  python cliente_iot.py --send archivo.txt  # Enviar archivo al servidor")
        print("  python cliente_iot.py --stream ID         # Iniciar streaming")

def main():
    """Wrapper para ejecutar main_async"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()