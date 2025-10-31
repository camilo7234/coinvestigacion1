# cliente_iot.py - Cliente IoT sincronizado con el panel
# Usa automáticamente la IP del panel (last_connection.json) o config_cliente.json como respaldo

import socket
import json
import os
import argparse
import asyncio
from tqdm import tqdm
from device_events import event_manager, DeviceEvent
from src.canonical import normalize_classification, display_label_from_label

BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, "config_cliente.json")
LAST_CONN_PATH = os.path.join(BASE_DIR, "last_connection.json")


# ...existing code for cargar_config(), guardar_config() and configurar()...

async def enviar_archivo(ruta_archivo):
    """Envía un archivo al servidor usando la configuración actual."""
    config = cargar_config()
    if not config:
        print("❌ No se pudo cargar configuración.")
        return
    host, port = obtener_host_y_puerto()
    buffer = int(cfg.get("BUFFER_SIZE", 4096))
    serial = serial or cfg.get("SERIAL", "DESCONOCIDO")
    size = os.path.getsize(path)
    checksum = hashlib.sha256(open(path, "rb").read()).hexdigest()
    header = {
        "action": "send_file",
        "filename": os.path.basename(path),
        "size": size,
        "checksum": checksum,
        "serial": serial
    }

    # Normalizar clasificación en el header si fuera proporcionada por algún caller
    if header.get('clasificacion') is not None:
        try:
            header['clasificacion'] = normalize_classification(header.get('clasificacion'))
            header['display_label'] = display_label_from_label(header['clasificacion'])
        except Exception:
            pass

    try:
        with socket.create_connection((host, port)) as s:
            s.sendall(json.dumps(header).encode() + b"\n")
            ack = s.recv(16)
            if not ack or not ack.startswith(b"ACK"):
                print(f"❌ Servidor no aceptó transferencia ({ack})")
                return
            with open(path, "rb") as f, tqdm(total=size, unit="B", unit_scale=True, desc="Enviando") as barra:
                for chunk in iter(lambda: f.read(buffer), b""):
                    s.sendall(chunk)
                    barra.update(len(chunk))
            try:
                print("Respuesta final servidor:", s.recv(64))
            except Exception:
                pass
            print("✅ Archivo enviado correctamente.")
    except Exception as e:
        print(f"❌ Error enviando archivo a {host}:{port} -> {e}")

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