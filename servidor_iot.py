import socket
import json
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Set
from src.device_events import event_manager, DeviceEvent
from src.canonical import normalize_classification, display_label_from_label

# Configuración
HOST = "0.0.0.0"  # Escucha en todas las interfaces
PORT = 5000
BUFFER_SIZE = 4096
DEST_DIR = os.path.join(os.path.dirname(__file__), "archivos_recibidos")
IOT_DEVICES_FILE = os.path.join(os.path.dirname(__file__), "iot_devices.json")
IOT_DATA_FILE = os.path.join(os.path.dirname(__file__), "datos_sensores.json")

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Gestor de eventos (singleton importado desde src.device_events)

# Estado global
clientes_activos: Dict[str, asyncio.StreamWriter] = {}
dispositivos_conectados: Set[str] = set()

async def manejar_cliente(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Maneja una conexión cliente individual"""
    addr = writer.get_extra_info('peername')
    client_id = f"{addr[0]}:{addr[1]}"
    
    try:
        # Registrar cliente
        clientes_activos[client_id] = writer
        log.info(f"📡 Nueva conexión desde {addr}")

        # Recibir encabezado
        header_data = await reader.readuntil(b"\n")
        if not header_data:
            return

        try:
            header = json.loads(header_data.decode().strip())
            
            if header.get("type") == "streaming":
                await manejar_streaming(reader, writer, header, client_id)
            else:
                await manejar_archivo(reader, writer, header, client_id)
                
        except json.JSONDecodeError:
            # Si no es JSON, asumir nombre de archivo directo
            filename = header_data.decode().strip()
            await manejar_archivo_simple(reader, writer, filename, client_id)

    except Exception as e:
        log.error(f"❌ Error manejando cliente {client_id}: {e}")
        await event_manager.emit_event(DeviceEvent(
            type="client_error",
            timestamp=datetime.now(),
            data={"error": str(e)},
            device_id=client_id
        ))
    finally:
        # Limpieza
        writer.close()
        await writer.wait_closed()
        clientes_activos.pop(client_id, None)
        dispositivos_conectados.discard(client_id)
        log.info(f"👋 Cliente {client_id} desconectado")

async def manejar_streaming(reader, writer, header, client_id):
    """Maneja una conexión de streaming"""
    device_id = header.get("device_id", client_id)
    dispositivos_conectados.add(device_id)
    
    # Preparar payload y normalizar clasificación si existe
    data_payload = {"device_id": device_id}
    # Si el header incluye clasificación, normalizarla y añadir etiqueta de presentación
    if header.get('clasificacion') is not None:
        try:
            normalized = normalize_classification(header.get('clasificacion'))
            data_payload['clasificacion'] = normalized
            data_payload['display_label'] = display_label_from_label(normalized)
        except Exception:
            pass

    await event_manager.emit_event(DeviceEvent(
        type="stream_started",
        timestamp=datetime.now(),
        data=data_payload,
        device_id=device_id
    ))

    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
                
            if data.strip() == b"heartbeat":
                await event_manager.register_heartbeat(device_id)
                writer.write(b"heartbeat_ack\n")
                await writer.drain()
                continue
                
            # Procesar datos de streaming
            await event_manager.emit_event(DeviceEvent(
                type="stream_data",
                timestamp=datetime.now(),
                data={"raw_data": data.decode()},
                device_id=device_id
            ))

    except Exception as e:
        log.error(f"❌ Error en streaming {device_id}: {e}")
    finally:
        dispositivos_conectados.discard(device_id)

async def manejar_archivo(reader, writer, header, client_id):
    """Maneja la recepción de un archivo con metadata"""
    filename = header["filename"]
    size = int(header["size"])
    checksum = header["checksum"]
    
    log.info(f"📦 Recibiendo archivo: {filename} ({size/1e6:.2f} MB)")
    writer.write(b"ACK\n")
    await writer.drain()

    filepath = os.path.join(DEST_DIR, filename)
    total_received = 0
    
    with open(filepath, "wb") as f:
        while total_received < size:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            f.write(data)
            total_received += len(data)
            
            # Emitir progreso
            await event_manager.emit_event(DeviceEvent(
                type="file_progress",
                timestamp=datetime.now(),
                data={
                    "filename": filename,
                    "bytes_received": total_received,
                    "total_size": size
                },
                device_id=client_id
            ))

    log.info(f"✅ Archivo recibido: {filepath} ({total_received/1e6:.2f} MB)")
    writer.write(b"EOF_OK\n")
    await writer.drain()

async def manejar_archivo_simple(reader, writer, filename, client_id):
    """Maneja la recepción de un archivo simple"""
    filepath = os.path.join(DEST_DIR, filename)
    total_received = 0
    
    with open(filepath, "wb") as f:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            f.write(data)
            total_received += len(data)

    log.info(f"✅ Archivo recibido: {filepath} ({total_received/1e6:.2f} MB)")

async def main():
    """Función principal del servidor"""
    os.makedirs(DEST_DIR, exist_ok=True)
    
    # Iniciar gestor de eventos
    await event_manager.start()
    
    # Crear servidor
    server = await asyncio.start_server(
        manejar_cliente,
        HOST,
        PORT
    )
    
    log.info(f"🌐 Servidor IoT escuchando en {HOST}:{PORT}...")
    
    try:
        async with server:
            await server.serve_forever()
    finally:
        await event_manager.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("👋 Servidor detenido por el usuario")
    except Exception as e:
        log.error(f"❌ Error fatal: {e}")