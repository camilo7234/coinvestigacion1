# servidor_iot.py
# Servidor IoT robusto y completo, conserva todas las funcionalidades previas:
# - ping (texto y JSON)
# - hello (registro de dispositivo en iot_devices.json)
# - data (telemetría en tiempo real -> guarda en datos_sensores.json)
# - send_file (transferencia con ACK y checksum)
# - lanza ejecutar_sesion_remota_iot(serial, method_params, gui_refresh_callback) si disponible
# - no bloquea al lanzar sesiones (usa hilos)
# - tolera encabezados inválidos y envía códigos de error

import socket
import json
import os
import threading
import sys
import importlib
import hashlib
import time
from datetime import datetime
from typing import Optional

HOST = "0.0.0.0"
PORT = 5000
BUFFER_SIZE = 4096
DEST_DIR = os.path.join(os.path.dirname(__file__), "archivos_recibidos")
IOT_DEVICES_FILE = os.path.join(os.path.dirname(__file__), "iot_devices.json")
IOT_DATA_FILE = os.path.join(os.path.dirname(__file__), "datos_sensores.json")

os.makedirs(DEST_DIR, exist_ok=True)

# Diccionario en memoria para dispositivos detectados
iot_devices = {}

# Cargar persistencia previa (si existe)
try:
    if os.path.exists(IOT_DEVICES_FILE):
        with open(IOT_DEVICES_FILE, "r", encoding="utf-8") as fh:
            iot_devices.update(json.load(fh))
except Exception:
    # no critical, continuar
    pass

# Asegurar import desde la raíz del proyecto (para módulos src.*)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ""))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def save_iot_devices():
    """Persistir iot_devices en archivo JSON."""
    try:
        with open(IOT_DEVICES_FILE, "w", encoding="utf-8") as fh:
            json.dump(iot_devices, fh, indent=2, default=str)
    except Exception as e:
        print(f"⚠️ No se pudo guardar {IOT_DEVICES_FILE}: {e}")


def save_sensor_data(serial: str, payload):
    """Guardar la última lectura por serial en datos_sensores.json (para monitor)."""
    try:
        data = {}
        if os.path.exists(IOT_DATA_FILE):
            with open(IOT_DATA_FILE, "r", encoding="utf-8") as fh:
                try:
                    data = json.load(fh)
                except Exception:
                    data = {}
        data[serial] = {
            "timestamp": _now_ts(),
            "payload": payload
        }
        with open(IOT_DATA_FILE, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
    except Exception as e:
        print(f"⚠️ No se pudieron guardar datos IoT: {e}")


def try_launch_remote_session(serial: str, method_params: Optional[dict] = None, gui_refresh_callback=None):
    """
    Intenta lanzar ejecutar_sesion_remota_iot(serial, method_params, gui_refresh_callback)
    en un hilo sin bloquear el servidor. Si no existe el módulo, lo registra.
    """
    try:
        if method_params is None:
            method_params = {}
        try:
            from src.pstrace_connection import ejecutar_sesion_remota_iot
        except Exception:
            # Fallback a import por nombre si no está dentro del paquete src
            mod = importlib.import_module("pstrace_connection")
            ejecutar_sesion_remota_iot = getattr(mod, "ejecutar_sesion_remota_iot")
        thread = threading.Thread(
            target=ejecutar_sesion_remota_iot,
            args=(serial, method_params, gui_refresh_callback),
            daemon=True
        )
        thread.start()
        print(f"🔧 Sesión remota lanzada para {serial}")
    except Exception as e:
        print(f"⚠ Error lanzando sesión remota: {e}")


def _recv_header(conn: socket.socket, max_header_bytes: int = 64 * 1024) -> Optional[str]:
    """
    Lee byte-a-byte hasta encontrar newline. Devuelve texto (sin newline) o None.
    Maneja CRLF y protege contra headers demasiado grandes.
    """
    try:
        header_data = b""
        while not header_data.endswith(b"\n"):
            chunk = conn.recv(1)
            if not chunk:
                break
            header_data += chunk
            if len(header_data) > max_header_bytes:
                break
        if not header_data:
            return None
        # Decodificar y normalizar CRLF/CR
        text = header_data.decode(errors="replace").rstrip("\r\n")
        return text.strip()
    except Exception:
        return None


def handle_client(conn: socket.socket, addr):
    """Maneja una conexión entrante (header JSON terminado en \\n, luego posible payload binario)."""
    try:
        header_text = _recv_header(conn)
        if header_text is None:
            print("⚠ Conexión vacía o header no recibido.")
            try:
                conn.close()
            except Exception:
                pass
            return

        # Soportar ping en texto simple (cliente antiguo)
        if header_text.lower() == "ping":
            print(f"📡 Ping (texto) recibido desde {addr}")
            try:
                conn.sendall(b"PONG\n")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            return

        # Intentar parsear JSON
        try:
            header = json.loads(header_text)
        except Exception as e:
            print(f"❌ Encabezado inválido desde {addr}: {e} - {header_text!r}")
            try:
                conn.sendall(b"ERR_INVALID_HEADER\n")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            return

        # A partir de aquí, header es un objeto JSON (normalmente dict)
        # Detectar y manejar actions primero (ping, hello, data)
        if isinstance(header, dict):
            # ------------- Acción: ping (JSON) -------------
            if header.get("action") == "ping":
                print(f"📡 Ping JSON recibido desde {addr}")
                try:
                    conn.sendall(b"PONG\n")
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                return

            # ------------- Acción: hello (registro de dispositivo) -------------
            if header.get("action") == "hello":
                serial = header.get("serial", "DESCONOCIDO")
                device_type = header.get("device_type", "UNKNOWN")
                ipaddr = addr[0]
                iot_devices[serial] = {
                    "ip": ipaddr,
                    "device_type": device_type,
                    "last_seen": time.time()
                }
                save_iot_devices()
                print(f"🔔 Hello recibido: serial={serial} type={device_type} desde {ipaddr}")
                try:
                    conn.sendall(b"ACK_HELLO\n")
                except Exception:
                    pass
                # lanzar sesión remota no bloqueante
                try_launch_remote_session(serial, method_params={}, gui_refresh_callback=None)
                try:
                    conn.close()
                except Exception:
                    pass
                return

            # ------------- Acción: data (telemetría en vivo) -------------
            if header.get("action") == "data":
                serial = header.get("serial", "DESCONOCIDO")
                payload = header.get("payload", {})
                print(f"📊 Datos en vivo desde {serial}: {payload}")
                # Guardar último valor para monitor GUI
                save_sensor_data(serial, payload)
                try:
                    conn.sendall(b"ACK_DATA\n")
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                return

            # ------------- Acción: send_file (transferencia de archivo) -------------
            if header.get("action") == "send_file":
                # validar keys necesarias
                if not all(k in header for k in ("filename", "size", "checksum")):
                    print(f"❌ Encabezado incompleto para send_file: {header}")
                    try:
                        conn.sendall(b"ERR_INCOMPLETE_HEADER\n")
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                    return

                filename = header["filename"]
                size = int(header["size"])
                checksum = header["checksum"]
                serial = header.get("serial", "DESCONOCIDO")
                print(f"🔎 Dispositivo detectado: {serial} - Recibiendo {filename} ({size/1e6:.2f} MB)")

                # Lanzar sesión remota en hilo (no bloquear)
                try_launch_remote_session(serial, method_params={}, gui_refresh_callback=None)

                filepath = os.path.join(DEST_DIR, filename)
                # Confirmar que servidor está listo para recibir
                try:
                    conn.sendall(b"ACK")
                except Exception:
                    pass

                with open(filepath, "wb") as f:
                    total_received = 0
                    while total_received < size:
                        data = conn.recv(BUFFER_SIZE)
                        if not data:
                            break
                        f.write(data)
                        total_received += len(data)

                print(f"✅ Archivo recibido: {filepath} ({total_received/1e6:.2f} MB)")

                # Validar checksum (no romper en caso de error, solo informar)
                try:
                    actual = hashlib.sha256(open(filepath, "rb").read()).hexdigest()
                    if actual != checksum:
                        print(f"⚠️ Checksum no coincide: esperado={checksum} actual={actual}")
                        try:
                            conn.sendall(b"ERR_CHECKSUM\n")
                        except Exception:
                            pass
                    else:
                        try:
                            conn.sendall(b"EOF_OK")
                        except Exception:
                            pass
                except Exception as ex:
                    print(f"⚠️ No se pudo verificar checksum: {ex}")
                    try:
                        conn.sendall(b"EOF_OK")
                    except Exception:
                        pass

                try:
                    conn.close()
                except Exception:
                    pass
                return

        # Si llegamos aquí: el header JSON no contenía un action conocido.
        # Pero puede tratarse de un header "implicito" de envío de archivo (sin action),
        # por compatibilidad con clientes que no envían action.
        if isinstance(header, dict) and all(k in header for k in ("filename", "size", "checksum")):
            # Tratar como send_file retrocompatible
            filename = header["filename"]
            size = int(header["size"])
            checksum = header["checksum"]
            serial = header.get("serial", "DESCONOCIDO")
            print(f"🔎 (retro) Dispositivo detectado: {serial} - Recibiendo {filename} ({size/1e6:.2f} MB)")

            try_launch_remote_session(serial, method_params={}, gui_refresh_callback=None)

            filepath = os.path.join(DEST_DIR, filename)
            try:
                conn.sendall(b"ACK")
            except Exception:
                pass

            with open(filepath, "wb") as f:
                total_received = 0
                while total_received < size:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                    f.write(data)
                    total_received += len(data)

            print(f"✅ Archivo recibido: {filepath} ({total_received/1e6:.2f} MB)")

            try:
                actual = hashlib.sha256(open(filepath, "rb").read()).hexdigest()
                if actual != checksum:
                    print(f"⚠️ Checksum no coincide: esperado={checksum} actual={actual}")
                    try:
                        conn.sendall(b"ERR_CHECKSUM\n")
                    except Exception:
                        pass
                else:
                    try:
                        conn.sendall(b"EOF_OK")
                    except Exception:
                        pass
            except Exception as ex:
                print(f"⚠️ No se pudo verificar checksum: {ex}")
                try:
                    conn.sendall(b"EOF_OK")
                except Exception:
                    pass

            try:
                conn.close()
            except Exception:
                pass
            return

        # Si no se reconoció la petición:
        print(f"❌ Acción desconocida o header mal formado: {header}")
        try:
            conn.sendall(b"ERR_UNKNOWN_ACTION\n")
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    except Exception as e:
        print(f"❌ Error manejando cliente {addr}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    print(f"🌐 Servidor IoT escuchando en {HOST}:{PORT}...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(5)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
