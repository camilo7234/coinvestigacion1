# servidor_iot.py - versión robusta
import socket
import json
import os
import threading
import sys
import importlib
import hashlib

HOST = "0.0.0.0"  # Escucha en todas las interfaces
PORT = 5000
BUFFER_SIZE = 4096
DEST_DIR = "archivos_recibidos"

os.makedirs(DEST_DIR, exist_ok=True)

# Asegurar que la raíz del proyecto esté en sys.path para poder importar src.*
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "src"))
if project_root not in sys.path:
    # Insertar la carpeta que contiene tu paquete src (ajusta si tu estructura es distinta)
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "")))

print(f"🌐 Servidor IoT escuchando en {HOST}:{PORT}...")

def handle_client(conn, addr):
    try:
        header_data = b""
        while not header_data.endswith(b"\n"):
            chunk = conn.recv(1)
            if not chunk:
                break
            header_data += chunk

        if not header_data:
            print("⚠ Conexión vacía.")
            conn.close()
            return

        header_text = header_data.decode(errors="replace").strip()

        if header_text.lower() == "ping" or header_text.lower() == '{"action":"ping"}':
            print(f"📡 Ping recibido desde {addr}")
            try:
                conn.sendall(b"PONG\n")
            except Exception:
                pass
            conn.close()
            return

        try:
            header = json.loads(header_text)
        except Exception as e:
            print(f"❌ Encabezado inválido: {e} - {header_text!r}")
            try:
                conn.sendall(b"ERR_INVALID_HEADER\n")
            except Exception:
                pass
            conn.close()
            return

        if not all(k in header for k in ("filename", "size", "checksum")):
            print(f"❌ Encabezado incompleto: {header}")
            try:
                conn.sendall(b"ERR_INCOMPLETE_HEADER\n")
            except Exception:
                pass
            conn.close()
            return

        filename = header["filename"]
        size = int(header["size"])
        checksum = header["checksum"]
        serial = header.get("serial", "DESCONOCIDO")
        print(f"🔎 Dispositivo detectado: {serial} - Recibiendo {filename} ({size/1e6:.2f} MB)")

        # Intentar lanzar la sesión remota (no bloquear)
        try:
            try:
                from src.pstrace_connection import ejecutar_sesion_remota_iot
            except Exception:
                mod = importlib.import_module("pstrace_connection")
                ejecutar_sesion_remota_iot = getattr(mod, "ejecutar_sesion_remota_iot")
            threading.Thread(target=ejecutar_sesion_remota_iot, args=(serial, {}, None), daemon=True).start()
            print(f"🔧 Sesión remota lanzada para {serial}")
        except Exception as e:
            print(f"⚠ Error lanzando sesión remota: {e}")

        # Guardar archivo
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

        # Verificar checksum
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

    except Exception as e:
        print(f"❌ Error manejando cliente {addr}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.bind((HOST, PORT))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
