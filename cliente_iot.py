# cliente_iot.py - Cliente IoT sincronizado con el panel
# Usa automáticamente la IP del panel (last_connection.json) o config_cliente.json como respaldo

import socket
import json
import os
import argparse
import hashlib
import time
from tqdm import tqdm

BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, "config_cliente.json")
LAST_CONN_PATH = os.path.join(BASE_DIR, "last_connection.json")


def cargar_config():
    """Carga config_cliente.json, creando uno por defecto si no existe."""
    if not os.path.exists(CONFIG_PATH):
        print("⚙ No se encontró config_cliente.json, creando uno nuevo...")
        config = {
            "HOST": "127.0.0.1",
            "PORT": 5000,
            "BUFFER_SIZE": 4096,
            "SERIAL": "DEVICE_001",
            "DEVICE_TYPE": "SENSOR_NODE"
        }
        guardar_config(config)
        return config
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error leyendo configuración: {e}")
        return None


def guardar_config(config):
    """Guarda config_cliente.json."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)
    print(f"✅ Configuración guardada en {CONFIG_PATH}")


def cargar_ultima_conexion():
    """Carga la última IP/puerto usados por el panel."""
    if os.path.exists(LAST_CONN_PATH):
        try:
            with open(LAST_CONN_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def obtener_host_y_puerto():
    """Devuelve HOST y PORT desde last_connection.json o config_cliente.json."""
    last = cargar_ultima_conexion()
    if last and "HOST" in last and "PORT" in last:
        return last["HOST"], int(last["PORT"])
    cfg = cargar_config()
    return cfg["HOST"], int(cfg["PORT"])


def configurar():
    """Interfaz interactiva para actualizar config_cliente.json."""
    cfg = cargar_config() or {}
    print("\n🧠 Configuración actual:")
    print(json.dumps(cfg, indent=4))

    print("\n✏ Ingrese nuevos valores (Enter para mantener):")
    cfg["HOST"] = input(f"IP actual [{cfg.get('HOST', '127.0.0.1')}]: ") or cfg.get("HOST", "127.0.0.1")
    cfg["PORT"] = int(input(f"Puerto actual [{cfg.get('PORT', 5000)}]: ") or cfg.get("PORT", 5000))
    cfg["BUFFER_SIZE"] = int(input(f"Buffer [{cfg.get('BUFFER_SIZE', 4096)}]: ") or cfg.get("BUFFER_SIZE", 4096))
    cfg["SERIAL"] = input(f"Serial [{cfg.get('SERIAL', 'DEVICE_001')}]: ") or cfg.get("SERIAL", "DEVICE_001")
    cfg["DEVICE_TYPE"] = input(f"Tipo [{cfg.get('DEVICE_TYPE', 'SENSOR_NODE')}]: ") or cfg.get("DEVICE_TYPE", "SENSOR_NODE")

    guardar_config(cfg)
    print("✅ Configuración actualizada.")


def send_json(payload, timeout=5):
    """Envía un JSON terminado en \\n y devuelve respuesta."""
    host, port = obtener_host_y_puerto()
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(json.dumps(payload).encode() + b"\n")
            try:
                return s.recv(256)
            except Exception:
                return None
    except Exception as e:
        print(f"❌ Error enviando JSON a {host}:{port} -> {e}")
        return None


def send_hello(serial=None, device_type=None):
    cfg = cargar_config()
    serial = serial or cfg.get("SERIAL", "DESCONOCIDO")
    device_type = device_type or cfg.get("DEVICE_TYPE", "SENSOR_NODE")
    payload = {"action": "hello", "serial": serial, "device_type": device_type}
    resp = send_json(payload)
    print("Respuesta servidor (hello):", resp)


def send_data(serial, payload):
    msg = {"action": "data", "serial": serial, "payload": payload}
    resp = send_json(msg)
    print("Respuesta servidor (data):", resp)


def enviar_archivo(path, serial=None):
    """Envía un archivo con encabezado JSON + bytes binarios."""
    cfg = cargar_config()
    if not os.path.exists(path):
        print("❌ Archivo no encontrado:", path)
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


def main():
    parser = argparse.ArgumentParser(description="Cliente IoT sincronizado")
    parser.add_argument("--config", action="store_true", help="Editar configuración local")
    parser.add_argument("--hello", action="store_true", help="Enviar saludo de dispositivo")
    parser.add_argument("--serial", type=str, help="Serial del dispositivo")
    parser.add_argument("--devtype", type=str, help="Tipo de dispositivo")
    parser.add_argument("--data", action="store_true", help="Simular datos periódicos")
    parser.add_argument("--file", type=str, help="Enviar archivo")
    args = parser.parse_args()

    if args.config:
        configurar()
    elif args.hello:
        send_hello(serial=args.serial, device_type=args.devtype)
    elif args.data:
        cfg = cargar_config()
        serial = args.serial or cfg.get("SERIAL", "DEVICE_001")
        print("🌡 Enviando datos simulados... Ctrl+C para detener")
        try:
            while True:
                simulated = {
                    "temp": round(24 + 3 * ((time.time() % 10) / 10), 2),
                    "hum": round(40 + 5 * ((time.time() % 7) / 7), 2)
                }
                send_data(serial, simulated)
                time.sleep(3)
        except KeyboardInterrupt:
            print("🛑 Envío detenido.")
    elif args.file:
        enviar_archivo(args.file, serial=args.serial)
    else:
        print("Uso:")
        print("  python cliente_iot.py --config")
        print("  python cliente_iot.py --hello --serial ESP32_01")
        print("  python cliente_iot.py --data --serial ESP32_01")
        print("  python cliente_iot.py --file archivo.txt --serial ESP32_01")


if __name__ == "__main__":
    main()
