# cliente_iot.py - cliente completo (hello, data streaming, send_file, config)
import socket
import json
import os
import argparse
import hashlib
import time
from tqdm import tqdm

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config_cliente.json")


def cargar_config():
    """Carga o crea config_cliente.json con keys HOST, PORT, BUFFER_SIZE, SERIAL (opcional)."""
    if not os.path.exists(CONFIG_PATH):
        print("⚙ No se encontró config_cliente.json, creando archivo nuevo con valores por defecto...")
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
            config = json.load(f)
        # Validar que tenga las claves correctas
        if not all(k in config for k in ["HOST", "PORT", "BUFFER_SIZE"]):
            raise KeyError("Archivo de configuración incompleto.")
        return config
    except Exception as e:
        print(f"❌ Error al leer configuración: {e}")
        return None


def guardar_config(config):
    """Guardar config actual en config_cliente.json"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)
    print(f"✅ Configuración guardada en {CONFIG_PATH}")


def configurar():
    """Interfaz de consola para actualizar config_cliente.json"""
    config = cargar_config() or {}
    print("\n🧠 Configuración actual:")
    print(json.dumps(config, indent=4))

    print("\n✏ Ingrese nuevos valores (presione Enter para dejar el actual):")
    host = input(f"IP actual [{config.get('HOST', '')}]: ") or config.get('HOST', '127.0.0.1')
    port = input(f"Puerto actual [{config.get('PORT', '')}]: ") or config.get('PORT', 5000)
    buffer_size = input(f"Tamaño de buffer [{config.get('BUFFER_SIZE', '')}]: ") or config.get('BUFFER_SIZE', 4096)
    serial = input(f"Serial [{config.get('SERIAL','DEVICE_001')}]: ") or config.get('SERIAL', 'DEVICE_001')
    device_type = input(f"Device type [{config.get('DEVICE_TYPE','SENSOR_NODE')}]: ") or config.get('DEVICE_TYPE', 'SENSOR_NODE')

    config["HOST"] = host
    config["PORT"] = int(port)
    config["BUFFER_SIZE"] = int(buffer_size)
    config["SERIAL"] = serial
    config["DEVICE_TYPE"] = device_type

    guardar_config(config)
    print("✅ Configuración actualizada con éxito.")


def send_json(payload, timeout=5):
    """Envía un payload JSON (terminado en \\n) y devuelve la respuesta (bytes) si la hay."""
    cfg = cargar_config()
    if not cfg:
        print("❌ No se pudo cargar config.")
        return None
    host = cfg["HOST"]
    port = int(cfg["PORT"])
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(json.dumps(payload).encode() + b"\n")
            try:
                resp = s.recv(256)
                return resp
            except Exception:
                return None
    except Exception as e:
        print("❌ Error enviando JSON:", e)
        return None


def send_hello(serial=None, device_type=None):
    cfg = cargar_config()
    if not cfg:
        return
    serial = serial or cfg.get("SERIAL", "DESCONOCIDO")
    device_type = device_type or cfg.get("DEVICE_TYPE", "SENSOR_NODE")
    payload = {"action": "hello", "serial": serial, "device_type": device_type}
    resp = send_json(payload)
    print("Respuesta servidor (hello):", resp)


def send_data(serial, payload):
    """Envía una lectura de telemetría inmediata."""
    msg = {"action": "data", "serial": serial, "payload": payload}
    resp = send_json(msg)
    print("Respuesta servidor (data):", resp)


def enviar_archivo(path, serial=None):
    """Enviar archivo usando protocolo JSON header + bytes."""
    cfg = cargar_config()
    if not cfg:
        return
    if not os.path.exists(path):
        print("❌ Archivo no encontrado:", path)
        return
    size = os.path.getsize(path)
    checksum = hashlib.sha256(open(path, "rb").read()).hexdigest()
    header = {
        "action": "send_file",
        "filename": os.path.basename(path),
        "size": size,
        "checksum": checksum,
        "serial": serial or cfg.get("SERIAL", "DESCONOCIDO")
    }
    host = cfg["HOST"]
    port = int(cfg["PORT"])
    buffer = int(cfg.get("BUFFER_SIZE", 4096))
    try:
        with socket.create_connection((host, port)) as s:
            s.sendall(json.dumps(header).encode() + b"\n")
            ack = s.recv(16)
            if not ack or not ack.startswith(b"ACK"):
                print("❌ Servidor no aceptó la transferencia (ack mismatch) ->", ack)
                return
            with open(path, "rb") as f, tqdm(total=size, unit="B", unit_scale=True, desc="Enviando") as barra:
                while True:
                    chunk = f.read(buffer)
                    if not chunk:
                        break
                    s.sendall(chunk)
                    barra.update(len(chunk))
            try:
                final = s.recv(64)
                print("Respuesta final servidor:", final)
            except Exception:
                pass
            print("✅ Archivo enviado correctamente.")
    except Exception as e:
        print("❌ Error enviando archivo:", e)


def main():
    parser = argparse.ArgumentParser(description="Cliente IoT completo")
    parser.add_argument("--config", action="store_true", help="Editar configuración")
    parser.add_argument("--hello", action="store_true", help="Enviar hello")
    parser.add_argument("--serial", type=str, help="Serial para hello/data en línea")
    parser.add_argument("--devtype", type=str, help="Tipo de dispositivo para hello")
    parser.add_argument("--data", action="store_true", help="Enviar datos simulados continuamente")
    parser.add_argument("--file", type=str, help="Enviar archivo al servidor")
    args = parser.parse_args()

    if args.config:
        configurar()
    elif args.hello:
        send_hello(serial=args.serial, device_type=args.devtype)
    elif args.data:
        cfg = cargar_config()
        serial = args.serial or cfg.get("SERIAL", "DEVICE_001")
        print("🌡 Enviando datos simulados (Ctrl+C para detener)...")
        try:
            while True:
                simulated = {"temp": round(24 + 3 * ((time.time() % 10) / 10), 2), "hum": round(40 + 5 * ((time.time() % 7) / 7), 2)}
                send_data(serial, simulated)
                time.sleep(3)
        except KeyboardInterrupt:
            print("Interrupción por teclado, deteniendo envío de datos.")
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
