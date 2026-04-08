# central_mqtt_subscriber.py
import json, logging, os
import paho.mqtt.client as mqtt

CONFIG = {
    "MQTT_BROKER": "127.0.0.1",
    "MQTT_PORT": 1883,
    "MQTT_USER": "",
    "MQTT_PASS": "",
    "TOPIC": "coinvestigacion1/+/sessions"
}
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def on_connect(client, userdata, flags, rc):
    logging.info("Conectado al broker rc=%s. Subscribiendo %s", rc, CONFIG["TOPIC"])
    client.subscribe(CONFIG["TOPIC"], qos=1)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        session_dict = json.loads(payload)
        logging.info("Recibido device=%s session=%s", session_dict.get("device_serial"), session_dict.get("session_filename"))
        outdir = os.path.join(os.path.dirname(__file__), "..", "received_sessions")
        os.makedirs(outdir, exist_ok=True)
        fname = os.path.join(outdir, f"{session_dict.get('session_filename','untitled')}.json")
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(session_dict, f, ensure_ascii=False, indent=2)
        logging.info("Guardado %s", fname)
    except Exception as e:
        logging.exception("Error procesando mensaje: %s", e)

def main():
    client = mqtt.Client()
    if CONFIG["MQTT_USER"]:
        client.username_pw_set(CONFIG["MQTT_USER"], CONFIG["MQTT_PASS"])
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(CONFIG["MQTT_BROKER"], CONFIG["MQTT_PORT"])
    client.loop_forever()

if __name__ == "__main__":
    main()
