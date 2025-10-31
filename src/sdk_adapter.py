import logging
from typing import Optional

from device_events import event_manager, DeviceEvent
from src.canonical import normalize_classification, display_label_from_label

log = logging.getLogger(__name__)


class PalmSensAdapter:
    """Esqueleto de adaptador para PalmSens SDK.

    Implementa los métodos públicos esperados por el plan: connect, disconnect,
    start_measurement, stop_measurement, y mapea callbacks del SDK a eventos del
    `event_manager`. La parte de pythonnet/pspy se implementará en detalle más
    adelante cuando se integren pruebas contra hardware o sesiones .pssession.
    """

    def __init__(self, address: Optional[str] = None):
        self.address = address
        self.connected = False

    def connect(self):
        # Realizar conexión con SDK here (pythonnet)
        self.connected = True
        log.info("PalmSensAdapter connected to %s", self.address)
        # Emitir evento device_connected
        # event_manager.emit_nowait('device_connected', {'address': self.address}, device_id=self.address)

    def disconnect(self):
        self.connected = False
        log.info("PalmSensAdapter disconnected %s", self.address)
        # event_manager.emit_nowait('device_disconnected', {'address': self.address}, device_id=self.address)

    def start_measurement(self, params: dict):
        # Mapear inicio de medición a event_manager
        log.info("Start measurement with params: %s", params)
        # Normalizar clasificación en params si se proporcionó
        try:
            if isinstance(params, dict) and params.get('clasificacion') is not None:
                params['clasificacion'] = normalize_classification(params.get('clasificacion'))
                params['display_label'] = display_label_from_label(params['clasificacion'])
        except Exception:
            pass
        event_manager.emit_nowait('cv_measurement_start', params, device_id=self.address or 'UNKNOWN')

    def stop_measurement(self):
        log.info("Stop measurement")
        event_manager.emit_nowait('cv_measurement_stop', {}, device_id=self.address or 'UNKNOWN')
