import asyncio
import json
import logging
import os
from typing import Any, Dict

from device_events import event_manager, DeviceEvent

log = logging.getLogger(__name__)


class IoTPublisher:
    """Publisher simple que suscribe al event_manager y publica mensajes.

    Esta implementación guarda mensajes en memoria en `self.published` para
    facilitar tests sin requerir un broker MQTT real. En producción se puede
    extender para usar paho-mqtt o HTTP.
    """

    def __init__(self, topic_root: str = "coinvestigacion1"):
        self.topic_root = topic_root
        self._running = False
        self.published = []  # list of (topic, payload)
        self._subs = []

    async def start(self):
        if self._running:
            return
        self._running = True
        # Suscribir a eventos relevantes
        event_manager.subscribe('cv_data_point', self._on_cv_data_point)
        event_manager.subscribe('cv_config', self._on_cv_config)
        event_manager.subscribe('cv_measurement_complete', self._on_cv_complete)
        log.info("IoTPublisher started and subscribed to events")

    async def stop(self):
        if not self._running:
            return
        # No unsubscribe API public (DeviceEventManager supports unsubscribe)
        try:
            event_manager.unsubscribe('cv_data_point', self._on_cv_data_point)
            event_manager.unsubscribe('cv_config', self._on_cv_config)
            event_manager.unsubscribe('cv_measurement_complete', self._on_cv_complete)
        except Exception:
            pass
        self._running = False
        log.info("IoTPublisher stopped")

    async def _on_cv_data_point(self, event: DeviceEvent):
        payload = self._make_payload(event)
        topic = f"{self.topic_root}/{event.device_id}/data_point"
        await self._publish(topic, payload)

    async def _on_cv_config(self, event: DeviceEvent):
        payload = self._make_payload(event)
        topic = f"{self.topic_root}/{event.device_id}/config"
        await self._publish(topic, payload)

    async def _on_cv_complete(self, event: DeviceEvent):
        payload = self._make_payload(event)
        topic = f"{self.topic_root}/{event.device_id}/measurement_complete"
        await self._publish(topic, payload)

    def _make_payload(self, event: DeviceEvent) -> Dict[str, Any]:
        # Create serializable payload
        return {
            'type': event.type,
            'timestamp': event.timestamp.isoformat() if hasattr(event.timestamp, 'isoformat') else str(event.timestamp),
            'device_id': event.device_id,
            'data': event.data
        }

    async def _publish(self, topic: str, payload: Dict[str, Any]):
        # For now append to memory and log. Replace with real broker code later.
        # Make JSON serialization tolerant to datetimes, numpy types, etc.
        msg = json.dumps(payload, ensure_ascii=False, default=str)
        self.published.append((topic, msg))
        log.debug("Published to %s: %s", topic, msg[:200])


_default_publisher = IoTPublisher()

def get_default_publisher():
    return _default_publisher
