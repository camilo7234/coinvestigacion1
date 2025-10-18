"""
Módulo para gestión de eventos y callbacks del dispositivo PalmSens.
Maneja la comunicación en tiempo real y eventos asíncronos.
"""

import asyncio
import logging
from typing import Callable, Dict, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class DeviceEvent:
    """Estructura para eventos del dispositivo"""
    type: str
    timestamp: datetime
    data: Any
    device_id: str

class DeviceEventManager:
    """Gestor de eventos para dispositivos PalmSens"""
    
    def __init__(self):
        self._subscribers: Dict[str, list] = {}
        self._running = False
        self._last_heartbeat: Dict[str, datetime] = {}
        
    async def start(self):
        """Inicia el gestor de eventos"""
        self._running = True
        asyncio.create_task(self._heartbeat_monitor())
        logging.info("✓ Gestor de eventos iniciado")
        
    async def stop(self):
        """Detiene el gestor de eventos"""
        self._running = False
        logging.info("✓ Gestor de eventos detenido")
        
    def subscribe(self, event_type: str, callback: Callable):
        """Suscribe un callback a un tipo de evento"""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)
        
    def unsubscribe(self, event_type: str, callback: Callable):
        """Elimina la suscripción de un callback"""
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                cb for cb in self._subscribers[event_type] 
                if cb != callback
            ]
            
    async def emit_event(self, event: DeviceEvent):
        """Emite un evento a todos los suscriptores"""
        if event.type in self._subscribers:
            for callback in self._subscribers[event.type]:
                try:
                    await callback(event)
                except Exception as e:
                    logging.error(f"Error en callback {callback.__name__}: {str(e)}")
                    
    async def _heartbeat_monitor(self):
        """Monitorea heartbeats de dispositivos"""
        while self._running:
            now = datetime.now()
            for device_id, last_beat in self._last_heartbeat.items():
                if (now - last_beat).seconds > 10:  # 10s timeout
                    await self.emit_event(DeviceEvent(
                        type="device_timeout",
                        timestamp=now,
                        data={"last_seen": last_beat},
                        device_id=device_id
                    ))
            await asyncio.sleep(1)

    async def register_heartbeat(self, device_id: str):
        """Registra actividad del dispositivo"""
        self._last_heartbeat[device_id] = datetime.now()
        
# Ejemplos de uso:
async def handle_device_data(event: DeviceEvent):
    """Callback para datos del dispositivo"""
    logging.info(f"Datos recibidos de {event.device_id}: {event.data}")

async def handle_device_error(event: DeviceEvent):
    """Callback para errores del dispositivo"""
    logging.error(f"Error en dispositivo {event.device_id}: {event.data}")

# Uso del gestor de eventos
event_manager = DeviceEventManager()

# Ejemplo de configuración:
async def setup_device_events():
    await event_manager.start()
    
    # Registrar callbacks
    event_manager.subscribe("data_received", handle_device_data)
    event_manager.subscribe("device_error", handle_device_error)
    
    # Emitir evento de ejemplo
    await event_manager.emit_event(DeviceEvent(
        type="data_received",
        timestamp=datetime.now(),
        data={"voltage": 1.23, "current": 0.45},
        device_id="PSTrace001"
    ))