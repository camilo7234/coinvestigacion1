"""
Módulo para gestión de eventos y callbacks del dispositivo PalmSens.
Maneja la comunicación en tiempo real y eventos asíncronos.
"""

import asyncio
import logging
from typing import Callable, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import inspect
import threading
import concurrent.futures

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
        self._heartbeat_timeout = 10  # segundos por defecto
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._lock = threading.RLock()
        
    async def start(self):
        """Inicia el gestor de eventos"""
        # idempotente
        if self._running:
            logging.debug("DeviceEventManager ya está corriendo")
            return
        self._running = True
        asyncio.create_task(self._heartbeat_monitor())
        logging.info("✓ Gestor de eventos iniciado")
        
    async def stop(self):
        """Detiene el gestor de eventos"""
        self._running = False
        logging.info("✓ Gestor de eventos detenido")
        
    def subscribe(self, event_type: str, callback: Callable):
        """Suscribe un callback a un tipo de evento"""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)
        
    def unsubscribe(self, event_type: str, callback: Callable):
        """Elimina la suscripción de un callback"""
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [
                    cb for cb in self._subscribers[event_type]
                    if cb != callback
                ]
            
    async def emit_event(self, event: DeviceEvent):
        """Emite un evento a todos los suscriptores"""
        # Llamar tanto a subscriptores específicos como a los de '*'
        callbacks = []
        with self._lock:
            callbacks.extend(self._subscribers.get(event.type, []))
            callbacks.extend(self._subscribers.get('*', []))

        # Ejecutar cada callback; los callbacks síncronos se ejecutan en executor
        for callback in callbacks:
            try:
                if inspect.iscoroutinefunction(callback):
                    # callback asíncrono
                    await callback(event)
                else:
                    # callback síncrono: ejecutarlo en executor para no bloquear
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(self._executor, lambda cb, ev: cb(ev), callback, event)
            except Exception as e:
                try:
                    name = getattr(callback, '__name__', repr(callback))
                except Exception:
                    name = repr(callback)
                logging.error(f"Error en callback {name}: {str(e)}")
                    
    async def _heartbeat_monitor(self):
        """Monitorea heartbeats de dispositivos"""
        while self._running:
            now = datetime.now()
            with self._lock:
                items = list(self._last_heartbeat.items())
            for device_id, last_beat in items:
                if (now - last_beat).seconds > self._heartbeat_timeout:
                    await self.emit_event(DeviceEvent(
                        type="device_timeout",
                        timestamp=now,
                        data={"last_seen": last_beat},
                        device_id=device_id
                    ))
            await asyncio.sleep(1)

    async def register_heartbeat(self, device_id: str):
        """Registra actividad del dispositivo"""
        with self._lock:
            self._last_heartbeat[device_id] = datetime.now()

    async def emit(self, event_type: str, data: Any, device_id: str):
        """Conveniencia para emitir un evento creando DeviceEvent"""
        await self.emit_event(DeviceEvent(type=event_type, timestamp=datetime.now(), data=data, device_id=device_id))

    def emit_nowait(self, event_type: str, data: Any, device_id: str):
        """Emite un evento sin esperar (thread-safe)."""
        ev = DeviceEvent(type=event_type, timestamp=datetime.now(), data=data, device_id=device_id)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Encolar en bucle existente
                asyncio.run_coroutine_threadsafe(self.emit_event(ev), loop)
            else:
                # Ejecutar de forma síncrona
                loop.run_until_complete(self.emit_event(ev))
        except RuntimeError:
            # No hay loop: ejecutar en subtarea nueva
            try:
                asyncio.run(self.emit_event(ev))
            except Exception as e:
                logging.error(f"emit_nowait fallo: {e}")

    def get_subscriber_count(self, event_type: str = None) -> int:
        with self._lock:
            if event_type:
                return len(self._subscribers.get(event_type, []))
            return sum(len(v) for v in self._subscribers.values())

    @property
    def is_running(self) -> bool:
        return self._running
        
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