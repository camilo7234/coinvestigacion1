"""
Shim module at repository root to re-export the DeviceEventManager singleton
from the package `src.device_events` so code that imports `device_events` or
`src.device_events` sees the same objects.
"""
from src.device_events import DeviceEvent, DeviceEventManager, event_manager

__all__ = ["DeviceEvent", "DeviceEventManager", "event_manager"]
