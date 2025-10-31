import asyncio
import sys
from pathlib import Path

# Ensure repo root is on sys.path so imports like `device_events` and `src.*` work
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from device_events import event_manager, DeviceEvent
from src.iot_publisher import IoTPublisher


async def main():
    p = IoTPublisher('testroot')
    await p.start()
    # Emit a test event
    await event_manager.emit_event(DeviceEvent(type='cv_data_point', timestamp=__import__('datetime').datetime.now(), data={'v': 1}, device_id='DEV1'))
    await asyncio.sleep(0.1)
    await p.stop()
    print('PUBLISHED:', p.published)

if __name__ == '__main__':
    asyncio.run(main())
