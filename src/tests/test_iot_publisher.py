import asyncio
import sys
import os

PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

import pytest

from device_events import event_manager, DeviceEvent
from src.iot_publisher import IoTPublisher


@pytest.mark.asyncio
async def test_publisher_receives_events():
    pub = IoTPublisher(topic_root='testroot')
    await pub.start()

    # Emit fake event
    ev = DeviceEvent(type='cv_data_point', timestamp=__import__('datetime').datetime.now(), data={'v': 1}, device_id='DEV1')
    await event_manager.emit_event(ev)

    # small sleep to let callbacks run
    await asyncio.sleep(0.1)
    await pub.stop()

    assert len(pub.published) >= 1
    topic, payload = pub.published[0]
    assert 'testroot/DEV1/data_point' in topic
