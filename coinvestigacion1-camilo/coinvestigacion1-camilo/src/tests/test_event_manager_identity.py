import sys
import os

# Asegurar path a src cuando se ejecuta desde la raíz
PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

from device_events import event_manager as em1
from src.device_events import event_manager as em2

def test_singleton_identity():
    assert em1 is em2, f"event_manager mismatch: {id(em1)} != {id(em2)}"

if __name__ == '__main__':
    # Run quick check
    test_singleton_identity()
    print('OK: event_manager singleton identity')
