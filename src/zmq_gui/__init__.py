"""ZmqGui — standalone NiceGUI dashboard for any app publishing on ZMQ.

See README.md for architecture. The package exposes:
  - Dashboard:  the main class (app.py)
  - Bus:        thin ZMQ socket factory + topic framing (bus.py)
  - theme:      dark colour palette + layout constants

Everything else is an implementation detail.
"""

from .app import Dashboard, main  # noqa: F401
from .bus import Bus              # noqa: F401
