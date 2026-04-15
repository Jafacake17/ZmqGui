"""
Dark theme constants for the ZmqGui dashboard.

Ported verbatim from ModularTradeApp/console/gui/theme.py so colours
stay identical across the two projects. Edit here if you want a
different palette; restart the server to pick up changes.
"""

# -- Colour palette (CSS) --
BG_DARK = "#0f0f14"
BG_PANEL = "#191923"
BG_HEADER = "#232332"

# Text
TEXT_PRIMARY = "#dcdce6"
TEXT_SECONDARY = "#8c8ca0"
TEXT_MUTED = "#505064"

# Status colours
GREEN = "#32cd64"        # Running / profit
YELLOW = "#e6be32"       # Paused / warning
RED = "#dc3c3c"          # Errored / loss / alert
BLUE = "#4682dc"         # Info / neutral

# P&L line chart
PNL_LINE_COLOUR = "#46b482"

# Table alternating rows
ROW_EVEN = "#191923"
ROW_ODD = "#1e1e2a"

# -- Layout constants --
WINDOW_WIDTH = 1200
WINDOW_HEIGHT = 800
CHART_HEIGHT = 250
TRADE_LOG_HEIGHT = 200
