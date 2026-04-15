"""
ZmqGui — standalone NiceGUI dashboard that subscribes to any set of
ZMQ PUB endpoints and renders fills, heartbeats, metrics, and
arbitrage snapshots.

This is a faithful port of ModularTradeApp/console/gui/app.py,
decoupled from that project's imports so it can run as an
independent web server that any ZMQ-publishing app (orchestrators,
scanners, bots) can aim at.

Message envelope it speaks:
    topic = "fill.<scenario>.<strategy>" or "fill.<strategy>"
    topic = "metric.<strategy>"
    topic = "heartbeat.<strategy>"
    topic = "arb.scan" (whole-snapshot from a sidecar)
    topic = "tick.<symbol>" (on the separate feed endpoint)
    topic = "command" (stop / reload / ...)

See README.md for the dataflow picture.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from typing import Optional

import zmq
from nicegui import ui

from .bus import Bus
from .config import GuiCfg, load as load_config
from .theme import (
    BG_DARK, BG_PANEL, BG_HEADER,
    GREEN, YELLOW, RED, BLUE, TEXT_SECONDARY, TEXT_PRIMARY,
    PNL_LINE_COLOUR, ROW_EVEN, ROW_ODD,
    CHART_HEIGHT, TRADE_LOG_HEIGHT,
)

logger = logging.getLogger(__name__)

# Maximum items to keep in memory for display
MAX_TRADE_LOG = 200
MAX_PNL_POINTS = 500

# Dedup window for fills — publishers may dual-publish every fill on
# both `fill.<scenario>.<strategy>` and `fill.<strategy>` during the
# scenarios transition. We dedupe by (scenario_id, order_id) so each
# fill is processed exactly once. The deque is bounded so memory stays flat.
MAX_FILL_DEDUP = 4096


class Dashboard:
    """
    Main dashboard. Subscribes to configured ZMQ sources and serves a
    web GUI via NiceGUI.

    Usage:
        cfg = load_config("config.yaml")
        Dashboard(cfg).run()   # blocks — starts the web server
    """

    def __init__(self, cfg: GuiCfg):
        self.cfg = cfg

        # -- State updated by ZMQ thread, read by UI timer --
        self._lock = threading.Lock()

        # Per-strategy tracking
        # strategy_id -> {status, pnl, trades, wins, last_heartbeat, last_metric, ...}
        self._strategies: dict[str, dict] = {}

        # Per-scenario tracking — populated from scenario-tagged fills and
        # heartbeat scenario_exposure payloads. Empty dict means scenarios
        # are not enabled or have not started reporting yet, in which case
        # the strategy table behaves as if only one scenario exists.
        self._scenarios: dict[str, dict] = {}

        # Cumulative P&L over time: list of (timestamp, cumulative_pnl)
        self._pnl_history: list[tuple[float, float]] = []
        self._total_pnl: float = 0.0

        # Recent trades (fills) — newest first
        self._trades: deque[dict] = deque(maxlen=MAX_TRADE_LOG)

        # Fill deduplication — see MAX_FILL_DEDUP comment above.
        self._seen_fills: deque[tuple[str, str]] = deque(maxlen=MAX_FILL_DEDUP)
        self._seen_fills_set: set[tuple[str, str]] = set()

        # Alert banner text (cleared after display)
        self._alert: str = ""

        # Latest snapshot from an arbitrage scanner sidecar. None until
        # the first `arb.scan` message arrives. The Arbitrage tab's timer
        # reads this on every render tick.
        self._arb_scan: Optional[dict] = None

        # Mode label (e.g. "live" / "paper") — updated from heartbeat.
        self._mode: str = "--"

        # Open trades — keyed by (strategy_id, symbol). An entry fill
        # opens a position; a closing fill (opposite side, same key)
        # removes it. Also synced authoritatively from heartbeat's
        # `open_positions` payload when that's present.
        self._open_trades: dict[tuple[str, str], dict] = {}

        # Latest tick prices from the feed PUB — symbol → {bid, ask}
        self._tick_prices: dict[str, dict] = {}

        # ZMQ thread control
        self._zmq_running = False
        self._zmq_thread = None
        self._feed_thread = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def run(self):
        """Build the web GUI, start ZMQ, and launch the NiceGUI server. Blocks."""
        self._start_zmq()
        self._build_page()

        ui.run(
            title="ZmqGui",
            host="0.0.0.0",   # Bind to all interfaces (not just localhost)
            port=self.cfg.web_port,
            dark=True,
            reload=False,
            show=False,       # Don't auto-open browser (headless VPS friendly)
        )

    # ------------------------------------------------------------------ #
    # Page build — NiceGUI layout
    # ------------------------------------------------------------------ #

    def _build_page(self):
        """Define the single-page dashboard layout. NiceGUI builds it lazily."""

        dashboard = self
        cfg = self.cfg

        @ui.page("/")
        def index():
            ui.add_head_html(f"""
            <style>
                body {{
                    background-color: {BG_DARK} !important;
                    color: {TEXT_PRIMARY} !important;
                }}
                .nicegui-content {{
                    padding: 16px !important;
                }}
                .q-table__container {{
                    background-color: {BG_PANEL} !important;
                    color: {TEXT_PRIMARY} !important;
                }}
                .q-table thead th {{
                    background-color: {BG_HEADER} !important;
                    color: {TEXT_PRIMARY} !important;
                    font-weight: bold;
                }}
                .q-table tbody tr:nth-child(even) {{
                    background-color: {ROW_EVEN} !important;
                }}
                .q-table tbody tr:nth-child(odd) {{
                    background-color: {ROW_ODD} !important;
                }}
                .q-table tbody td {{
                    color: {TEXT_PRIMARY} !important;
                }}
            </style>
            """)

            # ---- Header bar ----
            with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                f"background-color: {BG_HEADER}; border-radius: 6px;"
            ):
                ui.label("ZMQ GUI").classes("text-xl font-bold").style(
                    f"color: {TEXT_PRIMARY};"
                )
                mode_label = ui.label("Mode: --").style(f"color: {TEXT_SECONDARY};")
                pnl_label = ui.label("Daily P&L: --").style(f"color: {GREEN};")
                strat_label = ui.label("Strategies: 0").style(f"color: {TEXT_SECONDARY};")
                clock_label = ui.label("").style(f"color: {TEXT_SECONDARY};")

            # Alert banner (hidden by default)
            alert_label = ui.label("").style(
                f"color: {RED}; font-weight: bold; padding: 4px 16px;"
            )

            # ---- Tabs ----
            with ui.tabs().classes("w-full").style(
                f"background-color: {BG_HEADER};"
            ) as tabs:
                console_tab = (
                    ui.tab("Console").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("console", True) else None
                )
                ftmo_tab = (
                    ui.tab("FTMO MT5").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("ftmo", True) else None
                )
                arb_tab = (
                    ui.tab("Arbitrage").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("arb", True) else None
                )

            default_tab = console_tab or ftmo_tab or arb_tab
            with ui.tab_panels(tabs, value=default_tab).classes("w-full").style(
                f"background-color: {BG_DARK};"
            ):

              # ================ CONSOLE TAB ================
              if console_tab is not None:
                with ui.tab_panel(console_tab):

                    # ---- Scenarios summary card ----
                    ui.label("Scenarios").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    scen_columns = [
                        {"name": "scenario", "label": "Scenario", "field": "scenario", "align": "left"},
                        {"name": "broker", "label": "Broker", "field": "broker", "align": "left"},
                        {"name": "live", "label": "Live", "field": "live", "align": "center"},
                        {"name": "status", "label": "Status", "field": "status", "align": "center"},
                        {"name": "pnl", "label": "P&L Today", "field": "pnl", "align": "right"},
                        {"name": "trades", "label": "Trades", "field": "trades", "align": "center"},
                        {"name": "win_pct", "label": "Win %", "field": "win_pct", "align": "center"},
                        {"name": "n_strategies", "label": "Strategies", "field": "n_strategies", "align": "center"},
                    ]
                    scen_table = ui.table(
                        columns=scen_columns, rows=[], row_key="scenario",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    scen_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.status === 'active' ? '""" + GREEN + r"""'
                                     : props.row.status === 'killed' ? '""" + RED + r"""'
                                     : '""" + YELLOW + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.status }}</span>
                        </q-td>
                    """)
                    scen_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)

                    # ---- Strategy table ----
                    columns = [
                        {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                        {"name": "status", "label": "Status", "field": "status", "align": "center"},
                        {"name": "pnl", "label": "P&L Today", "field": "pnl", "align": "right"},
                        {"name": "trades", "label": "Trades", "field": "trades", "align": "center"},
                        {"name": "win_pct", "label": "Win %", "field": "win_pct", "align": "center"},
                        {"name": "last_signal", "label": "Last Signal", "field": "last_signal", "align": "left"},
                    ]
                    strat_table = ui.table(
                        columns=columns, rows=[], row_key="strategy",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    strat_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.status === 'RUNNING' ? '""" + GREEN + r"""'
                                     : props.row.status === 'PAUSED' || props.row.status === 'STALE' ? '""" + YELLOW + r"""'
                                     : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.status }}</span>
                        </q-td>
                    """)
                    strat_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)

                    # ---- Open Trades table ----
                    ui.label("Open Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    ot_columns = [
                        {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                        {"name": "pair", "label": "Pair", "field": "pair", "align": "left"},
                        {"name": "direction", "label": "Direction", "field": "direction", "align": "center"},
                        {"name": "entry_price", "label": "Entry", "field": "entry_price", "align": "right"},
                        {"name": "current_price", "label": "Current", "field": "current_price", "align": "right"},
                        {"name": "stop_loss", "label": "SL", "field": "stop_loss", "align": "right"},
                        {"name": "take_profit", "label": "TP", "field": "take_profit", "align": "right"},
                        {"name": "timeout", "label": "Timeout", "field": "timeout", "align": "center"},
                    ]
                    ot_rows = [{
                        "key": "_empty",
                        "strategy": "--",
                        "pair": "No open trades",
                        "direction": "--",
                        "entry_price": "--",
                        "current_price": "--",
                        "stop_loss": "--",
                        "take_profit": "--",
                        "timeout": "--",
                        "pnl_positive": True,
                    }]
                    open_trades_table = ui.table(
                        columns=ot_columns, rows=ot_rows, row_key="key",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    open_trades_table.add_slot("body-cell-direction", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.direction === 'BUY' ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.direction }}</span>
                        </q-td>
                    """)
                    open_trades_table.add_slot("body-cell-current_price", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_positive ? '""" + GREEN + r"""' : '""" + RED + r"""'
                            }">{{ props.row.current_price }}</span>
                        </q-td>
                    """)

                    # ---- Bottom panel: chart (left) + trade log (right) ----
                    with ui.row().classes("w-full gap-4 mt-4"):
                        chart_fig = {
                            "data": [{
                                "x": [], "y": [],
                                "type": "scatter", "mode": "lines",
                                "line": {"color": PNL_LINE_COLOUR, "width": 2},
                                "name": "Cumulative P&L",
                            }],
                            "layout": {
                                "paper_bgcolor": BG_DARK,
                                "plot_bgcolor": BG_PANEL,
                                "font": {"color": TEXT_PRIMARY},
                                "margin": {"l": 50, "r": 20, "t": 30, "b": 30},
                                "xaxis": {"showticklabels": False, "gridcolor": "#32324a"},
                                "yaxis": {"title": "P&L", "gridcolor": "#32324a"},
                                "title": {"text": "Cumulative P&L", "font": {"size": 14}},
                                "showlegend": False,
                            },
                        }
                        pnl_chart = ui.plotly(chart_fig).classes("flex-grow").style(
                            f"min-width: 500px; height: {CHART_HEIGHT + TRADE_LOG_HEIGHT}px;"
                        )

                        with ui.card().classes("flex-shrink-0").style(
                            f"width: 380px; height: {CHART_HEIGHT + TRADE_LOG_HEIGHT}px; "
                            f"background-color: {BG_PANEL}; overflow-y: auto;"
                        ):
                            ui.label("Recent Trades").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            trade_log = ui.label("Waiting for trades...").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )
              else:
                  scen_table = strat_table = open_trades_table = None
                  pnl_chart = trade_log = None
                  chart_fig = None

              # ================ FTMO MT5 TAB ================
              if ftmo_tab is not None:
                with ui.tab_panel(ftmo_tab):
                    with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                        f"background-color: {BG_PANEL}; border-radius: 6px;"
                    ):
                        ftmo_status_label = ui.label("MT5 Status: checking...").style(
                            f"color: {YELLOW}; font-weight: bold;"
                        )
                        ui.label(cfg.ftmo.account_summary).style(
                            f"color: {TEXT_SECONDARY};"
                        )
                        ui.label(cfg.ftmo.ea_summary).style(
                            f"color: {TEXT_SECONDARY};"
                        )

                    ui.label("FTMO Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    ftmo_trade_columns = [
                        {"name": "time", "label": "Time", "field": "time", "align": "left"},
                        {"name": "type", "label": "Type", "field": "type", "align": "center"},
                        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                        {"name": "lots", "label": "Lots", "field": "lots", "align": "right"},
                        {"name": "price", "label": "Price", "field": "price", "align": "right"},
                        {"name": "sl", "label": "SL", "field": "sl", "align": "right"},
                        {"name": "tp", "label": "TP", "field": "tp", "align": "right"},
                        {"name": "pnl", "label": "P&L", "field": "pnl", "align": "right"},
                        {"name": "comment", "label": "Comment", "field": "comment", "align": "left"},
                    ]
                    ftmo_trade_table = ui.table(
                        columns=ftmo_trade_columns, rows=[], row_key="time",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    ftmo_trade_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)
                    ftmo_trade_table.add_slot("body-cell-type", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.type === 'SELL' ? '""" + RED + r"""' : '""" + GREEN + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.type }}</span>
                        </q-td>
                    """)

                    with ui.row().classes("w-full gap-4 mt-4"):
                        with ui.card().classes("flex-grow").style(
                            f"height: 400px; background-color: {BG_PANEL}; overflow-y: auto;"
                        ):
                            ui.label("EA Log").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            ftmo_log_label = ui.label("Loading...").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 12px;"
                            )
                        with ui.card().style(
                            f"width: 300px; height: 400px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Challenge Progress").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            ftmo_progress_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )

                    def update_ftmo():
                        """Read MT5 logs and update the FTMO tab (every 5s)."""
                        import glob
                        mt5_dir = os.path.expanduser(cfg.ftmo.mt5_dir)

                        # Check if MT5 is running
                        mt5_running = False
                        try:
                            import subprocess
                            result = subprocess.run(
                                ["pgrep", "-f", "terminal64.exe"],
                                capture_output=True, text=True,
                            )
                            mt5_running = result.returncode == 0
                        except Exception:
                            pass

                        if mt5_running:
                            ftmo_status_label.set_text("MT5 Status: RUNNING")
                            ftmo_status_label.style(replace=f"color: {GREEN}; font-weight: bold;")
                        else:
                            ftmo_status_label.set_text("MT5 Status: OFFLINE")
                            ftmo_status_label.style(replace=f"color: {RED}; font-weight: bold;")

                        log_dir = os.path.join(mt5_dir, "logs")
                        log_files = sorted(glob.glob(os.path.join(log_dir, "*.log")))
                        ea_lines = []
                        trade_entries = []

                        if log_files:
                            try:
                                with open(log_files[-1], "rb") as f:
                                    content = f.read().decode("utf-16-le", errors="ignore")
                                for line in content.split("\n"):
                                    line = line.strip()
                                    if not line or len(line) < 5:
                                        continue
                                    if any(k in line for k in [
                                        "Expert", "ENTRY SIGNAL", "SELL", "BUY",
                                        "Order", "Deal", "Trade", "profit",
                                        "error", "GoldDonch", "automat",
                                    ]):
                                        ea_lines.append(line)
                                    if "SELL opened" in line or "BUY opened" in line:
                                        trade_entries.append({
                                            "time": line[:12].strip(),
                                            "type": "SELL" if "SELL" in line else "BUY",
                                            "symbol": "XAUUSD",
                                            "lots": "6.0",
                                            "price": "--",
                                            "sl": "--",
                                            "tp": "--",
                                            "pnl": "--",
                                            "pnl_raw": 0,
                                            "comment": line[40:].strip() if len(line) > 40 else "",
                                        })
                            except Exception as e:
                                ea_lines = [f"Error reading log: {e}"]

                        if ea_lines:
                            ftmo_log_label.set_text("\n".join(ea_lines[-40:]))
                        else:
                            ftmo_log_label.set_text(
                                "No EA activity yet.\n"
                                "(MT5 log file at " + log_dir + ")"
                            )

                        ftmo_trade_table.rows = trade_entries if trade_entries else []
                        ftmo_trade_table.update()

                        ch = cfg.ftmo.challenge
                        ftmo_progress_label.set_text(
                            f"Account: £{ch['account_gbp']:,}\n"
                            f"Target:  £{ch['target_gbp']:,} "
                            f"({ch['target_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Max DD:  £{ch['max_dd_gbp']:,} "
                            f"({ch['max_dd_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Daily:   £{ch['daily_dd_gbp']:,} "
                            f"({ch['daily_dd_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Min days: {ch['min_days']}\n"
                            f"Period:  {ch['period_days']} days\n\n"
                            f"Lot size: {ch['lot_size']}\n"
                            f"Expected: £{ch['expected_pnl']:,} / {ch['period_days']}d\n"
                            f"Max DD:   £{ch['expected_dd']:,}\n\n"
                            f"Trades today: {len(trade_entries)}\n"
                            f"Status: {'Active' if mt5_running else 'OFFLINE'}"
                        )

                    ui.timer(5.0, update_ftmo)

              # ================ ARBITRAGE TAB ================
              if arb_tab is not None:
                with ui.tab_panel(arb_tab):
                    with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                        f"background-color: {BG_PANEL}; border-radius: 6px;"
                    ):
                        arb_status_label = ui.label("Arb Scanner: waiting for first scan…").style(
                            f"color: {YELLOW}; font-weight: bold;"
                        )
                        arb_summary_label = ui.label("").style(f"color: {TEXT_SECONDARY};")
                        arb_pnl_label = ui.label("").style(f"color: {TEXT_SECONDARY};")

                    ui.label("Profitable Opportunities").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    arb_opp_columns = [
                        {"name": "sport",      "label": "Sport",      "field": "sport",      "align": "left"},
                        {"name": "event",      "label": "Event",      "field": "event",      "align": "left"},
                        {"name": "market",     "label": "Market",     "field": "market",     "align": "left"},
                        {"name": "selection",  "label": "Selection",  "field": "selection",  "align": "left"},
                        {"name": "back",       "label": "Back @ Px",  "field": "back",       "align": "right"},
                        {"name": "lay",        "label": "Lay @ Px",   "field": "lay",        "align": "right"},
                        {"name": "net_margin", "label": "Net %",      "field": "net_margin", "align": "right"},
                        {"name": "pnl",        "label": "Sim P&L (£)","field": "pnl",        "align": "right"},
                        {"name": "confidence", "label": "Match %",    "field": "confidence", "align": "center"},
                    ]
                    arb_opp_table = ui.table(
                        columns=arb_opp_columns, rows=[], row_key="event",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    arb_opp_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)

                    with ui.row().classes("w-full gap-4 mt-4"):
                        with ui.card().classes("flex-grow").style(
                            f"min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Last Scan Stats").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_stats_label = ui.label("waiting…").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )
                        with ui.card().style(
                            f"width: 320px; min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Configuration").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_config_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )

                    def update_arb():
                        with dashboard._lock:
                            snap = dashboard._arb_scan
                        try:
                            with open(os.path.expanduser(cfg.arb.config_path)) as _f:
                                _cfg = json.load(_f).get("scan", {})
                            sports = ", ".join(s["name"] for s in _cfg.get("sports", []))
                            arb_config_label.set_text(
                                f"Sports:      {sports}\n"
                                f"Stake:       £{_cfg.get('stake', '?')}\n"
                                f"Min margin:  {_cfg.get('min_margin', 0) * 100:.1f}%\n"
                                f"Price depth: {_cfg.get('price_depth', '?')} levels\n"
                            )
                        except Exception:
                            arb_config_label.set_text("(config not readable)")

                        if snap is None:
                            arb_status_label.set_text(
                                "Arb Scanner: waiting for first scan…"
                            )
                            arb_status_label.style(
                                replace=f"color: {YELLOW}; font-weight: bold;"
                            )
                            arb_summary_label.set_text("")
                            arb_pnl_label.set_text("")
                            arb_opp_table.rows = []
                            arb_opp_table.update()
                            arb_stats_label.set_text("waiting…")
                            return

                        age = time.time() - snap.get("scan_completed_at", 0)
                        if age > 300:
                            col = RED
                            status_text = f"Arb Scanner: STALE (last scan {age:.0f}s ago)"
                        elif snap.get("profitable_count", 0) > 0:
                            col = GREEN
                            status_text = (
                                f"Arb Scanner: ACTIVE — "
                                f"{snap['profitable_count']} profitable opps"
                            )
                        else:
                            col = YELLOW
                            status_text = "Arb Scanner: ACTIVE — no opps right now"
                        arb_status_label.set_text(status_text)
                        arb_status_label.style(replace=f"color: {col}; font-weight: bold;")
                        arb_summary_label.set_text(
                            f"{snap.get('matched_pairs', 0)} matched pairs from "
                            f"{snap.get('matchbook_events', 0)} MB / "
                            f"{snap.get('betfair_events', 0)} BF events  ·  "
                            f"scan {snap.get('duration_secs', 0):.1f}s "
                            f"({age:.0f}s ago)"
                        )
                        pnl = snap.get("total_simulated_pnl", 0.0)
                        pnl_col = GREEN if pnl > 0 else (RED if pnl < 0 else TEXT_SECONDARY)
                        arb_pnl_label.set_text(f"Total sim P&L: £{pnl:+.2f}")
                        arb_pnl_label.style(replace=f"color: {pnl_col}; font-weight: bold;")

                        rows = []
                        for opp in snap.get("opportunities", []):
                            if opp.get("net_margin", 0) <= 0:
                                continue
                            rows.append({
                                "sport": opp.get("sport", "").title(),
                                "event": opp.get("event", ""),
                                "market": opp.get("market", ""),
                                "selection": opp.get("selection", ""),
                                "back": (
                                    f"{opp.get('back_odds', 0):.2f} "
                                    f"({opp.get('back_exchange', '')[:2].upper()})"
                                ),
                                "lay": (
                                    f"{opp.get('lay_odds', 0):.2f} "
                                    f"({opp.get('lay_exchange', '')[:2].upper()})"
                                ),
                                "net_margin": f"{opp.get('net_margin', 0) * 100:+.2f}%",
                                "pnl": f"£{opp.get('simulated_pnl', 0):+.2f}",
                                "pnl_raw": opp.get("simulated_pnl", 0),
                                "confidence": f"{opp.get('match_confidence', 0):.0f}",
                            })
                        arb_opp_table.rows = rows
                        arb_opp_table.update()

                        arb_stats_label.set_text(
                            f"Last completed:    {time.strftime('%H:%M:%S', time.localtime(snap.get('scan_completed_at', 0)))}\n"
                            f"Scan duration:     {snap.get('duration_secs', 0):.1f}s\n"
                            f"Matchbook events:  {snap.get('matchbook_events', 0)}\n"
                            f"Betfair events:    {snap.get('betfair_events', 0)}\n"
                            f"Matched pairs:     {snap.get('matched_pairs', 0)}\n"
                            f"All opportunities: {snap.get('opportunities_count', 0)}\n"
                            f"Profitable:        {snap.get('profitable_count', 0)}\n"
                            f"Best net margin:   {snap.get('best_margin', 0) * 100:+.2f}%\n"
                            f"Total sim P&L:     £{pnl:+.2f}"
                        )

                    ui.timer(2.0, update_arb)

            # ---- Periodic UI update — Console tab refresh ----
            # Skipped when Console tab is disabled.
            if console_tab is not None:
                def update_ui():
                    with dashboard._lock:
                        for info in dashboard._strategies.values():
                            if "_metric_age" in info:
                                info["_metric_age"] += 1

                        strategies = {k: dict(v) for k, v in dashboard._strategies.items()}
                        pnl_history = list(dashboard._pnl_history)
                        total_pnl = dashboard._total_pnl
                        trades = list(dashboard._trades)
                        alert = dashboard._alert
                        mode = dashboard._mode
                        dashboard._alert = ""
                        open_trades = dict(dashboard._open_trades)
                        tick_prices = dict(dashboard._tick_prices)
                        scenarios = {
                            k: {
                                "status": v.get("status"),
                                "broker": v.get("broker"),
                                "live": v.get("live"),
                                "pnl": v.get("pnl", 0.0),
                                "trades": v.get("trades", 0),
                                "wins": v.get("wins", 0),
                                "n_strategies": len(v.get("strategies") or {}),
                            }
                            for k, v in dashboard._scenarios.items()
                        }

                    now = time.strftime("%Y-%m-%d %H:%M:%S")

                    pnl_sign = "+" if total_pnl >= 0 else ""
                    pnl_colour = GREEN if total_pnl >= 0 else RED
                    mode_label.set_text(f"Mode: {mode.upper()}")
                    pnl_label.set_text(f"Daily P&L: {pnl_sign}{total_pnl:.2f}")
                    pnl_label.style(replace=f"color: {pnl_colour};")
                    strat_label.set_text(f"Strategies: {len(strategies)}")
                    clock_label.set_text(now)

                    alert_label.set_text(f"ALERT: {alert}" if alert else "")

                    # Sidecar apps (like the arb scanner) publish under
                    # their own strategy_id and have a dedicated tab —
                    # exclude them here so data isn't shown in two places.
                    new_rows = []
                    for sid, info in sorted(strategies.items()):
                        if sid == "arbitrage":
                            continue
                        status = info.get("status", "unknown")
                        hb_age = time.time() - info.get("last_heartbeat", 0)
                        if hb_age > 15 and status == "running":
                            status = "stale"
                        pnl = info.get("pnl", 0.0)
                        pnl_s = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
                        wins = info.get("wins", 0)
                        total_trades = info.get("trades", 0)
                        win_pct = (
                            f"{(wins / total_trades * 100):.0f}%"
                            if total_trades > 0 else "--"
                        )
                        last_metric = info.get("last_metric", "")
                        hb_activity = info.get("_heartbeat_activity", "")
                        display_text = last_metric if last_metric else hb_activity
                        new_rows.append({
                            "strategy": sid,
                            "status": status.upper(),
                            "pnl": pnl_s,
                            "pnl_raw": pnl,
                            "trades": f"{total_trades}/{wins}",
                            "win_pct": win_pct,
                            "last_signal": display_text,
                        })
                    strat_table.rows = new_rows
                    strat_table.update()

                    # Scenarios table — hidden in the legacy single-scenario case
                    scen_new_rows = []
                    show_scenarios = (
                        len(scenarios) > 1
                        or (len(scenarios) == 1 and "default" not in scenarios)
                    )
                    if show_scenarios:
                        for scen_id, sinfo in sorted(scenarios.items()):
                            spnl = sinfo.get("pnl", 0.0)
                            spnl_s = f"+{spnl:.2f}" if spnl >= 0 else f"{spnl:.2f}"
                            s_trades = sinfo.get("trades", 0)
                            s_wins = sinfo.get("wins", 0)
                            s_winpct = (
                                f"{(s_wins / s_trades * 100):.0f}%"
                                if s_trades > 0 else "--"
                            )
                            scen_new_rows.append({
                                "scenario": scen_id,
                                "broker": sinfo.get("broker") or "--",
                                "live": "yes" if sinfo.get("live") else "paper",
                                "status": sinfo.get("status") or "active",
                                "pnl": spnl_s,
                                "pnl_raw": spnl,
                                "trades": str(s_trades),
                                "win_pct": s_winpct,
                                "n_strategies": str(sinfo.get("n_strategies", 0)),
                            })
                    scen_table.rows = scen_new_rows
                    scen_table.update()

                    # Open trades
                    ot_new_rows = []
                    now_ts = time.time()
                    for (sid, sym), ot in sorted(open_trades.items()):
                        entry_px = ot.get("entry_price", 0)
                        sl = ot.get("stop_loss", 0)
                        tp = ot.get("take_profit", 0)
                        timeout_sec = ot.get("timeout_seconds", 0)
                        entry_ts = ot.get("entry_ts", 0)
                        direction = ot.get("side", "?").upper()
                        tp_data = tick_prices.get(sym)
                        if tp_data:
                            cur_price = (
                                tp_data["bid"] if ot.get("side") == "buy"
                                else tp_data["ask"]
                            )
                        else:
                            cur_price = 0
                        if entry_px > 50:
                            fmt = ".2f"
                        elif entry_px > 10:
                            fmt = ".3f"
                        else:
                            fmt = ".5f"
                        if ot.get("side") == "buy":
                            pnl_positive = cur_price >= entry_px
                        else:
                            pnl_positive = (
                                cur_price <= entry_px if cur_price > 0 else True
                            )
                        if timeout_sec > 0 and entry_ts > 0:
                            remaining = max(0, timeout_sec - (now_ts - entry_ts))
                            mins = int(remaining // 60)
                            secs = int(remaining % 60)
                            timeout_str = f"{mins}m {secs:02d}s"
                        else:
                            timeout_str = "--"
                        ot_new_rows.append({
                            "key": f"{sid}_{sym}",
                            "strategy": sid,
                            "pair": sym,
                            "direction": direction,
                            "entry_price": f"{entry_px:{fmt}}",
                            "current_price": (
                                f"{cur_price:{fmt}}" if cur_price > 0 else "--"
                            ),
                            "stop_loss": f"{sl:{fmt}}" if sl else "--",
                            "take_profit": f"{tp:{fmt}}" if tp else "--",
                            "timeout": timeout_str,
                            "pnl_positive": pnl_positive,
                        })
                    if not ot_new_rows:
                        ot_new_rows = [{
                            "key": "_empty",
                            "strategy": "--", "pair": "No open trades",
                            "direction": "--",
                            "entry_price": "--", "current_price": "--",
                            "stop_loss": "--", "take_profit": "--",
                            "timeout": "--", "pnl_positive": True,
                        }]
                    open_trades_table.rows = ot_new_rows
                    open_trades_table.update()

                    # P&L chart
                    if pnl_history:
                        xs = [p[0] for p in pnl_history]
                        ys = [p[1] for p in pnl_history]
                        chart_fig["data"][0]["x"] = xs
                        chart_fig["data"][0]["y"] = ys
                        pnl_chart.update()

                    # Trade log
                    if trades:
                        lines = []
                        for t in trades[:30]:
                            side_marker = "BUY " if t.get("side") == "buy" else "SELL"
                            lines.append(
                                f"{side_marker} {t.get('symbol', '?')} "
                                f"qty={t.get('quantity', 0)} @ {t.get('price', 0):.4f} "
                                f"[{t.get('strategy_id', '?')}]"
                            )
                        trade_log.set_text("\n".join(lines))

                ui.timer(1.0, update_ui)

    # ------------------------------------------------------------------ #
    # ZMQ subscriber threads
    # ------------------------------------------------------------------ #

    def _start_zmq(self):
        self._zmq_running = True
        self._zmq_thread = threading.Thread(
            target=self._zmq_loop, name="zmqgui-events", daemon=True,
        )
        self._zmq_thread.start()
        if self.cfg.feed_source:
            self._feed_thread = threading.Thread(
                target=self._feed_loop, name="zmqgui-feed", daemon=True,
            )
            self._feed_thread.start()

    def _zmq_loop(self):
        """Subscribe to every configured PUB endpoint and route messages.

        One SUB socket connects to N endpoints — ZMQ merges the streams
        into a single recv loop. Empty-string topic filter accepts every
        topic; we dispatch by topic prefix in `_process_message`.
        """
        bus = Bus()
        if not self.cfg.sources:
            logger.warning("No ZMQ sources configured — dashboard will be empty.")
            return
        sub = bus.subscriber(self.cfg.sources[0], [""])
        for endpoint in self.cfg.sources[1:]:
            sub.connect(endpoint)
            logger.info("SUB also connected to %s", endpoint)

        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)

        while self._zmq_running:
            events = dict(poller.poll(timeout=100))
            if sub not in events:
                continue
            try:
                msg = Bus.sub_recv(sub)
            except Exception:
                logger.debug("ZMQ recv error", exc_info=True)
                continue
            self._process_message(msg)

        sub.close()
        bus.close()

    def _feed_loop(self):
        """Separate SUB socket for the tick feed (open trades current prices)."""
        bus = Bus()
        sub = bus.subscriber(self.cfg.feed_source, ["tick."])
        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)

        while self._zmq_running:
            events = dict(poller.poll(timeout=200))
            if sub not in events:
                continue
            try:
                msg = Bus.sub_recv(sub)
            except Exception:
                continue
            topic = msg.get("topic", "")
            if topic.startswith("tick."):
                symbol = topic.split(".", 1)[1]
                with self._lock:
                    self._tick_prices[symbol] = {
                        "bid": msg.get("bid", 0),
                        "ask": msg.get("ask", 0),
                    }

        sub.close()
        bus.close()

    def _process_message(self, msg: dict):
        topic = msg.get("topic", "")
        if topic.startswith("fill."):
            self._on_fill(msg)
        elif topic.startswith("metric."):
            self._on_metric(msg)
        elif topic.startswith("heartbeat."):
            self._on_heartbeat(msg)
        elif topic == "command":
            self._on_command(msg)
        elif topic.startswith("arb."):
            self._on_arb_scan(msg)

    # -- handlers (ported verbatim from ModularTradeApp GUI) -----------

    def _on_arb_scan(self, msg: dict):
        """Whole-snapshot replacement from an arb scanner sidecar."""
        with self._lock:
            self._arb_scan = msg

    def _on_fill(self, msg: dict):
        sid = msg.get("strategy_id", "unknown")
        symbol = msg.get("symbol", "")
        side = msg.get("side", "")
        price = msg.get("price", 0)
        quantity = msg.get("quantity", 0)
        scen_id = msg.get("scenario_id") or "default"
        order_id = msg.get("order_id", "")
        dedup_key = (scen_id, order_id)
        key = (sid, symbol)

        with self._lock:
            if order_id and dedup_key in self._seen_fills_set:
                return
            if order_id:
                if len(self._seen_fills) >= self._seen_fills.maxlen:
                    oldest = self._seen_fills[0]
                    self._seen_fills_set.discard(oldest)
                self._seen_fills.append(dedup_key)
                self._seen_fills_set.add(dedup_key)

            self._trades.appendleft(msg)

            scen_info = self._scenarios.setdefault(scen_id, {
                "status": "active", "broker": None, "live": False,
                "pnl": 0.0, "trades": 0, "wins": 0, "strategies": {},
            })
            scen_info["trades"] += 1
            scen_strat = scen_info["strategies"].setdefault(sid, {
                "pnl": 0.0, "trades": 0, "wins": 0,
            })
            scen_strat["trades"] += 1

            info = self._strategies.setdefault(sid, {
                "status": "running", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })
            info["trades"] += 1
            info["last_heartbeat"] = time.time()

            existing = self._open_trades.get(key)
            if existing and existing["side"] != side:
                entry_px = existing.get("entry_price", 0)
                close_qty = min(quantity, existing.get("quantity", 0))
                if existing["side"] == "buy":
                    pnl = (price - entry_px) * close_qty
                else:
                    pnl = (entry_px - price) * close_qty
                info["pnl"] += pnl
                scen_info["pnl"] += pnl
                scen_strat["pnl"] += pnl
                if pnl > 0:
                    info["wins"] = info.get("wins", 0) + 1
                    scen_info["wins"] += 1
                    scen_strat["wins"] += 1
                self._total_pnl = sum(
                    s.get("pnl", 0) for s in self._strategies.values()
                )
                del self._open_trades[key]
            elif not existing:
                self._open_trades[key] = {
                    "strategy": sid, "symbol": symbol, "side": side,
                    "entry_price": price, "quantity": quantity,
                    "stop_loss": msg.get("stop_loss", 0),
                    "take_profit": msg.get("take_profit", 0),
                    "timeout_seconds": msg.get("timeout_seconds", 0),
                    "entry_ts": msg.get("timestamp", time.time()),
                }

            ts = msg.get("timestamp", time.time())
            self._pnl_history.append((ts, self._total_pnl))
            if len(self._pnl_history) > MAX_PNL_POINTS:
                self._pnl_history = self._pnl_history[-MAX_PNL_POINTS:]

    def _on_metric(self, msg: dict):
        sid = msg.get("strategy_id", "unknown")
        name = msg.get("name", "")
        value = msg.get("value", 0)

        with self._lock:
            info = self._strategies.setdefault(sid, {
                "status": "running", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })
            info["last_metric"] = f"{name}={value}"
            info["last_heartbeat"] = time.time()
            info["_metric_age"] = 0

            if name == "pnl":
                info["pnl"] = float(value)
                self._total_pnl = sum(
                    s.get("pnl", 0) for s in self._strategies.values()
                )

            severity = msg.get("severity", "")
            if severity in ("warn", "error", "critical"):
                self._alert = f"[{sid}] {msg.get('message', name)}"

    def _on_heartbeat(self, msg: dict):
        sid = msg.get("strategy_id", "unknown")
        with self._lock:
            info = self._strategies.setdefault(sid, {
                "status": "unknown", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })

            status = msg.get("status", "")
            if status:
                info["status"] = status
            info["last_heartbeat"] = time.time()

            mode = msg.get("mode", "")
            if mode:
                self._mode = mode

            # Authoritative scenario state from the publisher
            scen_exp = msg.get("scenario_exposure")
            if isinstance(scen_exp, dict):
                for scen_id, scen_data in (scen_exp.get("scenarios") or {}).items():
                    info_scen = self._scenarios.setdefault(scen_id, {
                        "status": "active", "broker": None, "live": False,
                        "pnl": 0.0, "trades": 0, "wins": 0, "strategies": {},
                    })
                    info_scen["status"] = scen_data.get("status", info_scen["status"])
                    info_scen["broker"] = scen_data.get("broker", info_scen["broker"])
                    info_scen["live"] = bool(scen_data.get("live", info_scen["live"]))

            # Authoritative open_positions snapshot (recovers a late-joining GUI)
            positions = msg.get("open_positions")
            if positions is not None:
                self._open_trades.clear()
                for pos in positions:
                    sid_pos = pos.get("strategy_id", "unknown")
                    sym = pos.get("symbol", "")
                    k = (sid_pos, sym)
                    self._open_trades[k] = {
                        "strategy": sid_pos, "symbol": sym,
                        "side": pos.get("side", ""),
                        "entry_price": pos.get("entry_price", 0),
                        "quantity": pos.get("quantity", 0),
                        "stop_loss": pos.get("stop_loss", 0),
                        "take_profit": pos.get("take_profit", 0),
                        "timeout_seconds": pos.get("timeout_seconds", 0),
                        "entry_ts": pos.get("entry_ts", 0),
                    }

                existing_keys = {
                    (t.get("strategy_id", ""), t.get("symbol", ""))
                    for t in self._trades
                }
                for pos in positions:
                    pk = (pos.get("strategy_id", ""), pos.get("symbol", ""))
                    if pk not in existing_keys:
                        self._trades.appendleft({
                            "strategy_id": pos.get("strategy_id", ""),
                            "symbol": pos.get("symbol", ""),
                            "side": pos.get("side", ""),
                            "price": pos.get("entry_price", 0),
                            "quantity": pos.get("quantity", 0),
                            "timestamp": pos.get("entry_ts", 0),
                        })

            # Show activity in the "Last Signal" column when no fresh metric
            activity = msg.get("activity", "")
            if activity:
                idle = msg.get("idle_seconds", 0)
                if idle > 5:
                    activity_display = f"{activity} (idle {idle:.0f}s)"
                else:
                    activity_display = activity
                if not info["last_metric"] or info.get("_metric_age", 999) > 10:
                    info["last_metric"] = activity_display
                info["_heartbeat_activity"] = activity_display

    def _on_command(self, msg: dict):
        action = msg.get("action", "")
        if action == "stop":
            logger.info("GUI received stop command")


# ---------------------------------------------------------------------- #
# CLI entry point
# ---------------------------------------------------------------------- #

def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="ZmqGui dashboard")
    parser.add_argument("--config", type=str, default=None,
                        help="Path to config.yaml (defaults kick in if omitted)")
    parser.add_argument("--web-port", type=int, default=None,
                        help="Override web_port from config")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.web_port:
        cfg.web_port = args.web_port

    Dashboard(cfg).run()


if __name__ == "__main__":
    main()
