# ZmqGui — developer notes for Claude Code

## Run

```bash
source /home/jacob/ModularTradeApp/.venv/bin/activate
PYTHONPATH=/home/jacob/ZmqGui/src python -m zmq_gui --config /home/jacob/ZmqGui/config.yaml
```

Or via systemd: `systemctl --user start zmqgui`.

The parent orchestrator (Modular) can request a restart via cc-bus:
`mcp__cc-bus__send_to(target="zmqgui", body="restart")`.

## Repo layout

```
src/zmq_gui/
  app.py          — Dashboard class, all tabs, ZMQ threads (~4600 lines)
  bus.py          — Thin ZMQ SUB socket factory
  config.py       — GuiCfg dataclass + YAML loader
  theme.py        — CSS colour palette + layout constants
  __init__.py     — Public exports (Dashboard, Bus)
  __main__.py     — Entry point for python -m zmq_gui
  tabs/
    quick_screen.py — Quick Screen tab (ZMQ REQ/REP to tcp://127.0.0.1:5560)
zmqgui.service    — systemd-user unit file
config.yaml       — Local config (not committed)
config.example.yaml — Template
```

## State model

`Dashboard.__init__` owns all shared state. The ZMQ thread writes
under `self._lock`; the NiceGUI timer callbacks read under the same
lock (copied into locals first, then lock released before UI calls).

Key state dicts:

| Attribute | Updated by | Read by |
|---|---|---|
| `_strategies` | `_on_fill`, `_on_metric`, `_on_heartbeat` | Console `update_ui` |
| `_scenarios` | `_on_fill`, `_on_heartbeat` | Console `update_ui` |
| `_open_trades` | `_on_fill`, `_on_heartbeat` (open_positions) | Console `update_ui` |
| `_closed_trades` | `_on_fill`, `_seed_from_questdb` | Console `update_ui` |
| `_broker_spreads` | `_on_heartbeat` (broker_spreads field) | Console `update_ui` (broker cost table + spread chips) |
| `_tick_prices` | `_feed_loop` (tick.* from port 5550) | Console `update_ui` (open trade unrealised P&L) |
| `_arb_scans` | `_on_arb_scan` | Arb tab `update_arb/matches/dutch/trades` |
| `_vault_hooks` | `_on_vault_hook` | Vault tab `update_vault` |
| `_crypto_strategies/fills/chains` | `_on_crypto_heartbeat`, `_on_fill`, `_on_crypto_block` | Crypto tab `update_crypto` |
| `_econ_cal_ok_ts/alarm` | `_on_econ_calendar_alert` | Header `update_cal_badge` |
| `_mfe_mae_cache` | `_mfe_mae_loop` (QuestDB) | Console `update_ui` (geometry column) |
| `_margin_summary`, `_agg_bps` | `_on_heartbeat` | Console `update_ui` |

## Cost model (PR-5 unification, 2026-05-13)

`_broker_profiles` and `_calc_commission_bps` were removed. ZmqGui no
longer parses YAML cost files. `total_bps` and breakdown components
(`spread_bps`, `slippage_bps_rt`, `commission_bps_rt`,
`flat_bps_baseline_qty`) come directly from the orchestrator heartbeat
field `broker_spreads[<data_source>][<symbol>]`. Single source of
truth: `cost_model.cost_per_roundtrip_structural_bps` in the orch.

## Dashboard tabs

Seven tabs; all toggle-able via `config.yaml tabs:` section.

- **Console** — live trading view: scenarios, broker cost (8 pairs ×
  OANDA/Duka/IBKR), account margin tile, strategy conditions/spread
  chips table, open trades (bps + P&L), P&L chart, past trades with
  geometry column (MFE:TP:Act:SL:MAE in bps).
- **FTMO MT5** — reads MetaTrader 5 log files; shows EA log tail and
  FTMO challenge progress card. 5s timer.
- **Arbitrage** — sports arb scanner: profitable back/lay arbs,
  matched pairs, dutch arbs, trade ledger. 2s timers.
- **Vault** — KnowledgeVault repo-hook heartbeats (one row per repo).
- **Crypto** — on-chain paper-trader: strategy heartbeats, chain block
  ticks, recent fill stream.
- **Vuln** — adverse intelligence search; queries QuestDB
  `adverse_intel` table via psycopg2. On-demand (button click).
- **Quick Screen** — ad-hoc backtest builder; ZMQ REQ/REP to the
  Quick Screen service on tcp://127.0.0.1:5560. Catalogue fetched
  on tab init; results cached per session.

## ZMQ port map

| Port | Publisher | Topics |
|---|---|---|
| 5550 | Tick feed (OANDA/Duka) | `tick.<symbol>` |
| 5552 | ModularTradeApp orchestrator | `fill.*`, `metric.*`, `heartbeat.*`, `command` |
| 5560 | Quick Screen service | REQ/REP only |
| 5562 | Arbitrage scanner (PM) | `arb.scan`, `heartbeat.arbitrage`, `balance.arbitrage` |
| 5567 | Econ-calendar health | `alert.econ_calendar.*` |

## Invariants and gotchas

- **Dedup:** fills are deduped by `(scenario_id, order_id)` to handle
  dual-publishing during the scenarios transition. Bounded by
  `MAX_FILL_DEDUP = 4096`.
- **Sidecar exclusion:** `_SIDECAR_SIDS = {"arbitrage", "arbitrage_live"}`
  are excluded from the Console P&L total — their P&L lives on the Arb tab.
- **P&L conversion:** `_to_gbp()` converts quote-currency P&L to GBP.
  JPY pairs divide by `GBP_USD × USD_JPY`; USD pairs divide by
  `GBP_USD`. Falls back to 1.0 when feed rates are absent.
- **NiceGUI timer race patch:** `_NgTimer._get_context` is monkey-patched
  at import time to catch the `parent_slot` weakref-dead RuntimeError
  that fires when a short-lived health-check session tears down.
- **Tab persistence:** active tab is saved per-browser via
  `app.storage.user["active_tab"]`; restored on reload.
- **Calendar badge:** header badge shows green when a `.ok` heartbeat
  arrived within the last hour (3600s TTL). Persisted to
  `~/.config/zmqgui/econ_cal_state.json` so the green state survives
  a restart within 24h.

## GUI fix testing requirement

Operator rule: "GUI fixes must be tested with browser/Playwright/code-path
proof — heartbeat payload + curl-grep is NOT sufficient."

Any change that touches a live UI element must be verified in a browser
session. The `zmqgui-dev.log` file captures stdout from dev runs.
