# ZmqGui

A standalone NiceGUI dashboard that consumes any app publishing on
ZMQ PUB sockets, using the message envelope convention from
`ModularTradeApp` (topics `fill.*`, `metric.*`, `heartbeat.*`,
`arb.*`, `tick.*`, `block.*`, `balance.*`, `alert.econ_calendar.*`).
Like-for-like port of the `ModularTradeApp/console/gui` dashboard,
extracted so multiple apps can share one dashboard without each
carrying their own copy.

## Install

```bash
cd /home/jacob/ZmqGui
python3.11 -m venv .venv
.venv/bin/pip install -e .
```

## Run

```bash
cp config.example.yaml config.yaml
# edit config.yaml if your ZMQ ports or tab preferences differ
.venv/bin/zmq-gui --config config.yaml
# or
.venv/bin/python -m zmq_gui --config config.yaml
```

Visit <http://localhost:8080>.

## systemd

```bash
cp zmqgui.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now zmqgui.service
journalctl --user -u zmqgui -f
```

## Architecture

The dashboard is a dumb subscriber: one background thread connects
to each configured PUB endpoint and routes messages to shared state
by topic prefix. The NiceGUI thread renders the state on 1–5s timers
per tab. No app-specific imports — only the ZMQ envelope.

```
ModularTradeApp orchestrator ─ PUB 5552 ─┐
Arbitrage scanner (PM)       ─ PUB 5562 ─┤
Econ-calendar health service ─ PUB 5567 ─┴→ ZmqGui ─ HTTP :8080
Tick feed (OANDA/Duka)       ─ PUB 5550 ─  (separate feed thread)
```

Background threads started by `Dashboard.run()`:

| Thread | Purpose |
|---|---|
| `zmqgui-events` | SUB on all `sources` endpoints; routes by topic |
| `zmqgui-feed` | SUB on `feed_source`; updates `_tick_prices` for open-trade P&L |
| `zmqgui-watchdog` | Samples `_zmq_ticks` every 60s; escalates to ERROR if wedged |
| `zmqgui-mfemae` | Drains MFE/MAE queue; psycopg2 queries on QuestDB per closed trade |

## Tabs

| Tab | Key | What it shows |
|---|---|---|
| **Console** | `console` | Strategy table with conditions/filters/spread chips; open trades with live bps + P&L; closed trades with MFE:TP:Act:SL:MAE geometry; cumulative P&L chart; Broker Effective Costs (RT bps) |
| **FTMO MT5** | `ftmo` | MetaTrader 5 EA log tail + FTMO challenge progress card |
| **Arbitrage** | `arb` | Profitable arbs (back/lay + dutch); matched pairs diagnostics; trade ledger |
| **Vault** | `vault` | KnowledgeVault git repo-hook heartbeats per repo |
| **Crypto** | `crypto` | On-chain paper-trader: strategies, chain block ticks, recent fills |
| **Vuln** | `vuln` | Adverse intelligence search over QuestDB `adverse_intel` table |
| **Quick Screen** | `quick_screen` | Ad-hoc backtest builder over the Quick Screen RPC service (tcp://127.0.0.1:5560) |

## Topic envelope

Each message is a two-frame ZMQ multipart:

```
frame 0  topic bytes (e.g. "fill.ftmo-100k.gold-donchian")
frame 1  UTF-8 JSON body
```

The body must also carry a `topic` field for downstream routing (see
`Bus.sub_recv`). Standard topics:

| Topic prefix | Routed to |
|---|---|
| `fill.*` | `_on_fill` — updates positions, P&L, closed-trade deque |
| `metric.*` | `_on_metric` — updates last_signal, alerts |
| `heartbeat.*` | `_on_heartbeat` — updates strategy status, spread chips, margin |
| `heartbeat.vault-repo-hook` | `_on_vault_hook` — Vault tab |
| `heartbeat.flash-arb-*` / `heartbeat.triangular-arb-*` | `_on_crypto_heartbeat` |
| `arb.*` | `_on_arb_scan` — Arbitrage tab snapshot |
| `balance.arbitrage` | `_on_balance_arbitrage` — exchange balance panel |
| `block.*` | `_on_crypto_block` — Crypto chain block ticks |
| `alert.econ_calendar.*` | `_on_econ_calendar_alert` — header Calendar badge |
| `command` | `_on_command` — stop/reload signals |

## Cost model (PR-5, 2026-05-13)

The Broker Effective Costs panel reads `total_bps` and per-component
breakdown (`spread_bps`, `slippage_bps_rt`, `commission_bps_rt`,
`flat_bps_baseline_qty`) directly from the orchestrator heartbeat's
`broker_spreads[<data_source>][<instrument>]` fields. ZmqGui does not
parse YAML cost files or compute commission — single source of truth
is `cost_model.cost_per_roundtrip_structural_bps` in the orchestrator.

`total_bps` = structural RT cost (spread + 2×slippage + 2×per-unit commission).
Size-invariant. Flat fees show as a tooltip annotation at baseline qty=1000.

The cheapest broker per row is highlighted in green.

## QuestDB integration

On startup `_seed_from_questdb` pulls recent fills from
`localhost:8812` (QuestDB PG wire) to pre-populate the Past Trades
table. The MFE/MAE background thread queries `ticks_<symbol>` for
each closed trade to compute max-favourable / max-adverse excursion in
bps. The Vuln tab and the chart routes (`/chart/<trade_id>`) also use
QuestDB. All QuestDB dependencies are soft — the dashboard starts
cleanly when QuestDB is unreachable.

## Adding a new publisher

Add its PUB endpoint to `sources:` in `config.yaml`. If it uses the
standard envelope (`fill.*` / `metric.*` / `heartbeat.*`) it appears
automatically in the Console tab. Custom topics need a new handler in
`_process_message`.
