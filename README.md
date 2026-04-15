# ZmqGui

A standalone NiceGUI dashboard that consumes any app publishing on
ZMQ PUB sockets, using the message envelope convention from
`ModularTradeApp` (topics `fill.*`, `metric.*`, `heartbeat.*`,
`arb.*`, `tick.*`). Like-for-like port of the
`ModularTradeApp/console/gui` dashboard, extracted so multiple apps
(trading orchestrator, Arbitrage scanner, anything future) can share
one dashboard without each carrying their own copy.

## Install

```bash
cd /home/jacob/ZmqGui
python3.11 -m venv .venv
.venv/bin/pip install -e .
```

## Run

```bash
cp config.example.yaml config.yaml
# edit config.yaml if your ZMQ ports differ
.venv/bin/zmq-gui --config config.yaml
# or
.venv/bin/python -m zmq_gui --config config.yaml
```

Visit <http://localhost:8080>.

## Architecture

The dashboard is a dumb subscriber: one background thread connects
to each configured PUB endpoint and routes messages to shared state
by topic prefix. The NiceGUI thread renders the state on a 1s timer.
No app-specific imports; nothing knows about plugins, orchestrators,
or scanners — only about the ZMQ envelope.

```
ModularTradeApp orchestrator ─ PUB 5552 ─┐
                                          ├─→ ZmqGui ─ HTTP :8080
Arbitrage scanner sidecar   ─ PUB 5562 ─┘
```

Kill ZmqGui, the publishers keep running. Start ZmqGui later, it
joins mid-stream. ZMQ's slow-joiner behaviour means you miss messages
sent before you connect — sources that snapshot full state on every
publish (like the arb scanner) recover instantly; sources that send
deltas (like fills) you pick up going forward.

## Adding a new publisher

Just add its PUB endpoint to `sources:` in `config.yaml`. If it uses
the same envelope (topic = `{kind}.{id}`, body is a JSON dict with
`strategy_id` or `type`), it appears automatically in the Console
tab's strategies panel. Custom tabs (like the Arbitrage tab) are
activated by matching a topic prefix (`arb.*`) and rendering from a
snapshot kept in state.

## Topic envelope

Each message is a two-frame ZMQ multipart:

  frame 0  topic bytes (e.g. `fill.ftmo-100k.gold-donchian`)
  frame 1  UTF-8 JSON body

The body must also contain a `topic` field so downstream routing
works even if the multipart frame is dropped. See
`ModularTradeApp/console/core/messaging.py:Bus.pub_send`.
