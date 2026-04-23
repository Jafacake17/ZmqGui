"""
Config loading for ZmqGui.

YAML → a small dataclass with every knob the dashboard reads. Defaults
match the ModularTradeApp/Arbitrage sibling layout on this machine.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class FtmoCfg:
    enabled: bool = True
    mt5_dir: str = "~/.wine/drive_c/Program Files/MetaTrader 5"
    account_summary: str = "Account: -- | -- | --"
    ea_summary: str = "EA: -- | -- | --"
    challenge: dict = field(default_factory=lambda: {
        "account_gbp":  140000,
        "target_gbp":   7000,
        "max_dd_gbp":   14000,
        "daily_dd_gbp": 7000,
        "min_days":     2,
        "period_days":  14,
        "lot_size":     6.0,
        "expected_pnl": 7573,
        "expected_dd":  4047,
    })


@dataclass
class ArbCfg:
    enabled: bool = True
    config_path: str = "~/Arbitrage/config.json"


@dataclass
class GuiCfg:
    web_port: int = 8080
    sources: list[str] = field(default_factory=lambda: [
        "tcp://127.0.0.1:5552",
        "tcp://127.0.0.1:5562",
    ])
    feed_source: str | None = "tcp://127.0.0.1:5550"
    tabs: dict[str, bool] = field(default_factory=lambda: {
        "console": True, "ftmo": True, "arb": True, "vault": True,
    })
    ftmo: FtmoCfg = field(default_factory=FtmoCfg)
    arb: ArbCfg = field(default_factory=ArbCfg)


def load(path: str | None) -> GuiCfg:
    """Read a config.yaml and return a GuiCfg, or pure defaults if path is None.

    Missing keys fall back to defaults — so a minimal `config.yaml`
    with just `web_port: 9000` still works.
    """
    if not path:
        return GuiCfg()
    with open(os.path.expanduser(path)) as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    cfg = GuiCfg()
    cfg.web_port = int(raw.get("web_port", cfg.web_port))
    if "sources" in raw:
        cfg.sources = list(raw["sources"])
    if "feed_source" in raw:
        cfg.feed_source = raw["feed_source"]
    if "tabs" in raw:
        cfg.tabs.update(raw["tabs"])

    ftmo_raw = raw.get("ftmo") or {}
    cfg.ftmo.enabled = cfg.tabs.get("ftmo", True)
    cfg.ftmo.mt5_dir = ftmo_raw.get("mt5_dir", cfg.ftmo.mt5_dir)
    cfg.ftmo.account_summary = ftmo_raw.get("account_summary", cfg.ftmo.account_summary)
    cfg.ftmo.ea_summary = ftmo_raw.get("ea_summary", cfg.ftmo.ea_summary)
    if "challenge" in ftmo_raw:
        cfg.ftmo.challenge.update(ftmo_raw["challenge"])

    arb_raw = raw.get("arb") or {}
    cfg.arb.enabled = cfg.tabs.get("arb", True)
    cfg.arb.config_path = arb_raw.get("config_path", cfg.arb.config_path)

    return cfg
