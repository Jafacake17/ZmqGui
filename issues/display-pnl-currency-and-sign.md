# Display bug — P&L currency / sign / unit-label inconsistency

**Filed:** 2026-04-28
**Severity:** medium (operator-confusing, no trading-logic impact)
**Source incident:** Modular instance Apr 28 morning; operator believed a trade had lost £800; actual loss was £4.28 (200× over-read).

---

## ROOT CAUSE (most likely — operator confirmed pattern class)

The P&L computation is using `(exit_price - entry_price) × quantity` directly and treating the result as account-currency. For JPY-quoted instruments this gives the raw JPY notional value, which then renders as "£841" — off by a factor of ~155 (the instrument's quote rate against the account currency). Operator confirmed this matches their prior unit-conversion-factor bug class ("factor of 100 or 1000 — very common bug we've faced"). **The same bug class affected `effective_cost_panel` previously and was fixed in Bug #4 of the MTA tracker.**

The 841 JPY → "£800" misreading: 841/4.28 ≈ 196, very close to the USD_JPY rate (~155-160). The missing step is the divide-by-quote-rate pipeline that converts quote-currency P&L to account-currency P&L.

### Fix shape

```python
# Current (buggy)
pnl_account = (exit_price - entry_price) * quantity * side_sign  # in QUOTE currency, mislabelled

# Correct
pnl_quote_ccy = (exit_price - entry_price) * quantity * side_sign
pnl_account = pnl_quote_ccy / quote_to_account_fx_rate
```

For USD_JPY: `quote_to_account_fx_rate = USD_JPY` (if account=USD) or `USD_JPY * GBP_USD` (if account=GBP, divide by USD_JPY then divide by 1/GBP_USD = multiply by GBP_USD inverse).
For EUR_USD with account=USD: `quote_to_account_fx_rate = 1.0` (no conversion).
For EUR_USD with account=GBP: `quote_to_account_fx_rate = 1/GBP_USD ≈ 0.787`.
For XAU_USD with account=GBP: same as above, $-denominated quote, divide by GBP_USD.

The latest cross rate should come from the live tick feed for the relevant instrument (USD_JPY, GBP_USD, etc.). Fallback to a daily-snapshot if the feed for the cross is stale.

---

## Symptom

Operator viewing the ZmqGui Open/Closed trades tab observed a **USD_JPY SELL** trade with:
- entry price 159.16658, exit price 159.33500 (price up 0.169 JPY = +0.106%)
- Display shown to operator: `"+0.106%"` and (per operator's own quote) "£800 loss"
- Operator's mental math: "16 pips × £50/pip = £800" — assuming a 10-standard-lot position

Reality from `fills` table:
- **Position size: 5,000 units = 0.05 standard lot** (not 10 lots)
- Loss in JPY: 841 (= 0.16842 × 5000)
- Loss in USD at ~155 JPY/USD: $5.43
- Loss in GBP at ~1.27 USD/GBP: £4.28

The "£800" came from operator reading the raw 841 JPY value as if it were £.

---

## Root cause hypotheses (need verification by ZmqGui maintainer)

1. **P&L not converted to account currency.** Quote-currency P&L (JPY for USD_JPY, USD for EUR_USD, etc.) is shown raw. For JPY pairs, this gives a 155× over-statement when read as £/$.
2. **Sign-collapsed display.** `"+0.106%"` reflects the raw price-move percentage `(exit - entry) / entry` rather than trade-direction P&L. A SELL with price-up should show negative; a BUY with price-up should show positive. Currently both look identical.
3. **Currency unit label missing or ambiguous.** A bare numeric "841" can be misread as £, $, JPY, etc. depending on what column the user thinks they're looking at.

---

## Required fixes

1. **Always display P&L in account currency** (default £ or $, configurable per-account). Auto-convert from instrument quote currency via the latest FX rate from the relevant tick stream. JPY pairs divide by USD/JPY rate; cross-currency pairs convert via the appropriate cross.
2. **Sign-correct for trade direction.** Compute `pnl = (exit_price - entry_price) × qty` if BUY, `(entry_price - exit_price) × qty` if SELL. Show the resulting signed value, not the raw price-percent move.
3. **Currency label visible on every P&L cell.** Render as `£4.28` or `$5.43`, never bare `841`. If displaying multiple currencies in a list, the label is non-optional.
4. **(Optional, for power users) Tooltip with raw + converted view.** On hover/click of a P&L cell, show both the quote-currency raw value (`841 JPY`) and the account-currency converted value (`£4.28 @ 155 JPY/£`), plus the FX rate used. Helps verify the conversion.

---

## Test cases

To verify the fix works:

| symbol | side | entry | exit | qty | expected display |
|---|---|---|---|---|---|
| USD_JPY | SELL | 159.17 | 159.33 | 5000 | **−£4.28** (loss; SELL with price-up) |
| USD_JPY | BUY | 159.17 | 159.33 | 5000 | **+£4.28** (profit; BUY with price-up) |
| EUR_USD | SELL | 1.17 | 1.16 | 10000 | **+£78.74** (profit; SELL with price-down at 1.27 USD/£) |
| EUR_GBP | BUY | 0.866 | 0.870 | 10000 | **+£40.00** (profit; price up × qty, already in £ since GBP is quote) |
| XAU_USD | BUY | 3000 | 3010 | 30 | **+£236.22** (profit; $300 USD ÷ 1.27 = £236.22) |

The first two cases are the critical ones — same price move, opposite trade direction, must display opposite signs.

---

## Out of scope (separate workstreams)

- Trading logic / risk management is fine. Position sizing is correct (5000 units = 0.05 lot is the deliberate paper-trading micro-lot for FTMO scaling). No code changes needed in the orchestrator.
- The fills table (`store.py` in Modular) records the correct raw values. The bug is exclusively in the display layer.
- ZmqGui's "Open Trades" tab using running unrealised P&L should also adopt the same fix; same bug class.
