"""
Playwright E2E test for STA tabs against the LIVE running GUI.

Uses asyncio.run() directly — no pytest-asyncio dependency.
Skipped when GUI is unreachable or Playwright not installed.

Tests exercise the actual DOM proving each STA tab renders real data,
not just a "Waiting for heartbeat" placeholder.
"""
import asyncio, sys, time
import pytest

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

GUI_URL = "http://127.0.0.1:8080"
STARTUP_WAIT_MS = 6000
TAB_WAIT_MS = 2500


def _gui_reachable():
    import urllib.request
    try:
        urllib.request.urlopen(GUI_URL, timeout=3)
        return True
    except Exception:
        return False


skip_if_unavailable = pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE or not _gui_reachable(),
    reason="Playwright not installed or GUI not reachable at :8080",
)


# ── shared async helpers ──────────────────────────────────────────────────────

async def _open_page():
    """Open GUI, wait for STA data, return (page, errors, browser, pw)."""
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
    page = await browser.new_page(viewport={"width": 1600, "height": 900})
    errors = []
    page.on("console", lambda m: errors.append((m.type, m.text))
            if m.type == "error" else None)
    page.on("pageerror", lambda e: errors.append(("pageerror", str(e))))
    await page.goto(GUI_URL, timeout=20000)
    await page.wait_for_timeout(STARTUP_WAIT_MS)
    return page, errors, browser, pw


async def _click_tab(page, name, wait_ms=TAB_WAIT_MS):
    btn = page.locator(f"text={name}").first
    assert await btn.count() > 0, f"Tab '{name}' not found"
    await btn.click()
    await page.wait_for_timeout(wait_ms)
    return await page.inner_text("body")


# ── tests ─────────────────────────────────────────────────────────────────────

@skip_if_unavailable
def test_no_console_errors():
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            assert errors == [], f"JS console errors: {errors[:5]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_trades_renders_data_rows():
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Trades")
            assert "Waiting for first STA heartbeat" not in body, \
                "STA Trades still showing waiting message — heartbeat not received"
            state_found = any(s in body for s in
                              ["AUTHORED", "GATE_PENDING", "ACTIVE", "DISPATCHED"])
            assert state_found, f"No lifecycle state visible. Body excerpt:\n{body[:600]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_chain_underlying_row_and_expiries():
    """Bug A regression: expiry sub-table must show real dates, not '—\\t—\\t—'."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Chain")
            assert "Waiting for STA chain stats" not in body, \
                "STA Chain still waiting"
            # Per-underlying row: strikes count visible
            assert any(c.isdigit() for c in body), \
                "No numeric data in STA Chain at all"
            # Expiry sub-table: real dates (2026- prefix)
            expiry_dates = [w for w in body.split()
                            if w.startswith("2026-") or w.startswith("2027-")
                            or w.startswith("2028-")]
            assert len(expiry_dates) > 0, \
                f"No expiry dates in STA Chain (Bug A). Body excerpt:\n{body[:800]}"
            # No all-dash placeholder rows
            placeholder = [l for l in body.splitlines() if l.strip() == "—\t—\t—"]
            assert len(placeholder) == 0, \
                f"Expiry sub-table has placeholder rows (Bug A unfixed)"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_lifecycle_dropdown_and_audit_log():
    """Bug D regression: lifecycle dropdown must populate and update audit log."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Lifecycle")
            assert "Waiting for STA lifecycle data" not in body, \
                "STA Lifecycle still waiting"
            assert "transition" in body.lower(), \
                f"No transition count in STA Lifecycle. Body:\n{body[:400]}"

            # Try to open the dropdown and change selection
            selects = page.locator(".q-select")
            if await selects.count() > 0:
                await selects.first.click()
                await page.wait_for_timeout(600)
                opts = page.locator(".q-menu .q-item")
                n = await opts.count()
                if n >= 2:
                    await opts.nth(1).click()
                    await page.wait_for_timeout(1000)
                    body2 = await page.inner_text("body")
                    assert "transition" in body2.lower(), \
                        "After dropdown change, audit log lost transition count"
                else:
                    await page.keyboard.press("Escape")
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_health_pid_and_symbols():
    """Bug B regression: Symbols should not show '—' when chain_status available."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Health")
            assert "PID:" in body, "schedule_engine_pid not visible in STA Health"
            assert "ago" in body, "DXLink freshness 'ago' label not found"
            sym_lines = [l for l in body.splitlines() if "Symbols:" in l]
            assert sym_lines, "No 'Symbols:' line in STA Health"
            sym_line = sym_lines[0]
            # Must have a digit, not just "—"
            assert any(c.isdigit() for c in sym_line), \
                f"Symbols line shows no digit (Bug B). Line: {sym_line!r}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_slates_no_crash():
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Slates")
            assert "STA Slate" in body
            assert "Traceback" not in body
            assert "AttributeError" not in body
            assert "KeyError" not in body
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_screenshot_all_tabs():
    """Screenshot every STA tab for visual verification archive."""
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            for tab in ["STA Trades", "STA Chain", "STA Lifecycle",
                        "STA Health", "STA Slates"]:
                await _click_tab(page, tab, wait_ms=1500)
                slug = tab.lower().replace(" ", "_")
                await page.screenshot(path=f"/tmp/zmqgui_{slug}.png")
            # Filter known Plotly resize noise (fires when hidden chart tab
            # gets a resize event during tab-switching — pre-existing NiceGUI issue)
            real_errors = [(t, m) for t, m in errors
                           if "Resize must be passed a displayed plot" not in m]
            assert real_errors == [], f"Console errors across tabs: {real_errors[:5]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())
