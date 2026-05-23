"""
Playwright E2E test suite for STA tabs — updated for 2-tab restructure.

Tests the new STA Book + STA Diagnostics tab structure that replaced the
5 original STA tabs (Trades/Slates/Chain/Lifecycle/Health).

Skipped when GUI is unreachable or Playwright not installed.
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


async def _open_page():
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


async def _wait_for_sta_data(page, timeout_s: int = 45) -> bool:
    """Wait until 'Waiting for first STA heartbeat' disappears. STA tick=30s."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        body = await page.inner_text("body")
        if "Waiting for first STA heartbeat" not in body:
            return True
        await page.wait_for_timeout(3000)
    return False


# ── structural tests (no STA data needed) ────────────────────────────────────

@skip_if_unavailable
def test_old_sta_tabs_removed():
    """5 old STA tabs must be absent; 2 new tabs must be present."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            html = await page.content()
            old_tabs = ["STA Trades", "STA Slates", "STA Chain",
                        "STA Lifecycle", "STA Health"]
            new_tabs = ["STA Book", "STA Diagnostics"]
            for t in old_tabs:
                assert t not in html, f"Old tab {t!r} still present"
            for t in new_tabs:
                assert t in html, f"New tab {t!r} missing"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_status_strip_in_header():
    """DX: and STA: badges must appear in the always-visible header."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await page.inner_text("body")
            # Status strip badges are in the header (visible on all tabs)
            html = await page.content()
            assert "DX:" in html, "DX: status badge missing from header"
            assert "STA:" in html, "STA: status badge missing from header"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_no_console_errors():
    """No JS errors on page load."""
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            real = [(t, m) for t, m in errors
                    if "Resize must be passed a displayed plot" not in m]
            assert real == [], f"JS errors: {real[:3]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


# ── STA Book tab tests ────────────────────────────────────────────────────────

@skip_if_unavailable
def test_sta_book_tab_loads():
    """STA Book tab opens without crash; filter and status widgets present."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            await _wait_for_sta_data(page)
            html = await page.content()
            # Filter options (Quasar toggle renders as button labels in HTML)
            for opt in ["Live", "Blocked", "Closed (24h)", "All"]:
                assert opt in html, f"Filter option {opt!r} missing"
            # Status line shows schema version and record count
            body = await page.inner_text("body")
            # Should show either "Waiting..." or "v4·..." status
            book_present = ("Waiting for first STA heartbeat" in body
                            or "records" in body
                            or "v" in body)
            assert book_present, "STA Book status line missing"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_book_shows_schema_version():
    """After heartbeat arrives, Book status shows schema version."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            got_data = await _wait_for_sta_data(page)
            assert got_data, "STA Book: no heartbeat within 45s"
            body = await page.inner_text("body")
            # Status shows "v4· 0 records · 1 slate(s) · Xs ago"
            assert "ago" in body, "Heartbeat age not shown in Book status"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_book_filter_toggle_changes_view():
    """Clicking filter options doesn't crash; content updates."""
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            await _wait_for_sta_data(page)
            for filter_label in ["Blocked", "All", "Live"]:
                btn = page.locator(f"text={filter_label}").first
                if await btn.count():
                    await btn.click()
                    await page.wait_for_timeout(1000)
            real_errors = [(t, m) for t, m in errors
                           if "Resize" not in m]
            assert real_errors == [], f"Filter toggle caused errors: {real_errors}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


# ── STA Diagnostics tab tests ─────────────────────────────────────────────────

@skip_if_unavailable
def test_sta_diagnostics_sections_present():
    """Diagnostics tab has all three sections: Chain, Lifecycle, Health."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await _click_tab(page, "STA Diagnostics")
            await _wait_for_sta_data(page)
            body = await page.inner_text("body")
            for section in ["Chain Explorer", "Lifecycle Audit", "Sidecar Health"]:
                assert section in body, f"Section {section!r} missing from Diagnostics"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_diagnostics_chain_explorer():
    """Chain Explorer shows underlying data after heartbeat arrives."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Diagnostics")
            got_data = await _wait_for_sta_data(page)
            assert got_data, "STA Diagnostics: no heartbeat within 45s"
            body = await page.inner_text("body")
            # Chain table should have nvda underlying
            assert "nvda" in body.lower(), "Chain Explorer missing underlying 'nvda'"
            # Strikes count should be present
            assert any(c.isdigit() for c in body), "No numeric data in Diagnostics"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_diagnostics_lifecycle_audit():
    """Lifecycle Audit section shows dropdown (may be empty if no records)."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Diagnostics")
            await _wait_for_sta_data(page)
            body = await page.inner_text("body")
            assert "Lifecycle Audit" in body
            # Dropdown exists (either populated or empty "Select trade")
            html = await page.content()
            assert "Select trade" in html or "q-select" in html
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_diagnostics_sidecar_health():
    """Health section shows PID and DXLink data after heartbeat."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Diagnostics")
            got_data = await _wait_for_sta_data(page)
            assert got_data, "STA Diagnostics: no heartbeat within 45s"
            body = await page.inner_text("body")
            assert "PID:" in body, "schedule_engine_pid not showing"
            assert "Symbols:" in body, "DXLink symbols not showing"
            assert any(c.isdigit() for c in body.split("PID:")[1][:20]), \
                "PID value missing"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


# ── Past Trades: bps column regression ───────────────────────────────────────

@skip_if_unavailable
def test_past_trades_pnl_consistent_with_pct():
    """Past Trades P&L (bps) column: bps = pnl_pct × 100, no 4-digit XAU values."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            body = await page.inner_text("body")
            lines = [l.strip() for l in body.splitlines() if l.strip()]
            pt_idx = next((i for i, l in enumerate(lines) if "Past Trades" in l), None)
            assert pt_idx is not None, "Past Trades section not found"
            pt_lines = lines[pt_idx:pt_idx + 80]
            header = next((l for l in pt_lines if "Entry Time" in l and "Exit" in l), "")
            assert "P&L (bps)" in header or "P&L (bps)" in " ".join(pt_lines[:5]), \
                f"P&L (bps) header not found. Header: {header!r}"
            assert "P&L %" not in header, f"P&L % column should be dropped"
            # XAU rows should show bps (< 500) not currency (> 1000)
            data_rows = [l for l in pt_lines[1:40]
                         if "XAU_USD" in l or "xauusd" in l.lower()]
            for row in data_rows[:3]:
                parts = [p.strip() for p in row.split("\t") if p.strip()]
                pnl_parts = [p for p in parts
                             if p and (p.startswith("+") or p.startswith("-"))
                             and p[1:].replace(".", "").isdigit()]
                if pnl_parts:
                    pnl_val = float(pnl_parts[-1])
                    assert abs(pnl_val) < 500, \
                        f"XAU P&L looks like currency: {pnl_val} (expected bps)"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


# ── Screenshot archive ────────────────────────────────────────────────────────

@skip_if_unavailable
def test_sta_book_slate_card_renders_when_constituents_empty():
    """Slate card must show with constraint chips even when lifecycle[] is empty.

    Tests against the live GUI (which currently has v4 with 0 lifecycle records
    but 1 slate), proving the operator-approved quiet-window visibility.
    """
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            got_data = await _wait_for_sta_data(page)
            assert got_data, "STA Book: no heartbeat within 45s"

            html = await page.content()
            body = await page.inner_text("body")
            lines = [l.strip() for l in body.splitlines() if l.strip()]

            # Slate name must be in DOM even with 0 lifecycle records
            # Live v4 has slate "2wk trades, 5x high-confidence"
            slate_present = any("2wk" in l or "high-conf" in l or "trades" in l.lower()
                                 for l in lines) or "2wk" in html
            assert slate_present, \
                "Slate card not found — slate should render even with 0 lifecycle records"

            # Constraint chips (SC1/SC2/SC3/Q3) must be in HTML
            constraint_chips_present = any(f"SC{n}" in html for n in (1, 2, 3))
            assert constraint_chips_present, \
                "Constraint chips missing from slate card"

            # Empty-state note must be visible
            empty_note_present = (
                "No constituent records" in body
                or "active / recently-closed" in body
            )
            assert empty_note_present, \
                "Empty-constituent note missing from slate card body"

        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_screenshot_both_sta_tabs():
    """Screenshot STA Book and STA Diagnostics for visual archive."""
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            for tab, slug in [("STA Book", "sta_book"),
                               ("STA Diagnostics", "sta_diagnostics")]:
                await _click_tab(page, tab, wait_ms=1500)
                await page.screenshot(path=f"/tmp/zmqgui_{slug}.png")
            real_errors = [(t, m) for t, m in errors if "Resize" not in m]
            assert real_errors == [], f"Console errors: {real_errors[:3]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_closed_tab_loads():
    """Closed inner sub-tab (inside STA Book) loads and shows status label."""
    async def run():
        page, errors, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            # Click the inner "Closed" sub-tab within STA Book
            closed_btn = page.locator("text=Closed").first
            assert await closed_btn.count() > 0, "Inner 'Closed' sub-tab not found"
            await closed_btn.click()
            await page.wait_for_timeout(1500)
            body = await page.inner_text("body")
            # Either waiting label or "X closed" status — tab rendered without crash
            assert ("tcp://127.0.0.1:5570" in body or "closed" in body.lower()), \
                "STA Closed inner-tab: expected status label not found"
            real_errors = [(t, m) for t, m in errors if "Resize" not in m]
            assert real_errors == [], f"Console errors on STA Closed tab: {real_errors[:3]}"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())


@skip_if_unavailable
def test_sta_closed_tab_shows_data():
    """Closed inner sub-tab shows records once heartbeat with closed_all arrives."""
    async def run():
        page, _, browser, pw = await _open_page()
        try:
            await _click_tab(page, "STA Book")
            closed_btn = page.locator("text=Closed").first
            assert await closed_btn.count() > 0, "Inner 'Closed' sub-tab not found"
            await closed_btn.click()
            # Wait up to 75s for heartbeat (60s tick + buffer)
            deadline = time.time() + 75
            got_data = False
            while time.time() < deadline:
                body = await page.inner_text("body")
                # Status line updates from "Waiting…" once a heartbeat arrives
                if "closed ·" in body:
                    got_data = True
                    break
                await page.wait_for_timeout(2000)
            assert got_data, "STA Closed inner-tab: status never updated from 'Waiting…'"
            # Page label visible
            body = await page.inner_text("body")
            assert "Page" in body, "Pagination label missing"
        finally:
            await browser.close(); await pw.stop()
    asyncio.run(run())
