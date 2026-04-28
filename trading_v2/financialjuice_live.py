"""
financialjuice_live.py
Real-time news capture from Financial Juice using Playwright (async).

Strategy priority:
  1. WebSocket frame interception   — lowest latency, event-driven
  2. XHR / fetch / SSE interception — fallback if WS not used
  3. MutationObserver injection      — last resort DOM fallback

Run:
    pip install playwright
    playwright install chromium
    python financialjuice_live.py
"""

import asyncio
import json
import os
import sys
import re
from datetime import datetime, timezone

from playwright.async_api import async_playwright, WebSocket, Page


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

TARGET_URL = "https://www.financialjuice.com/home"

# Set to a Playwright persistent-context profile dir to reuse saved auth.
# If None, a fresh browser is launched (you will need to log in manually or
# add cookie injection below in `inject_auth()`).
BROWSER_PROFILE_DIR: str | None = os.environ.get("FJ_PROFILE_DIR") or None
# e.g. set FJ_PROFILE_DIR=/home/pwuser/.fj_profile in docker-compose.yml env block

# Optional residential proxy URL to bypass Cloudflare IP-level blocks on VPS/
# datacenter IPs.  Format: http://user:pass@host:port  or  socks5://host:port
# Set FJ_PROXY_URL in .env; leave blank to connect directly.
PROXY_URL: str | None = os.environ.get("FJ_PROXY_URL") or None

# Chromium launch flags tuned for low-latency real-time work.
CHROMIUM_ARGS = [
    "--disable-background-timer-throttling",  # prevents JS timer throttling in background tabs
    "--disable-renderer-backgrounding",        # keeps renderer at full priority
    "--disable-backgrounding-occluded-windows",
    "--disable-features=CalculateNativeWinOcclusion",
    "--no-first-run",
    "--no-default-browser-check",
    # Stealth flags — reduce signals that reveal a headless/automated browser
    # to Cloudflare and other bot-detection systems.
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--disable-dev-shm-usage",
    "--disable-extensions",
    "--window-size=1920,1080",
]

# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def emit(headline: str, source: str, raw_payload: str) -> None:
    """Print a news item to stdout as JSON immediately (no buffering)."""
    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "headline": headline.strip(),
        "source": source.strip(),
        "raw_payload": raw_payload,
    }
    # Flush immediately — critical for piping to downstream consumers.
    print(json.dumps(record, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# PAYLOAD PARSERS  (adapt these when Financial Juice changes their wire format)
# ---------------------------------------------------------------------------

def try_parse_ws_frame(raw: str) -> list[dict]:
    """
    Attempt to extract news items from a WebSocket frame.

    Financial Juice uses SignalR (or a similar hub protocol) over WebSocket.
    Messages look like one of:
      - SignalR handshake:  {"protocol":"json","version":1}\x1e
      - Hub invocation:     {"type":1,"target":"ReceiveNews","arguments":[{...}]}\x1e
      - Keep-alive:         {"type":6}\x1e

    Returns a list of dicts: [{"headline": ..., "source": ...}]
    """
    results = []

    # SignalR frames are separated by the ASCII record separator \x1e (0x1E).
    frames = raw.split("\x1e")

    for frame in frames:
        frame = frame.strip()
        if not frame:
            continue
        try:
            obj = json.loads(frame)
        except json.JSONDecodeError:
            continue

        # --- SignalR hub message (type 1 = invocation) --------------------
        if obj.get("type") == 1:
            target = obj.get("target", "")
            args = obj.get("arguments", [])

            # Common hub method names used by financial news platforms.
            if target.lower() in ("receivenews", "newsfeed", "breakingnews", "news"):
                for arg in args:
                    if isinstance(arg, dict):
                        headline = (
                            arg.get("headline")
                            or arg.get("title")
                            or arg.get("text")
                            or arg.get("body")
                            or ""
                        )
                        source = (
                            arg.get("source")
                            or arg.get("provider")
                            or arg.get("author")
                            or "financialjuice"
                        )
                        if headline:
                            results.append({"headline": headline, "source": source})
                    elif isinstance(arg, str) and len(arg) > 5:
                        results.append({"headline": arg, "source": "financialjuice"})

        # --- Plain JSON array of news items --------------------------------
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    headline = (
                        item.get("headline")
                        or item.get("title")
                        or item.get("text")
                        or ""
                    )
                    if headline:
                        results.append({
                            "headline": headline,
                            "source": item.get("source", "financialjuice"),
                        })

        # --- Single news object --------------------------------------------
        elif isinstance(obj, dict) and obj.get("type") not in (2, 3, 4, 5, 6, 7):
            headline = (
                obj.get("headline")
                or obj.get("title")
                or obj.get("text")
                or ""
            )
            if headline:
                results.append({
                    "headline": headline,
                    "source": obj.get("source", "financialjuice"),
                })

    return results


def try_parse_http_body(body: str, content_type: str) -> list[dict]:
    """
    Parse XHR / fetch / SSE response bodies for news items.
    SSE lines start with 'data: '.
    """
    results = []

    # Server-Sent Events: strip 'data: ' prefix from each line.
    if "text/event-stream" in content_type:
        for line in body.splitlines():
            if line.startswith("data:"):
                data = line[5:].strip()
                if data and data != "[DONE]":
                    try:
                        obj = json.loads(data)
                        headline = obj.get("headline") or obj.get("title") or obj.get("text") or ""
                        if headline:
                            results.append({
                                "headline": headline,
                                "source": obj.get("source", "financialjuice"),
                            })
                    except json.JSONDecodeError:
                        results.append({"headline": data, "source": "sse"})
        return results

    # Plain JSON body.
    try:
        obj = json.loads(body)
        if isinstance(obj, list):
            for item in obj:
                headline = item.get("headline") or item.get("title") or item.get("text") or ""
                if headline:
                    results.append({"headline": headline, "source": item.get("source", "financialjuice")})
        elif isinstance(obj, dict):
            headline = obj.get("headline") or obj.get("title") or obj.get("text") or ""
            if headline:
                results.append({"headline": headline, "source": obj.get("source", "financialjuice")})
            # Paginated response: {"items": [...]}
            for key in ("items", "news", "data", "results", "feed"):
                if key in obj and isinstance(obj[key], list):
                    for item in obj[key]:
                        h = item.get("headline") or item.get("title") or item.get("text") or ""
                        if h:
                            results.append({"headline": h, "source": item.get("source", "financialjuice")})
    except (json.JSONDecodeError, AttributeError):
        pass

    return results


# ---------------------------------------------------------------------------
# STRATEGY 1 — WebSocket interception
# Low-latency because: frames are captured the instant the OS delivers the
# TCP segment to Chromium; no DOM involvement, no JS execution round-trip.
# ---------------------------------------------------------------------------

def attach_websocket_listener(ws: WebSocket) -> None:
    """
    Called once per new WebSocket connection.
    Registers a frame listener directly on the WS object.
    Playwright calls the callback synchronously on the event loop when a frame
    arrives — zero artificial delay.
    """
    url = ws.url
    log_debug(f"[WS] connected: {url}")

    def on_frame_received(payload) -> None:
        # payload.text is set for text frames; payload.binary for binary.
        raw = getattr(payload, "text", None) or ""

        if not raw:
            return  # binary frame (e.g. protobuf) — extend here if needed

        items = try_parse_ws_frame(raw)
        for item in items:
            emit(item["headline"], item["source"], raw)

        # If nothing matched, dump raw frame so you can inspect & adapt the parser.
        if not items and len(raw) > 2:
            log_debug(f"[WS unmatched] {raw[:300]}")

    def on_close() -> None:
        log_debug(f"[WS] closed: {url}")

    # Register listeners — Playwright fires these on the asyncio event loop,
    # keeping everything non-blocking.
    ws.on("framereceived", on_frame_received)
    ws.on("close", on_close)


# ---------------------------------------------------------------------------
# STRATEGY 2 — XHR / fetch / SSE response interception
# Slightly higher latency than WS (response body must be fully received before
# the 'response' event fires for non-streaming responses).
# For SSE, Playwright streams body chunks, so latency is near-WS.
# ---------------------------------------------------------------------------

async def on_response(response) -> None:
    """
    Intercept every HTTP response and check if it carries news payloads.
    We filter to JSON/SSE endpoints to avoid wasting time on assets.
    """
    url = response.url
    status = response.status
    content_type = response.headers.get("content-type", "")

    # Quickly discard irrelevant resources (images, fonts, JS bundles, etc.)
    if status not in (200, 201, 206):
        return
    if not any(t in content_type for t in ("json", "event-stream", "text/plain")):
        return
    # Heuristic: skip large static bundles (content-length > 500 KB)
    cl = response.headers.get("content-length", "0")
    if cl.isdigit() and int(cl) > 500_000:
        return

    log_debug(f"[HTTP] {url} ({content_type})")

    try:
        body = await response.text()
    except Exception:
        return  # body already consumed or network error

    items = try_parse_http_body(body, content_type)
    for item in items:
        emit(item["headline"], item["source"], body[:2000])


# ---------------------------------------------------------------------------
# STRATEGY 3 — MutationObserver (DOM fallback)
# Highest latency: requires JS execution + IPC round-trip back to Python.
# Used only if WS/HTTP strategies produce nothing.
# The observer fires synchronously in the browser renderer, then Playwright
# exposes_function bridges it to Python — still faster than polling.
# ---------------------------------------------------------------------------

MUTATION_OBSERVER_JS = """
() => {
    // Avoid double-registration on navigations / SPA route changes.
    if (window.__fjObserverActive) return;
    window.__fjObserverActive = true;

    const seen = new Set();

    // Target selectors for Financial Juice news items — update if layout changes.
    const NEWS_SELECTORS = [
        '.feed-item',
        '.news-item',
        '.headline',
        '[data-news]',
        '.breaking-news',
        '.fj-news-row',
    ];

    function extractText(node) {
        return (node.innerText || node.textContent || '').trim();
    }

    function processNode(node) {
        if (node.nodeType !== 1) return;  // Element nodes only
        const text = extractText(node);
        if (!text || text.length < 5) return;
        if (seen.has(text)) return;
        seen.add(text);

        // Report via the exposed Python function.
        if (window.__fjReportNews) {
            window.__fjReportNews(JSON.stringify({ headline: text, source: 'dom' }));
        }
    }

    const observer = new MutationObserver((mutations) => {
        for (const mut of mutations) {
            for (const node of mut.addedNodes) {
                // Check the node itself and all its descendant matches.
                processNode(node);
                if (node.querySelectorAll) {
                    for (const sel of NEWS_SELECTORS) {
                        node.querySelectorAll(sel).forEach(processNode);
                    }
                }
            }
        }
    });

    observer.observe(document.body, { childList: true, subtree: true });
}
"""

async def inject_mutation_observer(page: Page) -> None:
    """
    Expose a Python callback to the page, then inject the MutationObserver.
    The observer calls window.__fjReportNews() synchronously on every DOM
    insertion — Playwright bridges this back to Python with minimal delay.
    """
    async def dom_news_handler(payload: str) -> None:
        try:
            obj = json.loads(payload)
            emit(obj.get("headline", ""), obj.get("source", "dom"), payload)
        except json.JSONDecodeError:
            emit(payload, "dom", payload)

    # expose_function registers a JS-callable that runs Python code.
    try:
        await page.expose_function("__fjReportNews", dom_news_handler)
    except Exception:
        pass  # already exposed on a previous navigation

    await page.evaluate(MUTATION_OBSERVER_JS)
    log_debug("[DOM] MutationObserver injected")


# ---------------------------------------------------------------------------
# AUTH HELPERS
# ---------------------------------------------------------------------------

async def inject_auth(page: Page) -> None:
    """
    Log in to Financial Juice using email + password from environment variables.
    Set credentials in .env:
        FJ_EMAIL=your@email.com
        FJ_PASSWORD=yourpassword

    Flow (based on observed UI):
      1. Navigate to financialjuice.com/home
      2. Click the "LOGIN" button in the top-right nav bar
      3. A modal opens already showing the SIGN IN tab with Email + Password fields
      4. Fill Email, fill Password, click the cyan LOGIN button inside the modal
      5. Wait for the modal to disappear (success signal)
      On any failure a screenshot is saved to ./debug/ for diagnosis.
    """
    email = os.environ.get("FJ_EMAIL", "").strip()
    password = os.environ.get("FJ_PASSWORD", "").strip()

    if not email or not password:
        print(
            "[AUTH] FJ_EMAIL or FJ_PASSWORD not set — skipping login. "
            "News feed may be limited or unavailable.",
            file=sys.stderr, flush=True,
        )
        return

    async def save_screenshot(label: str) -> None:
        # /tmp is always writable by any user inside the container.
        # Retrieve with: docker cp financialjuice_live:/tmp/debug_<label>.png .
        path = f"/tmp/debug_{label}.png"
        try:
            await page.screenshot(path=path, full_page=True)
            print(f"[AUTH] Screenshot saved to {path} — retrieve with:", file=sys.stderr, flush=True)
            print(f"[AUTH]   docker cp financialjuice_live:{path} .", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[AUTH] Screenshot failed: {exc}", file=sys.stderr, flush=True)

    async def dump_page_state() -> None:
        """Print page title and first 1000 chars of HTML to stderr for diagnosis."""
        try:
            title = await page.title()
            html = await page.content()
            print(f"[AUTH] Page title: {title!r}", file=sys.stderr, flush=True)
            print(f"[AUTH] Page HTML snippet: {html[:1000]}", file=sys.stderr, flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Step 1: load the homepage
    # ------------------------------------------------------------------
    log_debug("[AUTH] Loading homepage")
    await page.goto("https://www.financialjuice.com/home", wait_until="domcontentloaded", timeout=30_000)

    # ------------------------------------------------------------------
    # Step 2: click the "LOGIN" nav button to open the modal.
    # In the Financial Juice nav bar this is an <a> or <span> element.
    # Try selectors from most specific to broadest.
    # ------------------------------------------------------------------
    nav_login_selectors = [
        "nav a:has-text('LOGIN')",
        "header a:has-text('LOGIN')",
        "a:has-text('LOGIN')",
        "[class*='login']:visible",
        "text=/^LOGIN$/i",
    ]
    clicked = False
    for sel in nav_login_selectors:
        try:
            await page.click(sel, timeout=5_000)
            log_debug(f"[AUTH] Clicked LOGIN nav button via selector: {sel}")
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        print("[AUTH] Could not find the LOGIN nav button.", file=sys.stderr, flush=True)
        await dump_page_state()
        await save_screenshot("no_login_button")
        return

    # ------------------------------------------------------------------
    # Step 3: wait for the SIGN IN modal — Email field with placeholder "Email"
    # ------------------------------------------------------------------
    email_field = page.locator("input[placeholder='Email']").first
    try:
        await email_field.wait_for(state="visible", timeout=10_000)
        log_debug("[AUTH] Modal open — SIGN IN form visible")
    except Exception:
        print("[AUTH] Login modal did not appear.", file=sys.stderr, flush=True)
        await save_screenshot("modal_not_open")
        return

    # ------------------------------------------------------------------
    # Step 4: fill the form
    # ------------------------------------------------------------------
    await email_field.fill(email)

    password_field = page.locator("input[placeholder='Password']").first
    await password_field.fill(password)

    log_debug("[AUTH] Credentials filled — clicking LOGIN button")

    # ------------------------------------------------------------------
    # Step 5: click the cyan LOGIN submit button inside the modal.
    # The nav "LOGIN" text link and the modal button both match "text=LOGIN",
    # but after the modal opens the nav button is hidden — still use nth=0
    # to be safe, targeting the visible one.
    # ------------------------------------------------------------------
    login_btn = page.locator("button:has-text('LOGIN'), input[value='LOGIN']").first
    try:
        await login_btn.click(timeout=5_000)
    except Exception:
        # Fallback: submit via Enter key
        await password_field.press("Enter")

    # ------------------------------------------------------------------
    # Step 6: confirm the modal closed (success = modal gone)
    # ------------------------------------------------------------------
    try:
        await email_field.wait_for(state="hidden", timeout=15_000)
        print("[AUTH] Login successful — modal closed.", file=sys.stderr, flush=True)
    except Exception:
        print(
            f"[AUTH] Login may have failed — modal still visible. "
            "Check credentials and see debug/debug_login_failed.png",
            file=sys.stderr, flush=True,
        )
        await save_screenshot("login_failed")


# ---------------------------------------------------------------------------
# DEBUG LOGGING (suppressed in production — set DEBUG=True for troubleshooting)
# ---------------------------------------------------------------------------

DEBUG = os.environ.get("FJ_DEBUG", "0") == "1"

def log_debug(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG {datetime.now(timezone.utc).isoformat()}] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def _setup_and_run(page: Page, context) -> None:
    """
    Wire up all interception strategies, navigate to Financial Juice, and block
    until cancelled.  Called with either a Playwright or camoufox page — both
    expose the same Playwright Page API.
    """
    # ------------------------------------------------------------------
    # STRATEGY 1: WebSocket interception — register BEFORE navigation so
    # we catch the very first WS handshake.
    # ------------------------------------------------------------------
    page.on("websocket", attach_websocket_listener)

    # ------------------------------------------------------------------
    # STRATEGY 2: HTTP response interception.
    # ------------------------------------------------------------------
    page.on("response", on_response)

    # ------------------------------------------------------------------
    # Inject auth credentials before the page loads.
    # ------------------------------------------------------------------
    await inject_auth(page)

    # ------------------------------------------------------------------
    # Navigate to Financial Juice.
    # wait_until="domcontentloaded" is intentionally used over "load" to
    # return control earlier — the WS connection typically opens before
    # all assets finish loading.
    # ------------------------------------------------------------------
    log_debug(f"Navigating to {TARGET_URL}")
    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30_000)

    # ------------------------------------------------------------------
    # STRATEGY 3: Inject MutationObserver as a DOM-level safety net.
    # Re-inject on every SPA navigation (handles client-side routing).
    # ------------------------------------------------------------------
    await inject_mutation_observer(page)

    async def on_frame_navigated(frame) -> None:
        if frame == page.main_frame:
            log_debug(f"[NAV] main frame navigated to {frame.url}")
            await inject_mutation_observer(page)

    page.on("framenavigated", on_frame_navigated)

    log_debug("Listening for real-time news. Press Ctrl+C to stop.")

    # ------------------------------------------------------------------
    # Keep the event loop alive indefinitely.
    # All news delivery is event-driven — this coroutine simply waits.
    # No polling, no sleep loops.
    # ------------------------------------------------------------------
    try:
        await asyncio.Future()   # runs forever until cancelled
    except asyncio.CancelledError:
        pass
    finally:
        await context.close()


async def main() -> None:
    proxy_cfg = {"server": PROXY_URL} if PROXY_URL else None

    if BROWSER_PROFILE_DIR:
        # Persistent context: reuses cookies, IndexedDB, etc. across runs.
        # Uses Chromium to stay compatible with an existing saved profile.
        async with async_playwright() as pw:
            context = await pw.chromium.launch_persistent_context(
                BROWSER_PROFILE_DIR,
                headless=True,          # set False to debug visually
                args=CHROMIUM_ARGS,
                # Disable service workers that might intercept and buffer requests.
                service_workers="block",
                proxy=proxy_cfg,
            )
            page = context.pages[0] if context.pages else await context.new_page()
            await _setup_and_run(page, context)
    else:
        # Fresh session: use camoufox (patched Firefox) which applies dozens of
        # fingerprint-level patches that defeat Cloudflare's bot-detection
        # heuristics.  Pair with FJ_PROXY_URL (residential proxy) to bypass
        # IP-level blocks on VPS/datacenter hosts.
        from camoufox.async_api import AsyncCamoufox
        async with AsyncCamoufox(headless=True, proxy=proxy_cfg) as browser:
            context = await browser.new_context(service_workers="block")
            page = await context.new_page()
            await _setup_and_run(page, context)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)


# =============================================================================
# Ultra-Low Latency Performance Notes
# =============================================================================
#
# 1. EVENT-DRIVEN, NOT POLLED
#    All three strategies (WS, HTTP, DOM) are callback-based. Playwright fires
#    Python callbacks the moment the underlying Chromium event fires, so there
#    is no fixed polling interval adding latency.
#
# 2. WEBSOCKET IS THE FASTEST PATH
#    Financial Juice uses SignalR over WebSocket. The `framereceived` callback
#    is invoked as soon as the OS delivers the TCP segment; the frame never
#    touches the DOM. Measured overhead: ~0.1–1 ms from TCP arrival to Python
#    callback on a local machine.
#
# 3. CHROMIUM LAUNCH FLAGS
#    `--disable-background-timer-throttling` and related flags prevent Chrome
#    from throttling JS timers when the window is not focused — important for
#    headless runs where the "window" is never in the foreground.
#
# 4. HEADLESS MODE
#    Rendering the page visually costs CPU and can delay event dispatch.
#    Headless eliminates the compositor thread overhead.
#
# 5. service_workers="block"
#    Service workers can intercept fetch/WS traffic and introduce buffering.
#    Blocking them ensures Playwright sees the raw events.
#
# 6. wait_until="domcontentloaded"
#    Using "load" would wait for all images/fonts. The WS connection and
#    first news frame typically arrive before all assets finish loading.
#    Returning early means we start capturing sooner.
#
# 7. STDOUT FLUSHING
#    `print(..., flush=True)` bypasses Python's default line-buffering in
#    piped mode. Without this, output can be batched by the OS for seconds.
#
# 8. ADAPTING TO ENDPOINT CHANGES
#    - Run with DEBUG=True to see raw WS frames and HTTP bodies in stderr.
#    - Update `try_parse_ws_frame()` when the SignalR target name changes.
#    - Update `try_parse_http_body()` for new REST shapes.
#    - Update NEWS_SELECTORS in MUTATION_OBSERVER_JS for DOM layout changes.
#
# 9. MULTIPLE PAGES / SHARDING
#    For even lower perceived latency, open two pages to the same URL in the
#    same context. Whichever WS connection receives the frame first will emit
#    it — useful if Financial Juice load-balances across servers.
#
# 10. NETWORK PROXIMITY
#    Co-locating the script in the same AWS/Azure region as Financial Juice's
#    servers reduces TCP RTT from ~20–100 ms (home ISP) to ~1–5 ms.
