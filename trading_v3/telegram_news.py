"""
telegram_news.py
Real-time macro news from Telegram channels using Telethon (MTProto).

This is a drop-in replacement for trading_v2/financialjuice_live.py.
It emits the same JSON record format to stdout so downstream consumers
need no changes.

Output format (one JSON object per line, flushed immediately):
    {
        "received_at": "<ISO-8601 UTC>",
        "headline":    "<message text>",
        "source":      "<channel username>",
        "raw_payload": "<original Telethon Message repr, truncated>"
    }

Prerequisites
-------------
1. Create a Telegram application at https://my.telegram.org/apps
   to obtain API_ID and API_HASH.
2. Run generate_session.py ONCE on a machine with terminal access
   to produce a TG_SESSION string (serialised Telethon StringSession).
3. Set the three values in .env:
       TG_API_ID=12345678
       TG_API_HASH=abcdef1234567890abcdef1234567890
       TG_SESSION=<long base64 string from generate_session.py>
4. Optionally override TG_CHANNELS (comma-separated channel usernames
   or numeric IDs).  Default channels are public macro-news feeds that
   carry the same tier-1 headlines as FinancialJuice.

Run locally:
    pip install telethon
    python telegram_news.py

Run in Docker:
    docker compose up
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession


# ---------------------------------------------------------------------------
# CONFIG  (all values read from environment / .env via docker-compose)
# ---------------------------------------------------------------------------

API_ID: int = int(os.environ["TG_API_ID"])
API_HASH: str = os.environ["TG_API_HASH"]
SESSION: str = os.environ["TG_SESSION"]

# Comma-separated list of public channel usernames or numeric IDs to monitor.
# These channels publicly re-broadcast tier-1 macro/FX headlines in real time.
# Add or remove channels to taste.
_DEFAULT_CHANNELS = ",".join([
    "@financialjuice",       # FJ's own Telegram mirror
    "@FXMacroAlerts",        # macro event alerts
    "@macro_alerts",         # macro news aggregator
    "@forexlive",            # ForexLive breaking news
    "@newsquawk",            # Newsquawk headlines (free tier)
])

CHANNELS: list[str] = [
    ch.strip()
    for ch in os.environ.get("TG_CHANNELS", _DEFAULT_CHANNELS).split(",")
    if ch.strip()
]


# ---------------------------------------------------------------------------
# OUTPUT  (same schema as trading_v2 so downstream consumers are unchanged)
# ---------------------------------------------------------------------------

def emit(headline: str, source: str, raw_payload: str) -> None:
    """Write a news record to stdout as a single JSON line, flushed immediately."""
    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "headline": headline.strip(),
        "source": source.strip(),
        "raw_payload": raw_payload,
    }
    print(json.dumps(record, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main() -> None:
    client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)

    await client.start()

    # Resolve all configured channels once at startup to verify they exist
    # and to map entity objects for the event filter.
    entities = []
    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch)
            entities.append(entity)
            print(f"[INFO] Subscribed to channel: {ch}", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[WARN] Could not resolve channel {ch!r}: {exc}", file=sys.stderr, flush=True)

    if not entities:
        print("[ERROR] No channels resolved — check TG_CHANNELS and credentials.", file=sys.stderr, flush=True)
        return

    @client.on(events.NewMessage(chats=entities))
    async def handler(event: events.NewMessage.Event) -> None:
        message = event.message
        text = (message.message or "").strip()
        if not text:
            return  # skip media-only messages with no caption

        # Identify the channel by its username or numeric ID.
        chat = await event.get_chat()
        source = getattr(chat, "username", None) or str(chat.id)

        raw = repr(message)[:2000]  # truncate to avoid huge payloads
        emit(text, source, raw)

    print(
        f"[INFO] Listening for real-time news on {len(entities)} channel(s). "
        "Press Ctrl+C to stop.",
        file=sys.stderr, flush=True,
    )

    # Block until disconnected or Ctrl+C.
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
