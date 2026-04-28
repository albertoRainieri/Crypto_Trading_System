"""
generate_session.py
One-time helper to produce a Telethon StringSession value for use in .env.

Run this ONCE on any machine that has a terminal (NOT inside Docker):
    pip install telethon
    python generate_session.py

It will prompt for your phone number and a verification code sent by Telegram,
then print a SESSION string.  Copy that string into .env as TG_SESSION=...

You only need to do this once — the session stays valid until you log out or
revoke it from Telegram's active sessions list.
"""

import os
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID  = int(input("Enter TG_API_ID  : ").strip())
API_HASH =    input("Enter TG_API_HASH: ").strip()

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    session_string = client.session.save()

print()
print("=" * 60)
print("Copy the line below into your .env file:")
print()
print(f"TG_SESSION={session_string}")
print("=" * 60)
