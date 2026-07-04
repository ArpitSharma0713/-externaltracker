import os
import json
import asyncio
import random
from datetime import datetime, timezone

import redis
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import PhoneNumberInvalidError


load_dotenv()


REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "localhost"
)

REDIS_PORT = int(
    os.getenv(
        "REDIS_PORT",
        "6379"
    )
)

RAW_QUEUE_NAME = os.getenv(
    "PROVIO_RAW_MESSAGES_QUEUE",
    "provio_raw_messages_queue"
)

TG_API_ID = os.getenv(
    "TG_API_ID"
)

TG_API_HASH = os.getenv(
    "TG_API_HASH"
)

TG_SESSION_NAME = os.getenv(
    "TG_SESSION_NAME",
    "provio_tg_session"
)

TG_PHONE_NUMBER = os.getenv(
    "TG_PHONE_NUMBER",
    ""
).strip()

TG_TARGET_CHANNELS = [
    channel.strip()
    for channel in os.getenv(
        "TG_TARGET_CHANNELS",
        "@offcampusjobupdateslive,@fresher_openings_india,@dev_jobs_hub"
    ).split(",")
    if channel.strip()
]

TG_PUBLIC_PREVIEW_CHANNELS = [
    channel.strip().replace("@", "")
    for channel in os.getenv(
        "TG_PUBLIC_PREVIEW_CHANNELS",
        "india_tech_jobs,fresher_openings_india,dev_jobs_hub"
    ).split(",")
    if channel.strip()
]

TG_WORKER_MODE = os.getenv(
    "TG_WORKER_MODE",
    "telethon"
).lower()

PUBLIC_PREVIEW_POLL_SECONDS = int(
    os.getenv(
        "PUBLIC_PREVIEW_POLL_SECONDS",
        "60"
    )
)


redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)


def ensure_redis_connection():
    try:
        redis_client.ping()
        print("[TG-WORKER] Redis connected")
    except Exception as ex:
        raise RuntimeError(
            f"Redis connection failed: {str(ex)}"
        )


def enqueue_raw_message(
    payload: dict
):
    redis_client.rpush(
        RAW_QUEUE_NAME,
        json.dumps(
            payload,
            ensure_ascii=False
        )
    )


def normalize_channel_name(
    chat
) -> str:
    username = getattr(
        chat,
        "username",
        None
    )

    if username:
        return f"@{username}"

    chat_id = getattr(
        chat,
        "id",
        "unknown"
    )

    return str(chat_id)


async def run_telethon_worker():
    if not TG_API_ID:
        raise RuntimeError(
            "TG_API_ID is missing in .env"
        )

    if not TG_API_HASH:
        raise RuntimeError(
            "TG_API_HASH is missing in .env"
        )

    client = TelegramClient(
        TG_SESSION_NAME,
        api_id=int(TG_API_ID),
        api_hash=TG_API_HASH
    )

    @client.on(
        events.NewMessage(
            chats=TG_TARGET_CHANNELS
        )
    )
    async def handle_incoming_message(
        event
    ):
        await asyncio.sleep(
            random.uniform(
                1.2,
                3.5
            )
        )

        raw_text = event.message.message

        if not raw_text:
            return

        chat = await event.get_chat()

        payload = {
            "source": "telegram",
            "channel": normalize_channel_name(
                chat
            ),
            "message_id": event.message.id,
            "raw_text": raw_text,
            "timestamp": event.message.date.isoformat(),
            "media_urls": []
        }

        enqueue_raw_message(
            payload
        )

        print(
            f"[TG-WORKER] Captured message ID: "
            f"{event.message.id}"
        )

    print(
        "[TG-WORKER] Starting Telegram listener..."
    )

    print(
        f"[TG-WORKER] Target channels: "
        f"{TG_TARGET_CHANNELS}"
    )

    try:
        if TG_PHONE_NUMBER:
            await client.start(
                phone=TG_PHONE_NUMBER
            )
        else:
            await client.start()
    except PhoneNumberInvalidError as ex:
        raise RuntimeError(
            "Telegram rejected the phone number. Use full international "
            "format, for example +919876543210, or leave TG_PHONE_NUMBER "
            "blank and type the number at the prompt."
        ) from ex

    print(
        "[TG-WORKER] Connected to Telegram."
    )

    await client.run_until_disconnected()


def extract_public_preview_messages(
    html: str
):
    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    message_blocks = soup.select(
        ".tgme_widget_message"
    )

    messages = []

    for block in message_blocks:
        message_id = block.get(
            "data-post",
            ""
        )

        text_element = block.select_one(
            ".tgme_widget_message_text"
        )

        time_element = block.select_one(
            "time"
        )

        if text_element is None:
            continue

        raw_text = text_element.get_text(
            "\n",
            strip=True
        )

        if not raw_text:
            continue

        timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        if time_element is not None:
            timestamp = time_element.get(
                "datetime",
                timestamp
            )

        messages.append(
            {
                "message_id": message_id,
                "raw_text": raw_text,
                "timestamp": timestamp
            }
        )

    return messages


async def run_public_preview_fallback():
    print(
        "[TG-WORKER] Starting public preview fallback mode..."
    )

    print(
        f"[TG-WORKER] Preview channels: "
        f"{TG_PUBLIC_PREVIEW_CHANNELS}"
    )

    seen_messages = set()

    while True:
        for channel in TG_PUBLIC_PREVIEW_CHANNELS:
            try:
                await asyncio.sleep(
                    random.uniform(
                        1.2,
                        3.5
                    )
                )

                url = (
                    f"https://t.me/s/"
                    f"{channel}"
                )

                response = requests.get(
                    url,
                    timeout=15,
                    headers={
                        "User-Agent": "Mozilla/5.0"
                    }
                )

                print(
                    f"[TG-PREVIEW] "
                    f"{channel} "
                    f"status={response.status_code}"
                )

                if response.status_code != 200:
                    continue

                messages = extract_public_preview_messages(
                    response.text
                )

                for message in messages:
                    message_key = (
                        f"{channel}:"
                        f"{message['message_id']}"
                    )

                    if message_key in seen_messages:
                        continue

                    seen_messages.add(
                        message_key
                    )

                    payload = {
                        "source": "telegram_public_preview",
                        "channel": f"@{channel}",
                        "message_id": message["message_id"],
                        "raw_text": message["raw_text"],
                        "timestamp": message["timestamp"],
                        "media_urls": []
                    }

                    enqueue_raw_message(
                        payload
                    )

                    print(
                        f"[TG-PREVIEW] Captured message: "
                        f"{message['message_id']}"
                    )

            except Exception as ex:
                print(
                    f"[TG-PREVIEW] Error for {channel}: "
                    f"{str(ex)}"
                )

        await asyncio.sleep(
            PUBLIC_PREVIEW_POLL_SECONDS
        )


async def main():
    ensure_redis_connection()

    if TG_WORKER_MODE == "public_preview":
        await run_public_preview_fallback()
        return

    await run_telethon_worker()


if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )
    except KeyboardInterrupt:
        print("[TG-WORKER] Stopped by user.")
