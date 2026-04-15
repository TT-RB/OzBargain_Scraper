import json
import logging
import os

import discord
from rapidfuzz import fuzz
import asyncio
from aiohttp import web
from discord.ext import commands, tasks

from db import Database
from scraper import fetch_feed_entries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("OZBOT_CONFIG", "config.example.json")


def load_config(path=CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)


config = load_config()
BOT_TOKEN = os.environ.get("DISCORD_TOKEN") or config.get("discord_token")
FEED_URL = config.get("rss_url")
POLL_INTERVAL = config.get("poll_interval_seconds", 60)
COOLDOWN = config.get("cooldown_seconds", 600)
NOTIFY_CHANNEL_ID = os.environ.get("NOTIFY_CHANNEL_ID") or config.get("notify_channel_id")
WEB_PORT = int(os.environ.get("WEB_PORT") or config.get("web_port", 8000))


intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (id: {bot.user.id})")
    db_url = os.environ.get("DATABASE_URL") or config.get("database_url") or f"sqlite:///{config.get('db_path', 'ozbargain.db')}"
    bot.db = Database(db_url)
    await bot.db.init_db()
    if not poll_feed.is_running():
        poll_feed.start()
    # start the popular checker if it's defined in this module
    try:
        if not popular_deals_check.is_running():
            popular_deals_check.start()
    except NameError:
        logger.warning("popular_deals_check not defined; skipping popular checker start")
    # start HTTP test endpoint
    bot.web_task = asyncio.create_task(start_web_server())


async def start_web_server():
    async def handle_test_notify(request: web.Request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        # expected payload: {"user_ids": [123,...], "channel_id": 123, "content": "..."}
        user_ids = data.get("user_ids") or []
        if isinstance(user_ids, int):
            user_ids = [user_ids]
        content = data.get("content") or "Test notification"
        channel_id = data.get("channel_id") or NOTIFY_CHANNEL_ID

        # build mention prefix
        mentions = "".join(f"<@{int(uid)}> " for uid in user_ids)
        full_msg = f"{mentions}{content}"

        # send to channel if provided, else DM each user
        if channel_id:
            try:
                ch_id = int(channel_id)
                ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
                await ch.send(full_msg)
                return web.json_response({"ok": True, "sent_to": f"channel:{ch_id}"})
            except Exception as e:
                # fallback to DM
                for uid in user_ids:
                    try:
                        user = await bot.fetch_user(int(uid))
                        await user.send(content)
                    except Exception:
                        pass
                return web.json_response({"ok": True, "sent_to": "dm_fallback", "error": str(e)})

        # no channel: DM each user
        for uid in user_ids:
            try:
                user = await bot.fetch_user(int(uid))
                await user.send(content)
            except Exception:
                pass
        return web.json_response({"ok": True, "sent_to": "dms"})

    app = web.Application()
    app.router.add_post('/test_notify', handle_test_notify)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Test HTTP server running on port {WEB_PORT}")
    logger.info(f"Started with FEED_URL={FEED_URL}, POLL_INTERVAL={POLL_INTERVAL}s, COOLDOWN={COOLDOWN}s, NOTIFY_CHANNEL_ID={NOTIFY_CHANNEL_ID}")
    
    # keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await runner.cleanup()
        raise


@tasks.loop(seconds=POLL_INTERVAL)
async def poll_feed():
    logger.info("Polling feed at time %f", asyncio.get_event_loop().time())
    try:
        entries = await fetch_feed_entries(FEED_URL)
        for entry in entries:
            entry_id = (
                entry.get("id") or
                entry.get("guid")
            )
            if not entry_id:
                continue

            already = await bot.db.is_seen(entry_id)
            if already:
                continue

            title = entry.get("title", "")
            link = entry.get("link")
            summary = entry.get("summary", "")

            upvotes = entry.get("upvotes", 0)

            # record deal metadata/upvotes
            try:
                await bot.db.upsert_deal(entry_id, title, link, upvotes)
            except Exception:
                logger.exception("Failed to upsert deal %s", entry_id)

            subs = await bot.db.get_all_subscriptions()
            for row in subs:
                owner_id = row[0]
                keyword = row[1]
                fuzzy = bool(row[2])
                threshold = float(row[3] or 80)
                target_type = row[4] or "user"
                target_id = row[5] or owner_id

                matched = False

                # treat 'all' or '*' as match-all
                if keyword.strip().lower() in ("*", "all"):
                    matched = True
                else:
                    text = f"{title}\n{summary}"

                    if keyword.lower() in text.lower():
                        matched = True
                    elif fuzzy:
                        ratio = max(
                            fuzz.ratio(keyword.lower(), title.lower()),
                            fuzz.ratio(keyword.lower(), summary.lower()),
                        )
                        if ratio >= threshold:
                            matched = True

                if not matched:
                    logger.debug(f"No match for subscription {keyword} (fuzzy={fuzzy}, threshold={threshold}) on deal {entry_id} with title '{title}'")
                    continue

                can = await bot.db.can_notify_target(target_type, target_id, entry_id, COOLDOWN)
                if not can:
                    logger.info(f"Skipping notification for {target_type}:{target_id} on deal {entry_id} due to cooldown")
                    continue

                try:
                    msg = f"New OzBargain deal matched: **{title}**\n{link}\n"
                    msg += f"Upvotes: {upvotes}\n"
                    msg += f"Matched keyword: `{keyword}`"

                    if target_type == "user":
                        user = await bot.fetch_user(target_id)
                        await user.send(msg)
                        logger.info(f"Sent DM to user {target_id} for deal {entry_id}")
                    else:
                        ch = bot.get_channel(target_id) or await bot.fetch_channel(target_id)
                        await ch.send(msg)
                        logger.info(f"Sent notification to channel {target_id} for deal {entry_id}")

                    await bot.db.record_notification_target(target_type, target_id, entry_id)

                except Exception as e:
                    logger.exception("Failed to notify target %s:%s: %s", target_type, target_id, e)

            # ✅ mark seen AFTER processing (safer)
            await bot.db.mark_seen(entry_id)

    except Exception:
        logger.exception("Error while polling feed")

# background task to check for popular deals (e.g., >=50 upvotes within configured window)
@tasks.loop(seconds=POLL_INTERVAL)
async def popular_deals_check():
    try:
        POPULAR_THRESHOLD = int(config.get("popular_upvote_threshold", 50))
        POPULAR_WINDOW = int(config.get("popular_window_seconds", 3600))

        deals = await bot.db.get_popular_deals(
            min_upvotes=POPULAR_THRESHOLD,
            within_seconds=POPULAR_WINDOW
        )

        if not deals:
            return

        subs = await bot.db.get_all_subscriptions()
        seen_targets = set()

        for deal in deals:
            deal_id, title, url, first_seen_ts, last_upvotes, last_checked_ts = deal

            for row in subs:
                target_type = row[4] or "user"
                target_id = row[5] or row[0]

                key = (target_type, target_id)
                if key in seen_targets:
                    continue
                seen_targets.add(key)

                can = await bot.db.can_notify_target(
                    target_type, target_id, deal_id, COOLDOWN
                )
                if not can:
                    continue

                try:
                    msg = (
                        f"🔥 **Popular Deal Alert!**\n"
                        f"{title}\n{url}\n"
                        f"👍 {last_upvotes} upvotes\n"
                        f"Take a look 👀"
                    )

                    if target_type == "user":
                        user = await bot.fetch_user(target_id)
                        await user.send(msg)
                        logger.info(f"Sent popular deal DM to user {target_id} for deal {deal_id}")
                    else:
                        ch = bot.get_channel(target_id) or await bot.fetch_channel(target_id)
                        await ch.send(msg)
                        logger.info(f"Sent popular deal notification to channel {target_id} for deal {deal_id}")

                    await bot.db.record_notification_target(
                        target_type, target_id, deal_id
                    )

                except Exception as e:
                    logger.exception(
                        "Failed to notify %s:%s: %s",
                        target_type, target_id, e
                    )

    except Exception:
        logger.exception("Error in popular_deals_check")


@bot.command(name="addkeyword")
async def add_keyword(ctx, *, keyword: str):
    user_id = ctx.author.id
    await bot.db.add_subscription(user_id, keyword, target_type="user", target_id=user_id)
    await ctx.send(f"Added keyword '{keyword}' for {ctx.author.mention}")


@bot.command(name="recentdeals")
async def recent_deals(ctx, seconds: int = 3600, limit: int = 20):
    """Return recent deals first seen within `seconds` (default 3600s)."""
    rows = await bot.db.get_recent_deals(since_seconds=seconds, limit=limit)
    if not rows:
        await ctx.send(f"No deals found in the last {seconds} seconds.")
        return
    lines = []
    for r in rows:
        deal_id, title, url, first_seen_ts, upvotes, last_checked = r
        lines.append(f"- {title} ({upvotes} upvotes) - {url}")
    # send in chunks if too long
    chunk = "\n".join(lines)
    if len(chunk) < 1900:
        await ctx.send(f"Recent deals:\n{chunk}")
    else:
        # send truncated to avoid hitting discord length limits
        await ctx.send("Recent deals (truncated):\n" + chunk[:1800])


@bot.command(name="addchannelkeyword")
async def add_channel_keyword(ctx, *, keyword: str):
    owner_id = ctx.author.id
    await bot.db.add_subscription(owner_id, keyword, target_type="channel", target_id=ctx.channel.id)
    await ctx.send(f"Added channel keyword '{keyword}' for this channel")


@bot.command(name="removekeyword")
async def remove_keyword(ctx, *, keyword: str):
    user_id = ctx.author.id
    removed = await bot.db.remove_subscription(user_id, keyword, target_type="user", target_id=user_id)
    if removed:
        await ctx.send(f"Removed keyword '{keyword}'")
    else:
        await ctx.send(f"Keyword '{keyword}' not found")

@bot.command(name="removeallkeywords")
async def remove_all_keywords(ctx):
    user_id = ctx.author.id
    await bot.db.remove_all_subscriptions(user_id)
    await ctx.send("Removed all your keywords.")

@bot.command(name="removechannelkeyword")
async def remove_channel_keyword(ctx, *, keyword: str):
    owner_id = ctx.author.id
    removed = await bot.db.remove_subscription(owner_id, keyword, target_type="channel", target_id=ctx.channel.id)
    if removed:
        await ctx.send(f"Removed channel keyword '{keyword}'")
    else:
        await ctx.send(f"Channel keyword '{keyword}' not found")


@bot.command(name="listkeywords")
async def list_keywords(ctx):
    user_id = ctx.author.id
    rows = await bot.db.list_subscriptions(user_id)
    if not rows:
        await ctx.send("You have no keywords set.")
        return
    lines = []
    for r in rows:
        kw = r[0]
        target_type = r[3]
        target_id = r[4]
        display_kw = kw
        if isinstance(kw, str) and kw.strip().lower() in ("*", "all"):
            display_kw = f"{kw} (all matches)"
        if target_type == "channel":
            lines.append(f"- {display_kw} (channel: {target_id})")
        else:
            lines.append(f"- {display_kw} (dm)")
    await ctx.send("Your keywords:\n" + "\n".join(lines))

@bot.command(name="help", aliases=["commands"])
async def help_command(ctx):
    msg = (
        "**OzBargain Bot Commands**\n\n"
        "`!addkeyword <keyword>` — get DMs when a deal matches your keyword\n"
        "`!removekeyword <keyword>` — remove a keyword\n"
        "`!removeallkeywords` — remove all your keywords\n"
        "`!listkeywords` — list all your keywords\n\n"
        "`!addchannelkeyword <keyword>` — post to this channel when a deal matches\n"
        "`!removechannelkeyword <keyword>` — remove a channel keyword\n\n"
        "`!recentdeals [seconds] [limit]` — list recent deals (default: last 3600s, up to 20)\n"
    )
    await ctx.send(msg)


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("Set DISCORD_TOKEN env or put token in config.example.json")
        raise SystemExit(1)
    bot.run(BOT_TOKEN)
