import json
import logging
import os

import discord
from rapidfuzz import fuzz
from discord.ext import commands, tasks

from db import Database
from scraper import fetch_feed_entries, scrape_upvotes

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
COOLDOWN = config.get("cooldown_seconds", 3600)


intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user} (id: {bot.user.id})")
    db_url = os.environ.get("DATABASE_URL") or config.get("database_url") or f"sqlite:///{config.get('db_path', 'ozbargain.db')}"
    bot.db = Database(db_url)
    await bot.db.init_db()
    if not poll_feed.is_running():
        poll_feed.start()
    # health endpoint not used per configuration


@tasks.loop(seconds=POLL_INTERVAL)
async def poll_feed():
    logger.debug("Polling feed...")
    try:
        entries = await fetch_feed_entries(FEED_URL)
        for entry in entries:
            entry_id = entry.get("id") or entry.get("link")
            if not entry_id:
                continue
            already = await bot.db.is_seen(entry_id)
            if already:
                continue
            await bot.db.mark_seen(entry_id)

            title = entry.get("title", "")
            link = entry.get("link")
            summary = entry.get("summary", "")

            upvotes = None
            if link:
                try:
                    upvotes = await scrape_upvotes(link)
                except Exception:
                    upvotes = None

            subs = await bot.db.get_all_subscriptions()
            for row in subs:
                owner_id = row[0]
                keyword = row[1]
                fuzzy = bool(row[2])
                threshold = float(row[3] or 80)
                target_type = row[4] or "user"
                target_id = row[5] or owner_id
                matched = False
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
                    continue

                can = await bot.db.can_notify_target(target_type, target_id, entry_id, COOLDOWN)
                if not can:
                    continue

                try:
                    msg = f"New OzBargain deal matched: **{title}**\n{link}\n"
                    if upvotes is not None:
                        msg += f"Upvotes: {upvotes}\n"
                    msg += f"Matched keyword: `{keyword}`"
                    if target_type == "user":
                        user = await bot.fetch_user(target_id)
                        await user.send(msg)
                    else:
                        ch = bot.get_channel(target_id) or await bot.fetch_channel(target_id)
                        await ch.send(msg)
                    await bot.db.record_notification_target(target_type, target_id, entry_id)
                except Exception as e:
                    logger.exception("Failed to notify target %s:%s: %s", target_type, target_id, e)

    except Exception:
        logger.exception("Error while polling feed")


@bot.command(name="addkeyword")
async def add_keyword(ctx, *, keyword: str):
    user_id = ctx.author.id
    await bot.db.add_subscription(user_id, keyword, target_type="user", target_id=user_id)
    await ctx.send(f"Added keyword '{keyword}' for {ctx.author.mention}")


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
        if target_type == "channel":
            lines.append(f"- {kw} (channel: {target_id})")
        else:
            lines.append(f"- {kw} (dm)")
    await ctx.send("Your keywords:\n" + "\n".join(lines))


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("Set DISCORD_TOKEN env or put token in config.example.json")
        raise SystemExit(1)
    bot.run(BOT_TOKEN)
