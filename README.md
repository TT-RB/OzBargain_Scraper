# OzBargain Discord Notifier

Lightweight Discord bot that monitors OzBargain RSS feed and notifies users based on keywords.

Requirements
- Python 3.9+
- Install dependencies:

```bash
pip install -r requirements.txt
```

Configuration
1. Copy `config.example.json` to `config.json` and set your `discord_token` (or set `DISCORD_TOKEN` env var).
2. Optionally change `rss_url`, polling interval and cooldown in the config.

Run

```bash
export DISCORD_TOKEN="..."
python bot.py
```

Usage (Discord commands)
- `!addkeyword <keyword>` — add a keyword to receive DMs when matched
- `!removekeyword <keyword>` — remove a keyword
- `!listkeywords` — list your keywords

Notes
- Matching: case-insensitive substring first; optional fuzzy matching uses difflib ratio (threshold 0.7 default).
- Cooldown: per-user cooldown (default 3600s) prevents frequent notifications. Also prevents duplicate notify for same deal.
- Upvote scraping is heuristic; changes to OzBargain layout may require updates in `scraper.py`.

Docker
 - Build:

```bash
docker build -t ozbargain-bot:latest .
```

 - Run (pass envs):

```bash
docker run -e DISCORD_TOKEN="$DISCORD_TOKEN" -e DATABASE_URL="$DATABASE_URL" ozbargain-bot:latest
```

Railway notes
 - Railway supports deploying via Docker, a Procfile, or direct GitHub integration.
 - Railway provides managed add-ons: Postgres is the most common managed relational DB there (you can also provision MySQL, Redis, and other services via plugins). For reliability and compatibility, use Postgres and set `DATABASE_URL` in Railway env.
 - Add a `Procfile` (included) or use the `Dockerfile` above. Ensure `DISCORD_TOKEN` and `DATABASE_URL` are set in Railway project variables.
