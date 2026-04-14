import asyncio
import re
from typing import List, Dict

import feedparser


async def fetch_feed_entries(rss_url: str) -> List[Dict]:
    def _parse():
        feed = feedparser.parse(rss_url)
        out = []
        for e in feed.entries:
            out.append({
                "title": e.get("title", ""),
                "link": e.get("link"),
                "id": e.get("id") or e.get("link"),
                "summary": e.get("summary", ""),
            })
        return out

    return await asyncio.to_thread(_parse)
