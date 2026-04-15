import asyncio
import re
from typing import List, Dict

import feedparser


async def fetch_feed_entries(rss_url: str) -> List[Dict]:
    def _parse():
        feed = feedparser.parse(rss_url)
        out = []
        for e in feed.entries:
            # feedparser exposes ozb:meta attributes under e.get("ozb_meta") or similar
            # but it's unreliable — read votes-pos directly from the raw tag
            upvotes = 0
            ozb_meta = e.get("ozb_meta") or {}
            if ozb_meta:
                upvotes = int(ozb_meta.get("votes-pos", 0) or 0)

            out.append({
                "title": e.get("title", ""),
                "link": e.get("link"),
                "id": e.get("id") or e.get("link"),
                "summary": e.get("summary", ""),
                "upvotes": upvotes,
            })
        return out

    return await asyncio.to_thread(_parse)