import asyncio
import re
from typing import List, Dict

import feedparser
import requests
from bs4 import BeautifulSoup


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


async def scrape_upvotes(url: str) -> int:
    def _get():
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # heuristic: find element with vote / upvote / count
        # try common classes
        candidates = []
        for cls in ["vote-count", "count", "votes", "vote"]:
            el = soup.find(class_=re.compile(cls))
            if el and el.get_text(strip=True):
                candidates.append(el.get_text(strip=True))

        # fallback: search for numbers near 'upvote' or 'votes'
        if not candidates:
            text = soup.get_text(separator=" \n ")
            m = re.search(r"(\d{1,5})\s*(?:upvote|upvotes|votes)", text, re.I)
            if m:
                return int(m.group(1))

        for c in candidates:
            m = re.search(r"(\d{1,6})", c)
            if m:
                return int(m.group(1))
        return None

    return await asyncio.to_thread(_get)
