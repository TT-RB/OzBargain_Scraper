from databases import Database as _Database
import time
from typing import List, Tuple, Optional


class Database:
    def __init__(self, database_url: str = None):
        # expect DATABASE_URL env or a provider-supplied URL
        self.database_url = database_url or "sqlite:///ozbargain.db"
        self.db = _Database(self.database_url)

    async def init_db(self):
        await self.db.connect()
        # Create tables (Postgres-compatible)
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                keyword TEXT NOT NULL,
                fuzzy INTEGER DEFAULT 1,
                threshold REAL DEFAULT 80,
                target_type TEXT DEFAULT 'user',
                target_id BIGINT,
                UNIQUE(owner_id, keyword, target_type, target_id)
            )
            """
        )
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS notified2 (
                target_type TEXT NOT NULL,
                target_id BIGINT NOT NULL,
                deal_id TEXT NOT NULL,
                ts BIGINT NOT NULL,
                PRIMARY KEY(target_type, target_id, deal_id)
            )
            """
        )
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS last_notified2 (
                target_type TEXT NOT NULL,
                target_id BIGINT NOT NULL,
                ts BIGINT NOT NULL,
                PRIMARY KEY(target_type, target_id)
            )
            """
        )
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS seen (
                deal_id TEXT PRIMARY KEY,
                ts BIGINT NOT NULL
            )
            """
        )
        # table to keep basic deal metadata and upvote history
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS deals (
                deal_id TEXT PRIMARY KEY,
                title TEXT,
                url TEXT,
                first_seen_ts BIGINT NOT NULL,
                last_upvotes INTEGER DEFAULT 0,
                last_checked_ts BIGINT
            )
            """
        )

    async def add_subscription(self, owner_id: int, keyword: str, target_type: str = "user", target_id: Optional[int] = None, fuzzy: int = 1, threshold: float = 80):
        query = """
        INSERT INTO subscriptions(owner_id, keyword, fuzzy, threshold, target_type, target_id)
        VALUES (:owner_id, :keyword, :fuzzy, :threshold, :target_type, :target_id)
        ON CONFLICT (owner_id, keyword, target_type, target_id) DO NOTHING
        """
        await self.db.execute(query, values={
            "owner_id": owner_id,
            "keyword": keyword,
            "fuzzy": int(fuzzy),
            "threshold": float(threshold),
            "target_type": target_type,
            "target_id": target_id,
        })

    async def remove_subscription(self, owner_id: int, keyword: str, target_type: str = "user", target_id: Optional[int] = None) -> bool:
        query = "DELETE FROM subscriptions WHERE owner_id = :owner_id AND keyword = :keyword AND target_type = :target_type AND target_id = :target_id"
        res = await self.db.execute(query, values={"owner_id": owner_id, "keyword": keyword, "target_type": target_type, "target_id": target_id})
        # databases.execute returns last rowid for some backends; use a SELECT to confirm
        cur = await self.db.fetch_one("SELECT 1 FROM subscriptions WHERE owner_id = :owner_id AND keyword = :keyword AND target_type = :target_type AND target_id = :target_id", values={"owner_id": owner_id, "keyword": keyword, "target_type": target_type, "target_id": target_id})
        return cur is None

    async def remove_all_subscriptions(self, owner_id: int):
        await self.db.execute(
            """
            DELETE FROM subscriptions
            WHERE owner_id = :owner_id
            AND target_type = 'user'
            AND target_id = :owner_id
            """,
            values={"owner_id": owner_id}
        )

    async def list_subscriptions(self, owner_id: int):
        query = "SELECT keyword, fuzzy, threshold, target_type, target_id FROM subscriptions WHERE owner_id = :owner_id"
        rows = await self.db.fetch_all(query, values={"owner_id": owner_id})
        return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    async def get_all_subscriptions(self) -> List[Tuple[int, str, int, float, str, int]]:
        query = "SELECT owner_id, keyword, fuzzy, threshold, target_type, target_id FROM subscriptions"
        rows = await self.db.fetch_all(query)
        return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]

    async def record_notification_target(self, target_type: str, target_id: int, deal_id: str):
        ts = int(time.time())
        await self.db.execute(
            """
            INSERT INTO notified2(target_type, target_id, deal_id, ts)
            VALUES (:target_type, :target_id, :deal_id, :ts)
            ON CONFLICT (target_type, target_id, deal_id) DO UPDATE SET ts = EXCLUDED.ts
            """,
            values={"target_type": target_type, "target_id": target_id, "deal_id": deal_id, "ts": ts},
        )
        await self.db.execute(
            """
            INSERT INTO last_notified2(target_type, target_id, ts)
            VALUES (:target_type, :target_id, :ts)
            ON CONFLICT (target_type, target_id) DO UPDATE SET ts = EXCLUDED.ts
            """,
            values={"target_type": target_type, "target_id": target_id, "ts": ts},
        )

    async def can_notify_target(self, target_type: str, target_id: int, deal_id: str, cooldown_seconds: int) -> bool:
        cur = await self.db.fetch_one(
            "SELECT 1 FROM notified2 WHERE target_type = :target_type AND target_id = :target_id AND deal_id = :deal_id",
            values={
                "target_type": target_type,
                "target_id": target_id,
                "deal_id": deal_id
            }
        )

        if cur:
            return False

        return True

    async def mark_seen(self, deal_id: str):
        ts = int(time.time())
        await self.db.execute(
            """
            INSERT INTO seen(deal_id, ts) VALUES(:deal_id, :ts)
            ON CONFLICT (deal_id) DO UPDATE SET ts = EXCLUDED.ts
            """,
            values={"deal_id": deal_id, "ts": ts},
        )

    async def upsert_deal(self, deal_id: str, title: str, url: str, upvotes: int = 0):
        """Insert or update a deal record with latest upvotes and timestamps."""
        now = int(time.time())
        # try insert first; if exists, update upvotes and last_checked_ts
        await self.db.execute(
            """
            INSERT INTO deals(deal_id, title, url, first_seen_ts, last_upvotes, last_checked_ts)
            VALUES(:deal_id, :title, :url, :first_seen_ts, :last_upvotes, :last_checked_ts)
            ON CONFLICT (deal_id) DO UPDATE SET
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                last_upvotes = EXCLUDED.last_upvotes,
                last_checked_ts = EXCLUDED.last_checked_ts
            """,
            values={
                "deal_id": deal_id,
                "title": title,
                "url": url,
                "first_seen_ts": now,
                "last_upvotes": int(upvotes or 0),
                "last_checked_ts": now,
            },
        )

    async def get_recent_deals(self, since_seconds: int = 3600, limit: int = 50):
        """Return deals first seen within the last `since_seconds` seconds."""
        cutoff = int(time.time()) - int(since_seconds)
        query = "SELECT deal_id, title, url, first_seen_ts, last_upvotes, last_checked_ts FROM deals WHERE first_seen_ts >= :cutoff ORDER BY first_seen_ts DESC LIMIT :limit"
        rows = await self.db.fetch_all(query, values={"cutoff": cutoff, "limit": limit})
        return [ (r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows ]

    async def get_popular_deals(self, min_upvotes: int = 50, within_seconds: int = 1800):
        """Return deals with last_upvotes >= min_upvotes and first_seen within `within_seconds` seconds."""
        cutoff = int(time.time()) - int(within_seconds)
        query = "SELECT deal_id, title, url, first_seen_ts, last_upvotes, last_checked_ts FROM deals WHERE last_upvotes >= :min_upvotes AND first_seen_ts >= :cutoff ORDER BY last_upvotes DESC"
        rows = await self.db.fetch_all(query, values={"min_upvotes": int(min_upvotes), "cutoff": cutoff})
        return [ (r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows ]

    async def is_seen(self, deal_id: str) -> bool:
        cur = await self.db.fetch_one("SELECT 1 FROM seen WHERE deal_id = :deal_id", values={"deal_id": deal_id})
        return cur is not None

    async def clear_old_data(self, days_to_keep: int = 3):
        """
        Deletes records older than the retention period and updates Postgres statistics.
        """
        # 1. Calculate the cutoff (3 days ago)
        cutoff_ts = int(time.time()) - (days_to_keep * 86400)
        
        # 2. Define the tables to prune
        # Using a transaction ensures that if one fails, the DB stays consistent
        async with self.db.transaction():
            queries = [
                "DELETE FROM deals WHERE last_checked_ts < :cutoff",
                "DELETE FROM seen WHERE ts < :cutoff",
                "DELETE FROM notified2 WHERE ts < :cutoff",
                "DELETE FROM last_notified2 WHERE ts < :cutoff"
            ]
            
            for query in queries:
                await self.db.execute(query, values={"cutoff": cutoff_ts})

        # 3. Update Statistics
        # In Postgres, 'ANALYZE' tells the database to recount the rows.
        # This helps the query planner stay efficient after a mass deletion.
        await self.db.execute("ANALYZE deals")
        await self.db.execute("ANALYZE seen")
        await self.db.execute("ANALYZE notified2")

        print(f"Postgres maintenance complete. Data older than {days_to_keep} days pruned.")

    async def get_popular_subscribers(self):
        return await self.db.fetch_all(
        "SELECT owner_id, target_type, target_id FROM subscriptions WHERE keyword = '__popular__'"
    )