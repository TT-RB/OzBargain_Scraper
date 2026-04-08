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
        now = int(time.time())
        cur = await self.db.fetch_one("SELECT 1 FROM notified2 WHERE target_type = :target_type AND target_id = :target_id AND deal_id = :deal_id", values={"target_type": target_type, "target_id": target_id, "deal_id": deal_id})
        if cur:
            return False
        row = await self.db.fetch_one("SELECT ts FROM last_notified2 WHERE target_type = :target_type AND target_id = :target_id", values={"target_type": target_type, "target_id": target_id})
        if row:
            last_ts = row[0]
            if now - last_ts < cooldown_seconds:
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

    async def is_seen(self, deal_id: str) -> bool:
        cur = await self.db.fetch_one("SELECT 1 FROM seen WHERE deal_id = :deal_id", values={"deal_id": deal_id})
        return cur is not None

