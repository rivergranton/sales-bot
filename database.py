import sqlite3
import os
from datetime import datetime
import pytz

DB_PATH = os.environ.get("DB_PATH", "sales.db")

ET = pytz.timezone("America/New_York")

def today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")

class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS deals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                rep_name    TEXT    NOT NULL,
                team_name   TEXT    NOT NULL,
                team_key    TEXT    NOT NULL,
                products    TEXT    NOT NULL,
                premium     REAL    NOT NULL,
                association TEXT    DEFAULT '',
                deal_tags   TEXT    DEFAULT '',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS archived_deals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                rep_name    TEXT    NOT NULL,
                team_name   TEXT    NOT NULL,
                team_key    TEXT    NOT NULL,
                products    TEXT    NOT NULL,
                premium     REAL    NOT NULL,
                association TEXT    DEFAULT '',
                deal_tags   TEXT    DEFAULT '',
                created_at  TEXT    NOT NULL
            );
        """)
        self.conn.commit()

    def insert_deal(self, rep_name, team_name, team_key, products,
                    premium, association="", deal_tags=""):
        self.conn.execute("""
            INSERT INTO deals (date, rep_name, team_name, team_key,
                               products, premium, association, deal_tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (today_et(), rep_name, team_name, team_key,
              products, premium, association, deal_tags))
        self.conn.commit()

    def get_team_totals_today(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   deals
            WHERE  date = ?
            GROUP  BY team_key
            ORDER  BY total_av DESC
        """, (today_et(),)).fetchall()
        return [dict(r) for r in rows]

    def get_all_deals_today(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT rep_name, team_name, team_key,
                   products, premium, association, deal_tags
            FROM   deals
            WHERE  date = ?
            ORDER  BY id ASC
        """, (today_et(),)).fetchall()
        return [dict(r) for r in rows]

    def get_last_deal(self) -> dict | None:
        row = self.conn.execute("""
            SELECT rep_name, team_name, products, premium, association, deal_tags
            FROM   deals
            WHERE  date = ?
            ORDER  BY id DESC
            LIMIT  1
        """, (today_et(),)).fetchone()
        return dict(row) if row else None

    def get_rep_deal_count_today(self, rep_name: str) -> int:
        row = self.conn.execute("""
            SELECT COUNT(*) AS cnt FROM deals
            WHERE date = ? AND rep_name = ?
        """, (today_et(), rep_name)).fetchone()
        return row["cnt"] if row else 0

    def get_summary_today(self) -> dict:
        row = self.conn.execute("""
            SELECT COALESCE(SUM(premium) * 12, 0) AS total_av,
                   COUNT(*)                        AS total_deals,
                   COUNT(DISTINCT team_key)        AS teams_active
            FROM   deals
            WHERE  date = ?
        """, (today_et(),)).fetchone()
        return dict(row)

    # ── historical queries ────────────────────────────────────────────────────

    def get_team_totals_week(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   (SELECT * FROM deals UNION ALL SELECT * FROM archived_deals)
            WHERE  date >= date('now', '-6 days', 'localtime')
            GROUP  BY team_key
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_team_totals_month(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   (SELECT * FROM deals UNION ALL SELECT * FROM archived_deals)
            WHERE  strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
            GROUP  BY team_key
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_week(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT rep_name, team_name,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   (SELECT * FROM deals UNION ALL SELECT * FROM archived_deals)
            WHERE  date >= date('now', '-6 days', 'localtime')
            GROUP  BY rep_name
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_month(self) -> list[dict]:
        rows = self.conn.execute("""
            SELECT rep_name, team_name,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   (SELECT * FROM deals UNION ALL SELECT * FROM archived_deals)
            WHERE  strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
            GROUP  BY rep_name
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def archive_today(self):
        """Move today's deals into archived_deals at midnight."""
        self.conn.execute("""
            INSERT INTO archived_deals
            SELECT * FROM deals WHERE date = ?
        """, (today_et(),))
        self.conn.execute("DELETE FROM deals WHERE date = ?", (today_et(),))
        self.conn.commit()

    def delete_last_deal_today(self, rep_name: str) -> dict | None:
        """Delete the most recent deal today for a rep. Returns the deleted deal or None."""
        row = self.conn.execute("""
            SELECT id, products, premium FROM deals
            WHERE date = ? AND rep_name = ?
            ORDER BY id DESC LIMIT 1
        """, (today_et(), rep_name)).fetchone()
        if not row:
            return None
        self.conn.execute("DELETE FROM deals WHERE id = ?", (row["id"],))
        self.conn.commit()
        return dict(row)

    def delete_all_deals_today(self, rep_name: str) -> int:
        """Delete all of a rep's deals today. Returns count deleted."""
        cur = self.conn.execute("""
            DELETE FROM deals WHERE date = ? AND rep_name = ?
        """, (today_et(), rep_name))
        self.conn.commit()
        return cur.rowcount
