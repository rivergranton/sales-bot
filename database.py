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

            CREATE TABLE IF NOT EXISTS daily_summaries (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                team_key    TEXT    NOT NULL,
                team_name   TEXT    NOT NULL,
                total_av    REAL    NOT NULL DEFAULT 0,
                deal_count  INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS daily_rep_summaries (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                rep_name    TEXT    NOT NULL,
                team_key    TEXT    NOT NULL,
                team_name   TEXT    NOT NULL,
                total_av    REAL    NOT NULL DEFAULT 0,
                deal_count  INTEGER NOT NULL DEFAULT 0
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

    def _all_deals(self):
        return "(SELECT * FROM deals UNION ALL SELECT * FROM archived_deals)"

    def get_team_totals_week(self) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  date >= date('now', '-6 days', 'localtime')
            GROUP  BY team_key
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_team_totals_month(self) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
            GROUP  BY team_key
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_for_team_week(self, team_key: str) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT rep_name, team_name,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  date >= date('now', '-6 days', 'localtime')
              AND  team_key = ?
            GROUP  BY rep_name
            ORDER  BY total_av DESC
            LIMIT  10
        """, (team_key,)).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_for_team_month(self, team_key: str) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT rep_name, team_name,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
              AND  team_key = ?
            GROUP  BY rep_name
            ORDER  BY total_av DESC
            LIMIT  10
        """, (team_key,)).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_week(self) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT rep_name, team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  date >= date('now', '-6 days', 'localtime')
            GROUP  BY rep_name
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def get_rep_totals_month(self) -> list[dict]:
        rows = self.conn.execute(f"""
            SELECT rep_name, team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*)          AS deal_count
            FROM   {self._all_deals()}
            WHERE  strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
            GROUP  BY rep_name
            ORDER  BY total_av DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def delete_last_deal_today(self, rep_name: str) -> dict | None:
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
        cur = self.conn.execute("""
            DELETE FROM deals WHERE date = ? AND rep_name = ?
        """, (today_et(), rep_name))
        self.conn.commit()
        return cur.rowcount

    def save_daily_summaries(self):
        """Save today's team and rep totals before archiving."""
        date = today_et()

        # delete any existing summaries for today (in case called twice)
        self.conn.execute("DELETE FROM daily_summaries WHERE date = ?", (date,))
        self.conn.execute("DELETE FROM daily_rep_summaries WHERE date = ?", (date,))

        # save team totals
        teams = self.conn.execute("""
            SELECT team_name, team_key,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*) AS deal_count
            FROM deals WHERE date = ?
            GROUP BY team_key
        """, (date,)).fetchall()

        for t in teams:
            self.conn.execute("""
                INSERT INTO daily_summaries (date, team_key, team_name, total_av, deal_count)
                VALUES (?, ?, ?, ?, ?)
            """, (date, t["team_key"], t["team_name"], t["total_av"], t["deal_count"]))

        # save rep totals
        reps = self.conn.execute("""
            SELECT rep_name, team_key, team_name,
                   SUM(premium) * 12 AS total_av,
                   COUNT(*) AS deal_count
            FROM deals WHERE date = ?
            GROUP BY rep_name
        """, (date,)).fetchall()

        for r in reps:
            self.conn.execute("""
                INSERT INTO daily_rep_summaries
                (date, rep_name, team_key, team_name, total_av, deal_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date, r["rep_name"], r["team_key"], r["team_name"],
                  r["total_av"], r["deal_count"]))

        self.conn.commit()

    def archive_today(self):
        """Save summaries then move today's deals to archived_deals."""
        self.save_daily_summaries()
        self.conn.execute("""
            INSERT INTO archived_deals
            SELECT * FROM deals WHERE date = ?
        """, (today_et(),))
        self.conn.execute("DELETE FROM deals WHERE date = ?", (today_et(),))
        self.conn.commit()
