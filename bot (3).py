import discord
import os
import asyncio
import re
from datetime import datetime, timedelta
import pytz
from database import Database
from parser import parse_sale
from stats import build_stats_response, build_today_response

# ── config ───────────────────────────────────────────────────────────────────
SALES_CHANNEL = "🥂-general-chat"
BOARD_CHANNEL = "🥂-sales-scoreboard"
RESET_HOUR_ET = 0  # midnight Eastern

ADMIN_ROLES = {"~", "admin", "fsl", "sdsl", "dsl", "kevin"}

TEAM_MAP = {
    "AVH": ("🏠 AV House",        "AVH"),
    "DD":  ("💎 Diamond Dealers",  "DD"),
    "EMP": ("⛩️ Empire",           "EMP"),
    "FD":  ("🧼 Fresh Dealz",      "FD"),
    "HH":  ("🦴 Health Hounds",    "HH"),
    "RR":  ("🏎️ Redline Revenue",  "RR"),
}
GOATS = ("🐐 GOATs", "GOAT")

MILESTONE_EMOJI = {1: "", 2: "✌️", 3: "🎩", 4: "🔥"}

# ── helpers ──────────────────────────────────────────────────────────────────
def has_admin_role(member: discord.Member) -> bool:
    return any(r.name.lower() in ADMIN_ROLES for r in member.roles)

def get_team(display_name: str):
    name = display_name.strip()
    name_upper = name.upper()

    KEYWORD_MAP = {
        "AV HOUSE": "AVH", "AVH": "AVH",
        "DIAMOND DEALERS": "DD", "DD": "DD",
        "EMPIRE": "EMP", "EMP": "EMP",
        "FRESH DEALZ": "FD", "FRESH DEALS": "FD", "FD": "FD",
        "HEALTH HOUNDS": "HH", "HOUNDS": "HH", "HH": "HH",
        "REDLINE REVENUE": "RR", "REDLINE": "RR", "RR": "RR",
    }
    for keyword, key in KEYWORD_MAP.items():
        if keyword in name_upper:
            return TEAM_MAP[key]

    match = re.search(r"[\s_\-]([A-Za-z]+)$", name)
    if match:
        suffix = match.group(1).upper()
        if suffix in TEAM_MAP:
            return TEAM_MAP[suffix]

    return GOATS

def milestone_emoji(count: int) -> str:
    return MILESTONE_EMOJI.get(min(count, 4), "🔥")

def format_currency(n: float) -> str:
    return f"${n:,.0f}"

def build_scoreboard_messages(db: Database) -> list[str]:
    """
    Build scoreboard as a list of messages to handle 60+ deals.
    Message 1 = header + team rankings + last sale
    Messages 2+ = one per active team with individual deals
    """
    et    = pytz.timezone("America/New_York")
    today = datetime.now(et).strftime("%A, %B %d, %Y")

    teams     = db.get_team_totals_today()
    all_deals = db.get_all_deals_today()
    summary   = db.get_summary_today()

    deals_by_team: dict[str, list] = {}
    for d in all_deals:
        deals_by_team.setdefault(d["team_key"], []).append(d)

    rep_counts: dict[str, int] = {}
    for d in all_deals:
        rep_counts[d["rep_name"]] = rep_counts.get(d["rep_name"], 0) + 1

    all_team_keys = list(TEAM_MAP.keys()) + ["GOAT"]
    scored_keys   = {t["team_key"] for t in teams}
    unscored_keys = [k for k in all_team_keys if k not in scored_keys]

    medals = ["🥇", "🥈", "🥉"]

    # ── Message 1: header + rankings ─────────────────────────────────────────
    header = []
    header.append(f"**📊 Sales Scoreboard — {today}**")
    header.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    header.append(
        f"💰 **Total AV:** {format_currency(summary['total_av'])}   "
        f"📋 **Policies:** {summary['total_deals']}   "
        f"🏆 **Teams on the board:** {summary['teams_active']}/7"
    )
    header.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    for i, team in enumerate(teams):
        medal = medals[i] if i < 3 else f"**{i+1}.**"
        header.append(f"{medal} **{team['team_name']}** — {format_currency(team['total_av'])} AV ({team['deal_count']} deals)")

    if unscored_keys:
        header.append("─────────────────────────")
        for key in unscored_keys:
            info = TEAM_MAP.get(key, GOATS)
            header.append(f"➖ {info[0]} — No sales yet")

    last = db.get_last_deal()
    if last:
        count = rep_counts.get(last["rep_name"], 1)
        badge = milestone_emoji(count)
        header.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        header.append(
            f"⚡ **Last sale:** @{last['rep_name']} {badge} — "
            f"{last['products']} | {format_currency(last['premium'])}/mo"
        )

    header.append("*Resets daily at midnight ET*")
    messages = ["\n".join(header)]

    # ── Messages 2+: one per active team ─────────────────────────────────────
    for i, team in enumerate(teams):
        team_deals = deals_by_team.get(team["team_key"], [])
        if not team_deals:
            continue

        medal     = medals[i] if i < 3 else f"{i+1}."
        lines     = [f"{medal} **{team['team_name']}** — {format_currency(team['total_av'])} AV"]

        for deal in team_deals:
            count   = rep_counts.get(deal["rep_name"], 1)
            badge   = milestone_emoji(count)
            rep     = f"{deal['rep_name']} {badge}".strip()
            prem    = format_currency(deal["premium"])
            av      = format_currency(deal["premium"] * 12)
            tag_str = ""
            if deal["association"]: tag_str += f" [{deal['association']}]"
            if deal["deal_tags"]:   tag_str += f" [{deal['deal_tags']}]"
            lines.append(f"   ↳ @{rep} — {deal['products']}{tag_str} | {prem}/mo | {av} AV")

        messages.append("\n".join(lines))

    return messages


def build_scoreboard(db: Database) -> str:
    """Legacy single-message scoreboard — kept for compatibility."""
    return build_scoreboard_messages(db)[0]

# ── bot ───────────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

client = discord.Client(intents=intents)
db     = Database()

@client.event
async def on_ready():
    print(f"✅ Bot online as {client.user}")
    client.loop.create_task(midnight_reset())

@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    content = message.content.strip()
    content_lower = content.lower()

    # ── stats commands (any channel) ─────────────────────────────────────────
    if content_lower in ("!stats weekly", "!stats week"):
        await message.reply(build_stats_response(db, "weekly"))
        return
    if content_lower in ("!stats monthly", "!stats month"):
        await message.reply(build_stats_response(db, "monthly"))
        return
    if content_lower in ("!stats today", "!today"):
        await message.reply(build_today_response(db))
        return

    # ── !scantoday (re-parse today's sales channel history, admin only) ───────
    if content_lower in ("!scantoday", "!scan"):
        if not has_admin_role(message.author):
            await message.reply("❌ You don't have permission to use that command.")
            return

        sales_channel = discord.utils.get(message.guild.text_channels, name=SALES_CHANNEL)
        if not sales_channel:
            await message.reply(f"❌ Could not find channel: {SALES_CHANNEL}")
            return

        await message.reply("🔍 Scanning today's sales channel history — this may take a moment...")

        from datetime import timezone
        import datetime as dt
        et         = pytz.timezone("America/New_York")
        now_et     = datetime.now(et)
        midnight   = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc = midnight.astimezone(timezone.utc)

        found = 0
        skipped = 0

        async for msg in sales_channel.history(limit=500, after=midnight_utc, oldest_first=True):
            if msg.author.bot:
                continue

            result = await parse_sale(msg.content)
            if not result:
                continue

            rep_name  = msg.author.display_name
            team_info = get_team(rep_name)
            team_name, team_key = team_info

            # check if this exact deal is already logged (same rep, premium, products today)
            existing = db.get_all_deals_today()
            already_logged = any(
                d["rep_name"] == rep_name and
                abs(d["premium"] - result["premium"]) < 0.01 and
                d["products"] == result["products"]
                for d in existing
            )

            if already_logged:
                skipped += 1
                continue

            db.insert_deal(
                rep_name    = rep_name,
                team_name   = team_name,
                team_key    = team_key,
                products    = result["products"],
                premium     = result["premium"],
                association = result.get("association", ""),
                deal_tags   = result.get("deal_tags", ""),
            )
            found += 1

        await update_scoreboard(message.guild)
        await message.reply(
            f"✅ Scan complete! Found **{found}** new deal(s), skipped **{skipped}** already logged.\n"
            f"Scoreboard updated!"
        )
        return

    # ── admin correction commands (any channel) ───────────────────────────────
    if content_lower.startswith("!remove"):
        if not has_admin_role(message.author):
            await message.reply("❌ You don't have permission to use that command.")
            return

        # get mentioned user
        if not message.mentions:
            await message.reply("❌ Please tag the rep — example: `!remove @username`")
            return

        target = message.mentions[0]
        target_name = target.display_name

        if content_lower.startswith("!removeall"):
            # remove ALL of target's deals today
            count = db.delete_all_deals_today(target_name)
            if count == 0:
                await message.reply(f"⚠️ No deals found today for **{target_name}**.")
            else:
                await message.reply(f"✅ Removed all {count} deal(s) for **{target_name}** today. Scoreboard updated.")
                await update_scoreboard(message.guild)
        else:
            # remove only the most recent deal
            deal = db.delete_last_deal_today(target_name)
            if not deal:
                await message.reply(f"⚠️ No deals found today for **{target_name}**.")
            else:
                await message.reply(
                    f"✅ Removed last deal for **{target_name}** — "
                    f"{deal['products']} | {format_currency(deal['premium'])}/mo. "
                    f"Scoreboard updated."
                )
                await update_scoreboard(message.guild)
        return

    # ── !refreshscoreboard (any channel, admin only) ─────────────────────────
    if content_lower in ("!refreshscoreboard", "!refresh"):
        if not has_admin_role(message.author):
            await message.reply("❌ You don't have permission to use that command.")
            return
        await update_scoreboard(message.guild)
        await message.reply("✅ Scoreboard refreshed!")
        return

    # ── !add (manual entry, admin only) ──────────────────────────────────────
    if content_lower.startswith("!add "):
        if not has_admin_role(message.author):
            await message.reply("❌ You don't have permission to use that command.")
            return

        if not message.mentions:
            await message.reply("❌ Please tag the rep — example: `!add @username $532 SA 20MG Diamond`")
            return

        target      = message.mentions[0]
        target_name = target.display_name

        # strip the "!add @mention " prefix to get just the sale text
        # message.content after mentions looks like "!add @username $532SA..."
        # we remove the command and the mention to isolate the sale string
        sale_text = re.sub(r'<@!?\d+>', '', content).replace('!add', '').strip()

        if not sale_text:
            await message.reply("❌ No sale info found — example: `!add @username $532 SA 20MG Diamond`")
            return

        result = await parse_sale(sale_text)
        if not result:
            await message.reply(f"❌ Couldn't parse that sale. Try: `!add @{target_name} $532 SA 20MG Diamond`")
            return

        team_info = get_team(target_name)
        team_name, team_key = team_info

        db.insert_deal(
            rep_name    = target_name,
            team_name   = team_name,
            team_key    = team_key,
            products    = result["products"],
            premium     = result["premium"],
            association = result.get("association", ""),
            deal_tags   = result.get("deal_tags", ""),
        )

        count = db.get_rep_deal_count_today(target_name)
        badge = milestone_emoji(count)
        av    = result["premium"] * 12

        confirm = (
            f"✅ Manually logged! **{format_currency(result['premium'])} {result['products']}** "
            f"for **{target_name}** → {team_name}"
        )
        if result.get("association"):
            confirm += f" · {result['association']}"
        if result.get("deal_tags"):
            confirm += f" · {result['deal_tags']}"
        confirm += f"\nPremium: {format_currency(result['premium'])}/mo · AV: {format_currency(av)}"

        if count == 2:
            confirm += f"\n✌️ Double down {target_name}! 2 policies today!"
        elif count == 3:
            confirm += f"\n🎩 Hat trick {target_name}! 3 policies today!"
        elif count >= 4:
            confirm += f"\n🔥 {target_name} is ON FIRE! {count} policies today!"

        await message.reply(confirm)
        await update_scoreboard(message.guild)
        return

    # ── sale parsing (sales channel only) ────────────────────────────────────
    if message.channel.name != SALES_CHANNEL:
        return

    result = await parse_sale(message.content)
    if not result:
        return

    display   = message.author.display_name
    team_info = get_team(display)
    team_name, team_key = team_info

    db.insert_deal(
        rep_name    = display,
        team_name   = team_name,
        team_key    = team_key,
        products    = result["products"],
        premium     = result["premium"],
        association = result.get("association", ""),
        deal_tags   = result.get("deal_tags", ""),
    )

    count = db.get_rep_deal_count_today(display)
    av    = result["premium"] * 12

    confirm = (
        f"✅ Logged! **{format_currency(result['premium'])} {result['products']}** "
        f"for {team_name}"
    )
    if result.get("association"):
        confirm += f" · {result['association']}"
    if result.get("deal_tags"):
        confirm += f" · {result['deal_tags']}"
    confirm += (
        f"\nPremium: {format_currency(result['premium'])}/mo · "
        f"AV: {format_currency(av)}"
    )

    if count == 2:
        confirm += f"\n✌️ Double down @{display}! 2 policies today!"
    elif count == 3:
        confirm += f"\n🎩 Hat trick @{display}! 3 policies today!"
    elif count >= 4:
        confirm += f"\n🔥 @{display} is ON FIRE! {count} policies today!"

    await message.reply(confirm)
    await update_scoreboard(message.guild)

async def update_scoreboard(guild: discord.Guild):
    channel = discord.utils.get(guild.text_channels, name=BOARD_CHANNEL)
    if not channel:
        print(f"⚠️  Could not find channel: {BOARD_CHANNEL}")
        return

    new_messages = build_scoreboard_messages(db)

    # collect all existing bot scoreboard messages (header + team detail messages)
    existing = []
    try:
        async for msg in channel.history(limit=200):
            if msg.author == client.user and (
                "Sales Scoreboard" in msg.content or
                any(emoji in msg.content for emoji in ["🥇","🥈","🥉","➖"]) and "AV" in msg.content
            ):
                existing.append(msg)
    except Exception:
        pass

    existing = list(reversed(existing))  # oldest first

    # edit existing messages where possible, post new ones if needed
    for i, text in enumerate(new_messages):
        if i < len(existing):
            try:
                await existing[i].edit(content=text)
            except Exception:
                await channel.send(text)
        else:
            sent = await channel.send(text)
            if i == 0:
                try:
                    await sent.pin()
                except Exception:
                    pass

    # delete any leftover old messages if we now have fewer messages than before
    for leftover in existing[len(new_messages):]:
        try:
            await leftover.delete()
        except Exception:
            pass

async def midnight_reset():
    et = pytz.timezone("America/New_York")
    while True:
        now        = datetime.now(et)

        next_reset = now.replace(hour=RESET_HOUR_ET, minute=0, second=5, microsecond=0)
        if next_reset <= now:
            next_reset += timedelta(days=1)
        wait_secs = (next_reset - now).total_seconds()
        await asyncio.sleep(wait_secs)

        db.archive_today()

        for guild in client.guilds:
            ch = discord.utils.get(guild.text_channels, name=BOARD_CHANNEL)
            if ch:
                et_now = datetime.now(et).strftime("%A, %B %d, %Y")
                await ch.send(
                    f"🌅 **New day, new grind!** Scoreboard reset for {et_now}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"No sales yet — who's first on the board? 👀"
                )

client.run(os.environ["DISCORD_TOKEN"])
