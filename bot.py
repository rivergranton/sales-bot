import discord
import os
import asyncio
import re
from datetime import datetime
import pytz
from database import Database
from parser import parse_sale
from stats import build_stats_response

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
    match = re.search(r'[\s_\-]([A-Za-z]+)$', display_name.strip())
    if match:
        suffix = match.group(1).upper()
        if suffix in TEAM_MAP:
            return TEAM_MAP[suffix]
    return GOATS

def milestone_emoji(count: int) -> str:
    return MILESTONE_EMOJI.get(min(count, 4), "🔥")

def format_currency(n: float) -> str:
    return f"${n:,.0f}"

def build_scoreboard(db: Database) -> str:
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

    lines = []
    lines.append(f"**📊 Sales Scoreboard — {today}**")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(
        f"💰 **Total AV:** {format_currency(summary['total_av'])}   "
        f"📋 **Policies:** {summary['total_deals']}   "
        f"🏆 **Teams on the board:** {summary['teams_active']}/7"
    )
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    medals = ["🥇", "🥈", "🥉"]

    for i, team in enumerate(teams):
        medal     = medals[i] if i < 3 else f"**{i+1}.**"
        team_name = team["team_name"]
        team_av   = format_currency(team["total_av"])
        lines.append(f"\n{medal} **{team_name}** — {team_av} AV")

        for deal in deals_by_team.get(team["team_key"], []):
            count   = rep_counts.get(deal["rep_name"], 1)
            badge   = milestone_emoji(count)
            rep     = f"{deal['rep_name']} {badge}".strip()
            prods   = deal["products"]
            prem    = format_currency(deal["premium"])
            av      = format_currency(deal["premium"] * 12)
            assoc   = deal["association"]
            tags    = deal["deal_tags"]
            tag_str = ""
            if assoc:
                tag_str += f" [{assoc}]"
            if tags:
                tag_str += f" [{tags}]"
            lines.append(f"   ↳ @{rep} — {prods}{tag_str} | {prem}/mo | {av} AV")

    if unscored_keys:
        lines.append("\n─────────────────────────")
        for key in unscored_keys:
            info = TEAM_MAP.get(key, GOATS)
            lines.append(f"➖ {info[0]} — No sales yet")

    last = db.get_last_deal()
    if last:
        count = rep_counts.get(last["rep_name"], 1)
        badge = milestone_emoji(count)
        lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(
            f"⚡ **Last sale:** @{last['rep_name']} {badge} — "
            f"{last['products']} | {format_currency(last['premium'])}/mo"
        )

    lines.append("\n*Resets daily at midnight ET*")
    return "\n".join(lines)

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

    board_text = build_scoreboard(db)

    async for msg in channel.history(limit=20):
        if msg.author == client.user and "Sales Scoreboard" in msg.content:
            await msg.edit(content=board_text)
            return

    await channel.send(board_text)

async def midnight_reset():
    et = pytz.timezone("America/New_York")
    while True:
        now        = datetime.now(et)
        from datetime import timedelta
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
