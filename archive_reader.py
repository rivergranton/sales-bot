"""
Reads the daily-scoreboard-archive channel and aggregates
stats for weekly (Fri-Thu) or monthly (calendar month) periods.
"""
import re
from datetime import datetime, timedelta
import pytz

ET = pytz.timezone("America/New_York")

def parse_currency(s):
    """Parse $1,234 or $1,234.56 into float."""
    try:
        return float(re.sub(r"[^\d.]", "", s))
    except:
        return 0.0

def parse_archive_message(text):
    """
    Parse a single scoreboard team message block.
    Returns list of dicts: {rep_name, team_name, team_key, premium, av, products, association, deal_tags}
    """
    deals = []
    lines = text.split("\n")

    # extract team name from header line e.g. "🥇 **💎 Diamond Dealers** — **$48,028 AV** · 8 deals"
    team_name = ""
    team_key  = ""
    for line in lines:
        m = re.search(r"\*\*(.+?)\*\* — \*\*\$[\d,.]+ AV\*\*", line)
        if m:
            raw = m.group(1).strip()
            # strip medal emojis at start
            raw = re.sub(r"^[\U00010000-\U0010ffff\s]+", "", raw).strip()
            team_name = raw
            # derive team_key
            KEY_MAP = {
                "AV House": "AVH", "Diamond Dealers": "DD", "Empire": "EMP",
                "Fresh Dealz": "FD", "Health Hounds": "HH", "GOATs": "GOAT",
            }
            for k, v in KEY_MAP.items():
                if k.lower() in team_name.lower():
                    team_key = v
                    break
            break

    # parse individual deals
    i = 0
    while i < len(lines):
        line = lines[i]
        # rep line: "  ↳ **@RepName** 🎩" or "   ↳ @RepName ✌️"
        rep_match = re.search(r"↳\s+\**@([^*\n]+?)\**\s*[✌️🎩🔥]*\s*$", line.rstrip())
        if rep_match:
            rep_name = rep_match.group(1).strip()
            products = ""
            association = ""
            deal_tags = ""
            premium = 0.0
            av = 0.0

            # next line: products/tags in italics
            if i + 1 < len(lines):
                prod_line = lines[i+1].strip().strip("*").strip()
                # extract known tags
                for tag in ["OTF", "OCC", "OCK", "Flip", "Partial", "FNB"]:
                    if tag.lower() in prod_line.lower():
                        deal_tags = tag
                        prod_line = re.sub(re.escape(tag), "", prod_line, flags=re.IGNORECASE).strip()
                # known associations
                for assoc in ["Executive Diamond", "Diamond", "Sapphire", "Emerald",
                              "Ruby", "Entrepreneur", "Elite"]:
                    if assoc.lower() in prod_line.lower():
                        association = assoc
                        prod_line = re.sub(re.escape(assoc), "", prod_line, flags=re.IGNORECASE).strip()
                products = prod_line.strip(" +·,")

            # next line after that: "💵 $873/mo · 📈 $10,476 AV"
            if i + 2 < len(lines):
                money_line = lines[i+2]
                prem_m = re.search(r"💵\s*\$([0-9,]+(?:\.[0-9]+)?)/mo", money_line)
                av_m   = re.search(r"📈\s*\$([0-9,]+(?:\.[0-9]+)?)\s*AV", money_line)
                if prem_m:
                    premium = parse_currency(prem_m.group(1))
                if av_m:
                    av = parse_currency(av_m.group(1))

            if rep_name and (premium > 0 or av > 0):
                deals.append({
                    "rep_name":    rep_name,
                    "team_name":   team_name,
                    "team_key":    team_key,
                    "products":    products,
                    "premium":     premium,
                    "av":          av if av > 0 else premium * 12,
                    "association": association,
                    "deal_tags":   deal_tags,
                })
            i += 3
            continue
        i += 1

    return deals


def get_week_date_range():
    """Returns (friday_start, thursday_end) for the most recent completed Fri-Thu week."""
    now = datetime.now(ET)
    # find last Friday
    days_since_friday = (now.weekday() - 4) % 7
    last_friday = (now - timedelta(days=days_since_friday)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    last_thursday = last_friday + timedelta(days=6)
    return last_friday, last_thursday


def get_month_date_range():
    """Returns (first_day, last_day) for the previous calendar month."""
    now = datetime.now(ET)
    first_of_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = first_of_this - timedelta(seconds=1)
    last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return last_month_start, last_month_end


def parse_archive_date(header_text):
    """Parse date from archive header like '📅 **Daily Scoreboard Archive — Monday, April 20, 2026**'"""
    m = re.search(r"Archive\s*[—-]\s*\w+,?\s*(\w+ \d+,? \d{4})", header_text)
    if m:
        try:
            return datetime.strptime(m.group(1).replace(",", ""), "%B %d %Y")
        except:
            pass
    return None


async def read_archive_for_period(channel, period: str):
    """
    Read the daily-scoreboard-archive channel and aggregate all deals
    for the given period ('weekly' or 'monthly').
    Returns aggregated data dict.
    """
    if period == "weekly":
        start, end = get_week_date_range()
    else:
        start, end = get_month_date_range()

    all_deals = []
    fnb_agents = set()
    current_date = None
    in_range = False

    # collect messages oldest first
    messages = []
    async for msg in channel.history(limit=2000, oldest_first=True):
        messages.append(msg)

    for msg in messages:
        text = msg.content

        # detect date header
        if "Daily Scoreboard Archive" in text:
            parsed = parse_archive_date(text)
            if parsed:
                current_date = ET.localize(parsed)
                in_range = start <= current_date <= end
            continue

        if not in_range:
            continue

        # skip non-team messages
        if "▬▬▬" not in text or "AV" not in text:
            continue

        # skip the main header message
        if "Sales Scoreboard" in text or "Total AV" in text:
            continue

        deals = parse_archive_message(text)
        for d in deals:
            all_deals.append(d)
            if "FNB" in d.get("deal_tags", "").upper():
                fnb_agents.add(d["rep_name"])

    # aggregate by team
    teams = {}
    reps  = {}
    for d in all_deals:
        tk = d["team_key"] or "GOAT"
        tn = d["team_name"] or "🐐 GOATs"
        av = d["av"]

        if tk not in teams:
            teams[tk] = {"team_key": tk, "team_name": tn, "total_av": 0, "deal_count": 0}
        teams[tk]["total_av"]    += av
        teams[tk]["deal_count"]  += 1

        rn = d["rep_name"]
        if rn not in reps:
            reps[rn] = {"rep_name": rn, "team_key": tk, "team_name": tn, "total_av": 0, "deal_count": 0}
        reps[rn]["total_av"]   += av
        reps[rn]["deal_count"] += 1

    # sort
    teams_list = sorted(teams.values(), key=lambda x: x["total_av"], reverse=True)
    reps_list  = sorted(reps.values(),  key=lambda x: x["total_av"], reverse=True)

    total_av     = sum(d["av"] for d in all_deals)
    total_deals  = len(all_deals)
    active_agents = len(set(d["rep_name"] for d in all_deals))
    active_teams  = len(teams)

    fnb_list = [{"rep_name": r, "team_name": reps[r]["team_name"],
                 "team_key": reps[r]["team_key"]} for r in fnb_agents if r in reps]

    return {
        "teams":         teams_list,
        "reps":          reps_list,
        "total_av":      total_av,
        "total_deals":   total_deals,
        "active_agents": active_agents,
        "active_teams":  active_teams,
        "fnb_agents":    fnb_list,
        "start":         start,
        "end":           end,
    }
