from database import Database

def format_currency(n: float) -> str:
    return f"${n:,.0f}"

# ── office structure ──────────────────────────────────────────────────────────
OFFICES = {
    "beachside": {
        "name":  "🏖️ Beachside Office",
        "teams": ["AVH", "FD", "EMP"],
    },
    "orlando": {
        "name":  "🌴 Orlando Office",
        "teams": ["HH", "DD", "RR"],
    },
}
GOAT_KEY = "GOAT"

TEAM_DISPLAY = {
    "AVH": "🏠 AV House",
    "DD":  "💎 Diamond Dealers",
    "EMP": "⛩️ Empire",
    "FD":  "🧼 Fresh Dealz",
    "HH":  "🦴 Health Hounds",
    "RR":  "🏎️ Redline Revenue",
    "GOAT":"🐐 GOATs",
}

def build_office_stats(db: Database, period: str) -> list[str]:
    """
    Build office-structured stats as a list of messages (one per office + GOATs).
    period = 'weekly' or 'monthly'
    """
    if period == "weekly":
        teams    = {t["team_key"]: t for t in db.get_team_totals_week()}
        get_reps = db.get_rep_totals_for_team_week
        label    = "This Week"
    else:
        teams    = {t["team_key"]: t for t in db.get_team_totals_month()}
        get_reps = db.get_rep_totals_for_team_month
        label    = "This Month"

    medals = ["🥇", "🥈", "🥉", "4.", "5.", "6.", "7.", "8.", "9.", "10."]

    # ── calculate office totals ───────────────────────────────────────────────
    office_totals = {}
    for office_key, office in OFFICES.items():
        total = sum(teams.get(tk, {}).get("total_av", 0) for tk in office["teams"])
        office_totals[office_key] = total

    beachside_av = office_totals["beachside"]
    orlando_av   = office_totals["orlando"]

    # ── Message 1: head to head summary ──────────────────────────────────────
    summary = []
    summary.append(f"**📊 Sales Stats — {label}**")
    summary.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # head to head
    if beachside_av > orlando_av:
        leader, trailer = "🏖️ Beachside", "🌴 Orlando"
        leader_av, trailer_av = beachside_av, orlando_av
    elif orlando_av > beachside_av:
        leader, trailer = "🌴 Orlando", "🏖️ Beachside"
        leader_av, trailer_av = orlando_av, beachside_av
    else:
        leader = trailer = None

    summary.append("**🏆 Office Head to Head**")
    if leader:
        diff = leader_av - trailer_av
        summary.append(f"🥇 **{leader}** — {format_currency(leader_av)} AV")
        summary.append(f"🥈 **{trailer}** — {format_currency(trailer_av)} AV")
        summary.append(f"*{leader} leads by {format_currency(diff)} AV*")
    else:
        summary.append(f"🤝 **Tied!** Both offices at {format_currency(beachside_av)} AV")

    summary.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    summary.append("**Office totals:**")
    summary.append(f"🏖️ Beachside — {format_currency(beachside_av)} AV")
    summary.append(f"🌴 Orlando — {format_currency(orlando_av)} AV")

    goat_data = teams.get(GOAT_KEY)
    if goat_data:
        summary.append(f"🐐 GOATs — {format_currency(goat_data['total_av'])} AV")

    messages = ["\n".join(summary)]

    # ── Messages 2-3: one per office ─────────────────────────────────────────
    for office_key, office in OFFICES.items():
        office_av = office_totals[office_key]
        lines = []
        lines.append(f"**{office['name']} — {format_currency(office_av)} AV**")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

        for team_key in office["teams"]:
            team = teams.get(team_key)
            team_name = TEAM_DISPLAY.get(team_key, team_key)

            if not team:
                lines.append(f"\n**{team_name}** — No sales this period")
                lines.append("─────────────────────────")
                continue

            lines.append(f"\n**{team_name}** — {format_currency(team['total_av'])} AV ({team['deal_count']} deals)")
            lines.append("─────────────────────────")

            reps = get_reps(team_key)
            if reps:
                for i, r in enumerate(reps):
                    medal = medals[i] if i < len(medals) else f"{i+1}."
                    lines.append(
                        f"  {medal} @{r['rep_name']} — "
                        f"**{format_currency(r['total_av'])} AV** "
                        f"({r['deal_count']} deals)"
                    )
            else:
                lines.append("  *No rep data for this period*")

        messages.append("\n".join(lines))

    # ── Message 4: GOATs ─────────────────────────────────────────────────────
    goat_team = teams.get(GOAT_KEY)
    if goat_team:
        lines = []
        lines.append(f"**🐐 GOATs — {format_currency(goat_team['total_av'])} AV ({goat_team['deal_count']} deals)**")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

        reps = get_reps(GOAT_KEY)
        for i, r in enumerate(reps):
            medal = medals[i] if i < len(medals) else f"{i+1}."
            lines.append(
                f"  {medal} @{r['rep_name']} — "
                f"**{format_currency(r['total_av'])} AV** "
                f"({r['deal_count']} deals)"
            )
        messages.append("\n".join(lines))

    return messages


def build_today_response(db: Database) -> str:
    deals   = db.get_all_deals_today()
    summary = db.get_summary_today()

    if not deals:
        return "📭 No deals logged in the database for today yet."

    lines = [
        f"**📊 Today's logged deals — {summary['total_deals']} total | {format_currency(summary['total_av'])} AV**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ]
    for d in deals:
        av  = d['premium'] * 12
        tag = ""
        if d['association']: tag += f" [{d['association']}]"
        if d['deal_tags']:   tag += f" [{d['deal_tags']}]"
        lines.append(
            f"• @{d['rep_name']} ({d['team_name']}) — "
            f"{d['products']}{tag} | {format_currency(d['premium'])}/mo | {format_currency(av)} AV"
        )
    return "\n".join(lines)


def build_stats_response(db: Database, period: str) -> str:
    """Legacy single-message stats — kept for compatibility."""
    messages = build_office_stats(db, period)
    return messages[0] if messages else "No data found."
