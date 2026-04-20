from database import Database

def format_currency(n: float) -> str:
    return f"${n:,.0f}"

def build_stats_response(db: Database, period: str) -> str:
    if period == "weekly":
        teams = db.get_team_totals_week()
        reps  = db.get_rep_totals_week()
        label = "This Week"
    else:
        teams = db.get_team_totals_month()
        reps  = db.get_rep_totals_month()
        label = "This Month"

    lines = [f"**📊 Stats — {label}**",
             "━━━━━━━━━━━━━━━━━━━━━━━━━━",
             "**Teams:**"]

    medals = ["🥇", "🥈", "🥉"]
    for i, t in enumerate(teams):
        medal = medals[i] if i < 3 else f"{i+1}."
        lines.append(
            f"{medal} {t['team_name']} — "
            f"{format_currency(t['total_av'])} AV "
            f"({t['deal_count']} deals)"
        )

    lines.append("\n**Top Reps:**")
    for i, r in enumerate(reps[:5]):
        medal = medals[i] if i < 3 else f"{i+1}."
        lines.append(
            f"{medal} @{r['rep_name']} ({r['team_name']}) — "
            f"{format_currency(r['total_av'])} AV "
            f"({r['deal_count']} deals)"
        )

    return "\n".join(lines)


def build_today_response(db: Database) -> str:
    """Show all deals logged today — used to verify database contents."""
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
