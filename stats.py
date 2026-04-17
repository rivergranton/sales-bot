from database import Database

def format_currency(n: float) -> str:
    return f"${n:,.0f}"

def build_stats_response(db: Database, period: str) -> str:
    """
    Build a stats summary for 'weekly' or 'monthly' queries.
    Called when someone asks the bot a question like:
    '!stats weekly' or '!stats monthly'
    """
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
