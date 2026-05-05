from database import Database

def fmt(n): return f"${n:,.0f}"

OFFICES = {
    "beachside": {"name": "🏖️ Beachside Office", "teams": ["AVH","FD","EMP"]},
    "orlando":   {"name": "🌴 Orlando Office",    "teams": ["HH","DD"]},
}
GOAT_KEY = "GOAT"
TEAM_DISPLAY = {
    "AVH":"🏠 AV House","DD":"💎 Diamond Dealers","EMP":"⛩️ Empire",
    "FD":"🧼 Fresh Dealz","HH":"🦴 Health Hounds","GOAT":"🐐 GOATs",
}
OFFICE_DISPLAY = {
    "AVH":"🏖️ Beachside","DD":"🌴 Orlando","EMP":"🏖️ Beachside",
    "FD":"🏖️ Beachside","HH":"🌴 Orlando","GOAT":"🐐",
}
MEDALS = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]


def build_period_stats_from_data(data: dict, period: str) -> list[str]:
    """
    Build stats messages from pre-aggregated archive data dict.
    data keys: teams, reps, total_av, total_deals, active_agents,
               active_teams, fnb_agents, start, end
    """
    is_monthly  = period == "monthly"
    rep_limit   = 10 if is_monthly else 5

    teams_by_key = {t["team_key"]: t for t in data["teams"]}
    all_reps     = data["reps"]
    fnb_agents   = data.get("fnb_agents", [])

    # office totals
    office_avs = {}
    office_deals = {}
    for ok, od in OFFICES.items():
        office_avs[ok]   = sum(teams_by_key.get(tk, {}).get("total_av", 0)   for tk in od["teams"])
        office_deals[ok] = sum(teams_by_key.get(tk, {}).get("deal_count", 0) for tk in od["teams"])

    # top reps / top by policies
    top_reps     = all_reps[:10]
    top_policies = sorted(all_reps, key=lambda x: x["deal_count"], reverse=True)[:1]

    # date range label
    start = data.get("start")
    end   = data.get("end")
    if start and end:
        date_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
    else:
        date_label = "This period"

    # ── message 1: header ─────────────────────────────────────────────────────
    m1 = []
    period_title = f"Monthly Stats — {start.strftime('%B %Y')}" if is_monthly else f"Weekly Stats — {date_label}"
    m1.append(f"**📊 {period_title}**")
    m1.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    m1.append("🍊🥂 **Medguard & Mimosas**")
    m1.append(
        f"💰 **Total AV:** {fmt(data['total_av'])}   "
        f"📋 **Deals:** {data['total_deals']}   "
        f"👥 **Agents:** {data['active_agents']}   "
        f"🏆 **Teams:** {data['active_teams']}/6"
    )
    m1.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # head to head
    b_av = office_avs["beachside"]
    o_av = office_avs["orlando"]
    m1.append("\n**Office Head to Head**")
    if b_av >= o_av:
        lead, trail = ("🏖️ Beachside", b_av), ("🌴 Orlando", o_av)
    else:
        lead, trail = ("🌴 Orlando", o_av), ("🏖️ Beachside", b_av)
    m1.append(f"🥇 **{lead[0]}** — {fmt(lead[1])} AV")
    m1.append(f"🥈 **{trail[0]}** — {fmt(trail[1])} AV")
    diff = lead[1] - trail[1]
    if diff > 0:
        m1.append(f"*{lead[0]} leads by {fmt(diff)} AV*")
    else:
        m1.append("*Offices are tied!*")

    # agent of week/month
    aow_label = "Agent of the Month" if is_monthly else "Agent of the Week"
    m1.append(f"\n**{aow_label}**")
    if top_reps:
        top = top_reps[0]
        pol = top_policies[0] if top_policies else None
        same = pol and pol["rep_name"] == top["rep_name"]
        team_label = TEAM_DISPLAY.get(top["team_key"], top["team_name"])
        off_label  = OFFICE_DISPLAY.get(top["team_key"], "")
        note = f" · Also led in policies ({top['deal_count']} deals)" if same else ""
        m1.append(f"🏆 **@{top['rep_name']}**")
        m1.append(f"*{fmt(top['total_av'])} AV · {top['deal_count']} deals · {team_label} · {off_label}*{note}")

        if not same and pol:
            m1.append("\n**Most Policies Sold**")
            pt = TEAM_DISPLAY.get(pol["team_key"], pol["team_name"])
            po = OFFICE_DISPLAY.get(pol["team_key"], "")
            m1.append(f"📋 **@{pol['rep_name']}**")
            m1.append(f"*{pol['deal_count']} deals · {fmt(pol['total_av'])} AV · {pt} · {po}*")

    # FNB
    if fnb_agents:
        m1.append("\n**First New Business**")
        m1.append("*Issued their first policy this period!*")
        for a in fnb_agents:
            tl = TEAM_DISPLAY.get(a.get("team_key",""), a.get("team_name",""))
            m1.append(f"🎉 @{a['rep_name']} · *{tl}*")

    messages = ["\n".join(m1)]

    # ── message 2 (monthly): top 10 division-wide ─────────────────────────────
    if is_monthly and top_reps:
        pol = top_policies[0] if top_policies else None
        m2 = ["**Top 10 Agents — Division Wide**", "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"]
        for i, r in enumerate(top_reps):
            medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
            tl    = TEAM_DISPLAY.get(r["team_key"], r["team_name"])
            ol    = OFFICE_DISPLAY.get(r["team_key"], "")
            badges = []
            if i == 0:
                badges.append("Agent of the Month")
                if pol and pol["rep_name"] == r["rep_name"]:
                    badges.append("Most policies")
            elif pol and pol["rep_name"] == r["rep_name"]:
                badges.append(f"Most policies ({pol['deal_count']} deals)")
            badge_str = f" *[{' · '.join(badges)}]*" if badges else ""
            m2.append(f"{medal} **@{r['rep_name']}**{badge_str}")
            m2.append(f"   *{fmt(r['total_av'])} AV · {r['deal_count']} deals · {tl} · {ol}*")
        m2.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        messages.append("\n".join(m2))

    # ── office breakdowns ──────────────────────────────────────────────────────
    for ok, od in OFFICES.items():
        o_av    = office_avs[ok]
        o_deals = office_deals[ok]
        lines   = []
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        lines.append(f"**{od['name']}** — **{fmt(o_av)} AV** · {o_deals} deals")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

        for tk in od["teams"]:
            team = teams_by_key.get(tk)
            tn   = TEAM_DISPLAY.get(tk, tk)
            if not team or team["total_av"] == 0:
                lines.append(f"\n**{tn}** — No sales this period")
                lines.append("─────────────────────────")
                continue
            lines.append(f"\n**{tn}** — {fmt(team['total_av'])} AV · {team['deal_count']} deals")
            lines.append("─────────────────────────")
            team_reps = sorted(
                [r for r in all_reps if r["team_key"] == tk],
                key=lambda x: x["total_av"], reverse=True
            )[:rep_limit]
            for i, r in enumerate(team_reps):
                medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
                lines.append(f"  {medal} @{r['rep_name']} — **{fmt(r['total_av'])} AV** · {r['deal_count']} deals")

        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        messages.append("\n".join(lines))

    # ── GOATs ──────────────────────────────────────────────────────────────────
    goat = teams_by_key.get(GOAT_KEY)
    if goat and goat["total_av"] > 0:
        lines = ["▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"]
        lines.append(f"**🐐 GOATs** — **{fmt(goat['total_av'])} AV** · {goat['deal_count']} deals")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        goat_reps = sorted(
            [r for r in all_reps if r["team_key"] == GOAT_KEY],
            key=lambda x: x["total_av"], reverse=True
        )[:rep_limit]
        for i, r in enumerate(goat_reps):
            medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
            lines.append(f"  {medal} @{r['rep_name']} — **{fmt(r['total_av'])} AV** · {r['deal_count']} deals")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        messages.append("\n".join(lines))

    return messages


def build_today_response(db: Database) -> str:
    deals   = db.get_all_deals_today()
    summary = db.get_summary_today()
    if not deals:
        return "📭 No deals logged in the database for today yet."
    lines = [
        f"**📊 Today's logged deals — {summary['total_deals']} total | {fmt(summary['total_av'])} AV**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ]
    for d in deals:
        av  = d['premium'] * 12
        tag = ""
        if d['association']: tag += f" [{d['association']}]"
        if d['deal_tags']:   tag += f" [{d['deal_tags']}]"
        lines.append(
            f"• @{d['rep_name']} ({d['team_name']}) — "
            f"{d['products']}{tag} | {fmt(d['premium'])}/mo | {fmt(av)} AV"
        )
    return "\n".join(lines)


def build_stats_response(db: Database, period: str) -> str:
    return "Use !stats weekly or !stats monthly to see period stats."
