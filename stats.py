from database import Database

def fmt(n): return f"${n:,.0f}"

OFFICES = {
    "beachside": {"name": "🏖️ Beachside Office", "teams": ["AVH","FD","EMP"]},
    "orlando":   {"name": "🌴 Orlando Office",    "teams": ["HH","DD","RR"]},
}
GOAT_KEY = "GOAT"
TEAM_DISPLAY = {
    "AVH":"🏠 AV House","DD":"💎 Diamond Dealers","EMP":"⛩️ Empire",
    "FD":"🧼 Fresh Dealz","HH":"🦴 Health Hounds","RR":"🏎️ Redline Revenue","GOAT":"🐐 GOATs",
}
OFFICE_DISPLAY = {
    "AVH":"🏖️ Beachside","DD":"🌴 Orlando","EMP":"🏖️ Beachside",
    "FD":"🏖️ Beachside","HH":"🌴 Orlando","RR":"🌴 Orlando","GOAT":"🐐",
}
MEDALS = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]

def _office_totals(teams_by_key):
    return {
        ok: sum(teams_by_key.get(tk,{}).get("total_av",0) for tk in od["teams"])
        for ok, od in OFFICES.items()
    }

def _office_deal_counts(teams_by_key):
    return {
        ok: sum(teams_by_key.get(tk,{}).get("deal_count",0) for tk in od["teams"])
        for ok, od in OFFICES.items()
    }

def build_period_stats(db: Database, period: str) -> list[str]:
    """
    Build full stats post as list of Discord messages.
    period = 'weekly' or 'monthly'
    """
    is_monthly  = period == "monthly"
    rep_limit   = 10 if is_monthly else 5
    top_n_label = "Top 10 Agents" if is_monthly else "Agent of the Week"

    teams_list  = db.get_team_totals_period(period)
    teams       = {t["team_key"]: t for t in teams_list}
    div         = db.get_division_summary_period(period)
    fnb_agents  = db.get_fnb_agents_period(period)
    top_reps    = db.get_rep_totals_period(period, limit=10)
    top_policies= db.get_top_policy_count_period(period, limit=1)

    office_avs   = _office_totals(teams)
    office_deals = _office_deal_counts(teams)

    # ── message 1: header + division + h2h + agent(s) + fnb ──────────────────
    m1 = []

    # division block
    m1.append("🍊🥂 **Medguard & Mimosas**")
    m1.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    m1.append(
        f"💰 **Total AV:** {fmt(div['total_av'])}   "
        f"📋 **Deals:** {div['total_deals']}   "
        f"👥 **Agents:** {div['active_agents']}   "
        f"🏆 **Teams:** {div['active_teams']}/7"
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

    # agent of week/month + most policies
    m1.append(f"\n**{'Agent of the Month' if is_monthly else 'Agent of the Week'}**")
    if top_reps:
        top = top_reps[0]
        office_label = OFFICE_DISPLAY.get(top["team_key"], "")
        team_label   = TEAM_DISPLAY.get(top["team_key"], top["team_name"])

        # check if same person leads policies
        pol_leader = top_policies[0] if top_policies else None
        same_person = pol_leader and pol_leader["rep_name"] == top["rep_name"]

        note = ""
        if same_person:
            note = f" · Also led in policies ({pol_leader['deal_count']} deals)"

        m1.append(f"🏆 **@{top['rep_name']}**")
        m1.append(f"*{fmt(top['total_av'])} AV · {top['deal_count']} deals · {team_label} · {office_label}*{note}")

        # most policies — only if different person
        if not same_person and pol_leader:
            m1.append(f"\n**Most Policies Sold**")
            pol_team = TEAM_DISPLAY.get(pol_leader["team_key"], pol_leader["team_name"])
            pol_off  = OFFICE_DISPLAY.get(pol_leader["team_key"], "")
            m1.append(f"📋 **@{pol_leader['rep_name']}**")
            m1.append(f"*{pol_leader['deal_count']} deals · {fmt(pol_leader['total_av'])} AV · {pol_team} · {pol_off}*")

    # FNB section
    if fnb_agents:
        m1.append("\n**First New Business**")
        m1.append("*Issued their first policy this period!*")
        for a in fnb_agents:
            team_label = TEAM_DISPLAY.get(a["team_key"], a["team_name"])
            m1.append(f"🎉 @{a['rep_name']} · *{team_label}*")

    messages = ["\n".join(m1)]

    # ── message 2 (monthly only): top 10 division-wide ───────────────────────
    if is_monthly and top_reps:
        m2 = ["**Top 10 Agents — Division Wide**", "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"]
        pol_leader = top_policies[0] if top_policies else None
        for i, r in enumerate(top_reps):
            medal      = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
            team_label = TEAM_DISPLAY.get(r["team_key"], r["team_name"])
            off_label  = OFFICE_DISPLAY.get(r["team_key"], "")
            badges = []
            if i == 0:
                badges.append("Agent of the Month")
                if pol_leader and pol_leader["rep_name"] == r["rep_name"]:
                    badges.append("Most policies")
            elif pol_leader and pol_leader["rep_name"] == r["rep_name"] and i > 0:
                badges.append(f"Most policies ({pol_leader['deal_count']} deals)")
            badge_str = f" *[{' · '.join(badges)}]*" if badges else ""
            m2.append(f"{medal} **@{r['rep_name']}**{badge_str}")
            m2.append(f"   *{fmt(r['total_av'])} AV · {r['deal_count']} deals · {team_label} · {off_label}*")
        m2.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        messages.append("\n".join(m2))

    # ── office breakdown messages ─────────────────────────────────────────────
    for office_key, office in OFFICES.items():
        o_av    = office_avs[office_key]
        o_deals = office_deals[office_key]
        lines   = []
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        lines.append(f"**{office['name']}** — **{fmt(o_av)} AV** · {o_deals} deals")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

        for team_key in office["teams"]:
            team      = teams.get(team_key)
            team_name = TEAM_DISPLAY.get(team_key, team_key)
            if not team:
                lines.append(f"\n**{team_name}** — No sales this period")
                lines.append("─────────────────────────")
                continue
            lines.append(f"\n**{team_name}** — {fmt(team['total_av'])} AV · {team['deal_count']} deals")
            lines.append("─────────────────────────")
            reps = db.get_rep_totals_period(period, team_key=team_key, limit=rep_limit)
            for i, r in enumerate(reps):
                medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
                lines.append(f"  {medal} @{r['rep_name']} — **{fmt(r['total_av'])} AV** · {r['deal_count']} deals")

        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        messages.append("\n".join(lines))

    # ── GOATs message ─────────────────────────────────────────────────────────
    goat = teams.get(GOAT_KEY)
    if goat:
        lines = ["▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"]
        lines.append(f"**🐐 GOATs** — **{fmt(goat['total_av'])} AV** · {goat['deal_count']} deals")
        lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        reps = db.get_rep_totals_period(period, team_key=GOAT_KEY, limit=rep_limit)
        for i, r in enumerate(reps):
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
    msgs = build_period_stats(db, period)
    return msgs[0] if msgs else "No data found."
