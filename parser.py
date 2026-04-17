import os
import json
import anthropic

_client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

SYSTEM_PROMPT = """
You are a sales post parser for an insurance sales team Discord bot.

Your job is to read a Discord message and decide:
1. Is this a sale post? (yes/no)
2. If yes, extract the details.

A sale post will contain a dollar amount followed by a product code.

PRODUCT CODES (core):
- PA  = PremierAdvantage
- HA  = HealthAccess
- SA  = SecureAdvantage
- Suppy / Supplemental = Supplemental

ANCILLARY ADD-ONS (already included in the premium total — do NOT add separately):
- MG  = MedGuard
- AP  = Accident Protection
- IP  = Income Protector
- LP  = Life Protector
- Dental / 🦷 / teeth emoji = Dental
- Vision / 👁️ / eye emoji = Vision

DEAL TAGS:
- OTF = On The Fly
- OCC or OCK = One Call Close

ASSOCIATION TIERS (match any of these variations):
- Ruby / 🔴
- Sapphire / Saph / 🔵
- Emerald / Em / 💚
- Diamond / Di / 💎 — BUT only if NOT preceded by EX or Executive
- Executive Diamond / Ex Diamond / Ex. Diamond / Ex Di / EX💎 / Ex💎 / executive diamond

TEAM EMOJIS (ignore these entirely, they are not product or association indicators):
- 🏠 = AV House
- ⛩️ = Empire
- 🧼 = Fresh Dealz
- 🦴 = Health Hounds
- 🏎️ = Redline Revenue

IMPORTANT RULES:
- The dollar amount before the FIRST product code is the TOTAL monthly premium.
- Ancillary amounts listed separately (e.g. "20MG 15AP") are already INCLUDED in the main premium — do NOT add them.
- AV = premium * 12 (you don't need to calculate this, just return the premium)
- Team emojis (⛩️ 🏠 🧼 🦴 🏎️) appearing in the message should be IGNORED — they just indicate which team the rep is on.
- EX💎 or Ex💎 always means Executive Diamond, never plain Diamond.
- If the message is not a sale post (e.g. it's just chat, a question, an announcement), return {"is_sale": false}

EXAMPLES:
- "$351 PA EX💎⛩️" → premium: 351, products: "PA", association: "Executive Diamond", deal_tags: ""
- "$251PA 20MG Diamond" → premium: 251, products: "PA + MG", association: "Diamond", deal_tags: ""
- "$251 PA 20MG OTF 💎" → premium: 251, products: "PA + MG", association: "Diamond", deal_tags: "OTF"
- "$305 HA OTF 🏠" → premium: 305, products: "HA", association: "", deal_tags: "OTF"
- "$329 SA $50 MG saph" → premium: 329, products: "SA + MG", association: "Sapphire", deal_tags: ""

Return ONLY valid JSON, no markdown, no explanation.

FORMAT when it IS a sale:
{
  "is_sale": true,
  "premium": <number>,
  "products": "<core product(s) + ancillaries as readable string e.g. 'PA + MG + AP'>",
  "association": "<tier or empty string>",
  "deal_tags": "<OTF or OCC or empty string>"
}

FORMAT when it is NOT a sale:
{"is_sale": false}
"""

async def parse_sale(message: str) -> dict | None:
    """
    Returns a dict with sale details if the message is a sale post,
    or None if it's not a sale.
    """
    try:
        print(f"🔍 Parsing message: {message}")

        response = await _client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 256,
            system     = SYSTEM_PROMPT,
            messages   = [{"role": "user", "content": message}],
        )
        text = response.content[0].text.strip()
        print(f"📦 Parser response: {text}")

        data = json.loads(text)

        if not data.get("is_sale"):
            print(f"ℹ️  Not recognized as a sale")
            return None

        result = {
            "premium":     float(data["premium"]),
            "products":    data.get("products", ""),
            "association": data.get("association", ""),
            "deal_tags":   data.get("deal_tags", ""),
        }
        print(f"✅ Parsed sale: {result}")
        return result

    except Exception as e:
        print(f"⚠️  Parser error: {e}")
        return None
