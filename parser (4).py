import os
import json
import asyncio
import anthropic

_client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
_queue  = asyncio.Queue()
_worker_started = False

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
- Wrap / ACA Wrap / 🌯 = Wrap (ACA policy bundled with products)

ANCILLARY ADD-ONS (already included in the premium total — do NOT add separately):
- MG  = MedGuard
- AP  = Accident Protection
- IP  = Income Protector
- LP  = Life Protector
- Dental / 🦷 / teeth emoji = Dental
- Vision / 👁️ / eye emoji = Vision

DEAL TAGS (include any that appear):
- OTF = On The Fly
- OCC or OCK = One Call Close
- Flip = Policy Flip (someone switching from another policy)

ASSOCIATION TIERS:
Association is OPTIONAL — not every deal will have one. If none is present return empty string.

Standard tiers:
- Ruby / 🔴
- Sapphire / Saph / 🔵
- Emerald / Em / 💚
- Diamond / Di / 💎 — only if NOT preceded by EX or Executive
- Executive Diamond / Ex Diamond / Ex. Diamond / Ex Di / EX💎 / Ex💎

HealthAccess (HA) specific tiers:
- Entrepreneur / Entre / Entrep = Entrepreneur
- Elite = Elite

TEAM EMOJIS (ignore completely):
- 🏠 🏰 ⛩️ 🧼 🦴 🏎️ 💎 (when used as team indicator not association)

RUNNER/HELPER MENTIONS (ignore completely):
- Phrases like "for @username", "with @username", "with @username's help", "underneath @username"
  mean someone helped or is being credited — IGNORE these mentions entirely.
- Always credit the POSTER of the message, not any mentioned usernames.
- Discord mention tags like <@123456> should be stripped and ignored.

IMPORTANT RULES:
- The dollar amount before the FIRST product code is the TOTAL monthly premium.
- Ancillary amounts listed separately (e.g. "20MG 15AP") are already INCLUDED in the main premium.
- Decimal amounts are valid (e.g. $197.32)
- If the message is not a sale post (just chat, a question, announcement), return {"is_sale": false}
- A message with a dollar amount + product code IS a sale even if it also contains @ mentions

EXAMPLES:
- "$572 HA Entre AP MG OCC" → premium:572, products:"HA + AP + MG", association:"Entrepreneur", deal_tags:"OCC"
- "$197.32 HA Flip for @caleb" → premium:197.32, products:"HA", association:"", deal_tags:"Flip"
- "$251 PA 20MG OTF 💎" → premium:251, products:"PA + MG", association:"Diamond", deal_tags:"OTF"
- "$305 HA Elite" → premium:305, products:"HA", association:"Elite", deal_tags:""
- "$351 SA 20MG Diamond" → premium:351, products:"SA + MG", association:"Diamond", deal_tags:""
- "$400 PA" → premium:400, products:"PA", association:"", deal_tags:""
- "$101 🌯 🦷 👀 OCC" → premium:101, products:"Wrap + Dental + Vision", association:"", deal_tags:"OCC"

Return ONLY valid JSON, no markdown, no explanation.

FORMAT when it IS a sale:
{
  "is_sale": true,
  "premium": <number>,
  "products": "<core product(s) + ancillaries as readable string e.g. 'PA + MG + AP'>",
  "association": "<tier or empty string>",
  "deal_tags": "<OTF, OCC, Flip etc. or empty string>"
}

FORMAT when it is NOT a sale:
{"is_sale": false}
"""

async def _parse_worker():
    """Process parse requests one at a time to avoid race conditions."""
    while True:
        message_text, future = await _queue.get()
        try:
            print(f"🔍 Parsing message: {message_text}")
            response = await _client.messages.create(
                model      = "claude-sonnet-4-6",
                max_tokens = 256,
                system     = SYSTEM_PROMPT,
                messages   = [{"role": "user", "content": message_text}],
            )
            text = response.content[0].text.strip()
            text = text.replace("```json", "").replace("```", "").strip()
            print(f"📦 Parser response: {text}")
            data = json.loads(text)

            if not data.get("is_sale"):
                print("ℹ️  Not recognized as a sale")
                future.set_result(None)
            else:
                result = {
                    "premium":     float(data["premium"]),
                    "products":    data.get("products", ""),
                    "association": data.get("association", ""),
                    "deal_tags":   data.get("deal_tags", ""),
                }
                print(f"✅ Parsed sale: {result}")
                future.set_result(result)

        except Exception as e:
            print(f"⚠️  Parser error: {e}")
            future.set_exception(e)
        finally:
            _queue.task_done()

async def parse_sale(message: str) -> dict | None:
    """
    Queue a parse request and wait for the result.
    Sequential processing prevents race conditions on simultaneous posts.
    """
    global _worker_started
    if not _worker_started:
        asyncio.get_event_loop().create_task(_parse_worker())
        _worker_started = True

    future = asyncio.get_event_loop().create_future()
    await _queue.put((message, future))

    try:
        return await future
    except Exception:
        return None
