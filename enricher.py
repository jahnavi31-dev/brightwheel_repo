import json
import os
from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
from langsmith.wrappers import wrap_openai
from langsmith import traceable

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError(
        "\n\n  ❌  OPENAI_API_KEY not found in environment.\n"
        "  Check your .env file has exactly:  OPENAI_API_KEY=sk-proj-...\n"
        "  (no quotes, no export, no spaces around =)\n"
    )

_client = wrap_openai(OpenAI(api_key=OPENAI_API_KEY))


@traceable(name="ai-completion-call", run_type="llm")
def call_ai(question):
    try:
        response = _client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=200,
            messages=[
                {
                    "role": "system",
                    "content": "You are helping fill in missing data for childcare centers. Return only valid JSON, no extra text, no markdown fences."
                },
                {
                    "role": "user",
                    "content": question
                }
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"  [enricher] API call failed: {e}")
        return None


@traceable(name="enrich-record", run_type="chain")
def enrich_record(record, api_key=None):
    """
    Use AI to fill in only missing contact_name and contact_email.
    All other enrichment fields have been removed.
    """
    name    = record.get("business_name", "unknown")
    contact = record.get("contact_name", "")
    email   = record.get("contact_email", "")
    address = record.get("address", "")
    city    = record.get("city", "")
    state   = record.get("state", "")

    # If both fields are already present, skip the API call entirely
    if contact and email:
        return {}

    question = f"""I have a childcare center with this data:
- Name: {name}
- Contact: {contact or "MISSING"}
- Email: {email or "MISSING"}
- Address: {address}, {city}, {state}

Please infer only the missing fields. Return ONLY this JSON object (no markdown, no extra text):
{{
  "contact_email": "best guess for email if missing, else null",
  "contact_name": "best guess for contact name if missing, else null"
}}"""

    response_text = call_ai(question)
    if not response_text:
        return {"error": "AI enrichment call failed"}

    response_text = response_text.replace("```json", "").replace("```", "").strip()

    try:
        ai_data = json.loads(response_text)
    except json.JSONDecodeError as e:
        print(f"  [enricher] Could not parse AI response: {e}")
        return {"error": "Could not parse AI response"}

    enriched_fields = {}

    # Only add email if it was missing
    if not email and ai_data.get("contact_email"):
        enriched_fields["contact_email"] = {
            "value": ai_data["contact_email"],
            "from_source": "ai_inferred",
            "confirmed": False,
            "confidence": "low - AI guess, must verify"
        }

    # Only add contact name if it was missing
    if not contact and ai_data.get("contact_name"):
        enriched_fields["contact_name"] = {
            "value": ai_data["contact_name"],
            "from_source": "ai_inferred",
            "confirmed": False,
            "confidence": "low - AI guess, must verify"
        }

    return enriched_fields