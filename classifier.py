import json
import re
from groq import Groq
from config import Config

client = Groq(api_key=Config.GROQ_API_KEY)

LOCATION_HINTS = [
    "lagos", "abuja", "ibadan", "port harcourt", "enugu",
    "owerri", "benin", "abeokuta", "lekki", "ajah", "yaba",
    "surulere", "ikeja", "victoria island", "ikoyi", "magodo",
    "gbagada", "maitama", "wuse", "gwarinpa", "asokoro",
    "garki", "jabi", "kano", "kaduna", "ilorin", "nigeria"
]

SELLER_HINTS = [
    "for sale", "selling", "sell", "distress", "urgent sale",
    "relocating", "owner selling", "landlord"
]

BUYER_HINTS = [
    "for rent", "looking for", "need", "want", "seeking",
    "searching", "require", "available", "short let"
]

JOB_HINTS = [
    "hiring", "vacancy", "job", "recruit", "apply", "career",
    "employment", "worker needed", "staff needed"
]

def detect_location(text: str) -> str:
    text_lower = text.lower()
    for loc in LOCATION_HINTS:
        if loc in text_lower:
            return loc.title()
    return "Nigeria"

def detect_type(text: str) -> str:
    text_lower = text.lower()
    if any(h in text_lower for h in JOB_HINTS):
        return "Recruiter"
    if any(h in text_lower for h in SELLER_HINTS):
        return "Seller"
    if any(h in text_lower for h in BUYER_HINTS):
        return "Buyer"
    return "Unknown"

def classify_lead(text: str, source: str) -> dict:
    # Try Groq first
    try:
        prompt = f"""
You are a Nigerian real estate and job lead classifier.
Be very generous. If there is ANY property or job content mark is_valid as true.
Return ONLY raw JSON no markdown no backticks:
{{"type":"Seller or Buyer or Renter or Recruiter or JobSeeker or Unknown","location":"Nigerian city or Nigeria","intent":"one sentence summary","score":3,"is_valid":true}}

Text: "{text[:200]}"
"""
        response = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=150
        )
        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        # Extract JSON even if there's extra text
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            result = json.loads(match.group())
            result["is_valid"] = True
            return result
    except Exception as e:
        pass

    # Fallback — classify locally without Groq
    return {
        "type": detect_type(text),
        "location": detect_location(text),
        "intent": text[:150],
        "score": 2,
        "is_valid": True
}
