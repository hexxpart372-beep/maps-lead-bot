import json
from groq import Groq
from config import Config

client = Groq(api_key=Config.GROQ_API_KEY)

def classify_lead(text: str, source: str) -> dict:
    prompt = f"""
You are a Nigerian real estate lead classifier.

Analyze this post and return ONLY a JSON object with these exact fields:
- type: "Seller" or "Buyer" or "Unknown"
- location: city or area mentioned, or "Unknown"
- intent: one sentence summary of what they want
- score: urgency score 1-5 (5 is most urgent)
- is_valid: true or false (is this a real property lead?)

Post: "{text}"
Source: {source}

Return only valid JSON. No explanation. No markdown.
"""
    try:
        response = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        raw = response.choices[0].message.content.strip()
        return json.loads(raw)
    except Exception:
        return {
            "type": "Unknown",
            "location": "Unknown",
            "intent": text[:100],
            "score": 1,
            "is_valid": False
}
