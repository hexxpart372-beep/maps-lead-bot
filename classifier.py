import json
from groq import Groq
from config import Config

client = Groq(api_key=Config.GROQ_API_KEY)

def classify_lead(text: str, source: str) -> dict:
    prompt = f"""
You are a Nigerian real estate lead classifier. Your job is to extract any property-related intent from text.

Be generous in your classification. If there is ANY hint of someone buying, selling, renting, or looking for property in Nigeria — classify it as valid.

Analyze this text and return ONLY a JSON object with these exact fields:
- type: "Seller" or "Buyer" or "Renter" or "Landlord" or "Unknown"
- location: Nigerian city or area mentioned, or "Nigeria" if general
- intent: one clear sentence summarizing what they want
- score: urgency score 1-5 where:
  1 = general property content
  2 = mild interest
  3 = looking or considering
  4 = actively searching or selling
  5 = urgent, immediate need
- is_valid: true if there is ANY property-related content, false only if completely unrelated

Text: "{text}"
Source: {source}

Return only valid JSON. No explanation. No markdown. No backticks.
"""
    try:
        response = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        raw = response.choices[0].message.content.strip()
        # Clean any accidental markdown
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        # Don't discard — save with low score instead
        return {
            "type": "Unknown",
            "location": "Nigeria",
            "intent": text[:150],
            "score": 2,
            "is_valid": True
}
