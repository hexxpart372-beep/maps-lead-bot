import json
from groq import Groq
from config import Config

client = Groq(api_key=Config.GROQ_API_KEY)

def classify_lead(text: str, source: str) -> dict:
    prompt = f"""
You are a Nigerian real estate and job lead classifier.

Your job is to extract intent from any text. Be VERY generous.
If there is ANY mention of property, housing, land, rent, job, hiring, vacancy — mark it valid.

Return ONLY this JSON, nothing else, no backticks:
{{
  "type": "Seller" or "Buyer" or "Renter" or "Landlord" or "JobSeeker" or "Recruiter" or "Unknown",
  "location": "Nigerian city mentioned or Nigeria if general",
  "intent": "one sentence what they want",
  "score": 3,
  "is_valid": true
}}

Text: "{text[:300]}"
Source: {source}
"""
    try:
        response = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        return {
            "type": "Unknown",
            "location": "Nigeria",
            "intent": text[:150],
            "score": 2,
            "is_valid": True
        }
