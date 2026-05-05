import re

LOCATION_HINTS = [
    "lagos", "abuja", "ibadan", "port harcourt", "enugu",
    "owerri", "benin", "abeokuta", "lekki", "ajah", "yaba",
    "surulere", "ikeja", "victoria island", "ikoyi", "magodo",
    "gbagada", "maitama", "wuse", "gwarinpa", "asokoro",
    "garki", "jabi", "kano", "kaduna", "ilorin", "nigeria"
]

SELLER_HINTS = [
    "for sale", "selling", "sell", "distress", "urgent sale",
    "relocating", "owner selling", "landlord", "lease"
]

BUYER_HINTS = [
    "for rent", "looking for", "need", "want", "seeking",
    "searching", "require", "short let", "to let"
]

JOB_HINTS = [
    "hiring", "vacancy", "job", "recruit", "apply",
    "career", "employment", "staff needed", "worker needed"
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
    return "Property"

def classify_lead(text: str, source: str) -> dict:
    return {
        "type": detect_type(text),
        "location": detect_location(text),
        "intent": text[:150],
        "score": 3,
        "is_valid": True
           }
