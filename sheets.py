import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from config import Config

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_client():
    creds = Credentials.from_service_account_info(
        Config.GOOGLE_SERVICE_ACCOUNT_INFO,
        scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client.open_by_key(Config.GOOGLE_SHEET_ID)

def ensure_sheets():
    book = get_client()
    existing = [s.title for s in book.worksheets()]
    required = {
        "Leads": ["ID","Type","Location","Intent","Source","Score","Date","Status","Pack"],
        "Agents": ["ID","Name","Phone","Areas","Budget","Date Added","Deals Closed"],
        "Packs": ["Pack ID","Name","Type","Location","Lead Count","Date Created","Status","Sold To"]
    }
    for name, headers in required.items():
        if name not in existing:
            ws = book.add_worksheet(title=name, rows=1000, cols=20)
            ws.append_row(headers)
    return book

def save_lead(lead: dict):
    book = ensure_sheets()
    ws = book.worksheet("Leads")
    rows = ws.get_all_values()
    lead_id = f"L{len(rows):04d}"
    ws.append_row([
        lead_id,
        lead.get("type", "Unknown"),
        lead.get("location", "Unknown"),
        lead.get("intent", ""),
        lead.get("source", ""),
        lead.get("score", 0),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        "New",
        ""
    ])
    return lead_id

def get_new_leads(limit=20):
    book = ensure_sheets()
    ws = book.worksheet("Leads")
    rows = ws.get_all_records()
    return [r for r in rows if r.get("Status") == "New"][-limit:]

def get_leads_by_location(location: str):
    book = ensure_sheets()
    ws = book.worksheet("Leads")
    rows = ws.get_all_records()
    return [
        r for r in rows
        if location.lower() in r.get("Location","").lower()
        and r.get("Status") == "New"
    ]

def save_agent(agent: dict):
    book = ensure_sheets()
    ws = book.worksheet("Agents")
    rows = ws.get_all_values()
    agent_id = f"A{len(rows):03d}"
    ws.append_row([
        agent_id,
        agent.get("name",""),
        agent.get("phone",""),
        agent.get("areas",""),
        agent.get("budget",""),
        datetime.now().strftime("%Y-%m-%d"),
        0
    ])
    return agent_id

def get_all_agents():
    book = ensure_sheets()
    ws = book.worksheet("Agents")
    return ws.get_all_records()

def save_pack(pack: dict):
    book = ensure_sheets()
    ws = book.worksheet("Packs")
    rows = ws.get_all_values()
    pack_id = f"P{len(rows):03d}"
    ws.append_row([
        pack_id,
        pack.get("name",""),
        pack.get("type",""),
        pack.get("location",""),
        pack.get("lead_count", 0),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Ready",
        ""
    ])
    return pack_id

def mark_leads_packed(lead_ids: list, pack_id: str):
    book = ensure_sheets()
    ws = book.worksheet("Leads")
    rows = ws.get_all_records()
    for i, row in enumerate(rows, start=2):
        if row.get("ID") in lead_ids:
            ws.update_cell(i, 8, "Packed")
            ws.update_cell(i, 9, pack_id)

def get_stats():
    book = ensure_sheets()
    leads = book.worksheet("Leads").get_all_records()
    agents = book.worksheet("Agents").get_all_records()
    packs = book.worksheet("Packs").get_all_records()
    return {
        "total_leads": len(leads),
        "new_leads": len([l for l in leads if l.get("Status") == "New"]),
        "packed_leads": len([l for l in leads if l.get("Status") == "Packed"]),
        "total_agents": len(agents),
        "total_packs": len(packs)
  }
