import asyncio
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from config import Config
from sheets import (
    get_new_leads, save_agent, get_all_agents,
    save_pack, mark_leads_packed, get_stats,
    get_leads_by_location
)
from scraper import run_all_scrapers
from classifier import classify_lead
from sheets import save_lead

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != Config.OWNER_ID:
            await update.message.reply_text("❌ Unauthorized")
            return
        return await func(update, context)
    return wrapper

@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏠 *Real Estate Lead Bot*\n\n"
        "/newleads — View fresh leads\n"
        "/createpack [location] [type] — Bundle leads\n"
        "/agents — View all agents\n"
        "/addagent [name] [phone] [areas] — Add agent\n"
        "/matchagent [location] — Match leads to agents\n"
        "/stats — Dashboard\n"
        "/scrape — Run manual scrape now",
        parse_mode="Markdown"
    )

@owner_only
async def new_leads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    leads = get_new_leads(limit=15)
    if not leads:
        await update.message.reply_text("No new leads yet. Use /scrape to hunt 🔍")
        return
    msg = f"🔥 *Fresh Leads ({len(leads)})*\n\n"
    for lead in leads:
        msg += (
            f"📌 *{lead.get('ID')}* | {lead.get('Type')} | Score: {lead.get('Score')}/5\n"
            f"📍 {lead.get('Location')}\n"
            f"💬 {lead.get('Intent')}\n"
            f"🔗 {lead.get('Source')}\n"
            f"🕐 {lead.get('Date')}\n"
            f"─────────────\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")

@owner_only
async def create_pack(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /createpack [location] [type]\n"
            "Example: /createpack Lagos Seller"
        )
        return
    location = args[0].title()
    lead_type = args[1].title()
    leads = get_leads_by_location(location)
    leads = [l for l in leads if l.get("Type","").lower() == lead_type.lower()]
    if not leads:
        await update.message.reply_text(f"No {lead_type} leads found for {location}")
        return
    pack_id = save_pack({
        "name": f"{location} {lead_type} Pack",
        "type": lead_type,
        "location": location,
        "lead_count": len(leads)
    })
    lead_ids = [l["ID"] for l in leads]
    mark_leads_packed(lead_ids, pack_id)
    msg = f"📦 *Pack Created: {pack_id}*\n"
    msg += f"📍 {location} | 👤 {lead_type} | 📊 {len(leads)} leads\n\n"
    for i, lead in enumerate(leads, 1):
        msg += (
            f"*#{i}* Score: {lead.get('Score')}/5\n"
            f"Intent: {lead.get('Intent')}\n"
            f"Source: {lead.get('Source')}\n\n"
        )
    msg += "✅ Screenshot this and send to agents on WhatsApp"
    await update.message.reply_text(msg, parse_mode="Markdown")

@owner_only
async def list_agents(update: Update, context: ContextTypes.DEFAULT_TYPE):
    agents = get_all_agents()
    if not agents:
        await update.message.reply_text("No agents yet. Use /addagent")
        return
    msg = f"👥 *Your Agents ({len(agents)})*\n\n"
    for agent in agents:
        msg += (
            f"*{agent.get('Name')}* — {agent.get('ID')}\n"
            f"📞 {agent.get('Phone')}\n"
            f"📍 {agent.get('Areas')}\n"
            f"✅ Deals: {agent.get('Deals Closed')}\n"
            f"─────────────\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")

@owner_only
async def add_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /addagent [name] [phone] [areas]\n"
            "Example: /addagent Tunde 08012345678 Lagos,Abuja"
        )
        return
    agent_id = save_agent({
        "name": args[0],
        "phone": args[1],
        "areas": args[2],
        "budget": args[3] if len(args) > 3 else "Not specified"
    })
    await update.message.reply_text(
        f"✅ Agent added!\nID: {agent_id}\nName: {args[0]}\nPhone: {args[1]}\nAreas: {args[2]}"
    )

@owner_only
async def match_agent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /matchagent [location]\nExample: /matchagent Lagos")
        return
    location = args[0].title()
    agents = get_all_agents()
    leads = get_leads_by_location(location)
    matched = [a for a in agents if location.lower() in a.get("Areas","").lower()]
    if not matched:
        await update.message.reply_text(f"No agents found for {location}")
        return
    msg = f"🎯 *Match: {location}*\n"
    msg += f"📊 Available Leads: {len(leads)}\n\n"
    for agent in matched:
        msg += f"• *{agent.get('Name')}* — {agent.get('Phone')}\n"
    msg += "\n💡 Go sell them a pack on WhatsApp!"
    await update.message.reply_text(msg, parse_mode="Markdown")

@owner_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = get_stats()
    await update.message.reply_text(
        f"📊 *Dashboard*\n\n"
        f"🔥 Total Leads: {data['total_leads']}\n"
        f"🆕 New Leads: {data['new_leads']}\n"
        f"📦 Packed: {data['packed_leads']}\n"
        f"👥 Agents: {data['total_agents']}\n"
        f"🗂 Packs: {data['total_packs']}",
        parse_mode="Markdown"
    )

@owner_only
async def manual_scrape(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Scraping now... this may take a minute")
    try:
        posts = run_all_scrapers()
        new_count = 0
        for post in posts:
            result = classify_lead(post["text"], post["source"])
            if result.get("is_valid") and result.get("score", 0) >= 4:
                save_lead({
                    "type": result["type"],
                    "location": result["location"],
                    "intent": result["intent"],
                    "source": post["source"],
                    "score": result["score"]
                })
                new_count += 1
        await update.message.reply_text(
            f"✅ Scrape done!\n{new_count} new leads saved\nUse /newleads to view"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Scrape failed: {str(e)}")

async def auto_scheduler(app):
    while True:
        await asyncio.sleep(Config.SCRAPE_INTERVAL_MINUTES * 60)
        try:
            posts = run_all_scrapers()
            new_count = 0
            for post in posts:
                result = classify_lead(post["text"], post["source"])
                if result.get("is_valid") and result.get("score", 0) >= 4:
                    save_lead({
                        "type": result["type"],
                        "location": result["location"],
                        "intent": result["intent"],
                        "source": post["source"],
                        "score": result["score"]
                    })
                    new_count += 1
            if new_count > 0 and Config.OWNER_ID:
                await app.bot.send_message(
                    chat_id=Config.OWNER_ID,
                    text=f"🔥 Auto scrape done\n{new_count} new leads saved\nUse /newleads to view"
                )
        except Exception as e:
            logging.error(f"Auto scrape error: {e}")

async def main():
    app = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("newleads", new_leads))
    app.add_handler(CommandHandler("createpack", create_pack))
    app.add_handler(CommandHandler("agents", list_agents))
    app.add_handler(CommandHandler("addagent", add_agent))
    app.add_handler(CommandHandler("matchagent", match_agent))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("scrape", manual_scrape))
    asyncio.create_task(auto_scheduler(app))
    await app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
