# --- MONKEYPATCH APScheduler's astimezone AT THE VERY TOP ---
import os
import pytz
import datetime 
import logging

_early_formatter = logging.Formatter('%(asctime)s - %(levelname)s - PPDFBOT_EARLY - %(message)s')
_early_handler = logging.StreamHandler()
_early_handler.setFormatter(_early_formatter)
_early_logger = logging.getLogger("PDFBOT_EARLY_SETUP")
if not _early_logger.hasHandlers(): _early_logger.addHandler(_early_handler)
_early_logger.setLevel(logging.INFO)

_early_logger.info("Attempting to monkeypatch APScheduler's astimezone...")
try:
    import apscheduler.util
    _original_apscheduler_astimezone = apscheduler.util.astimezone
    def _patched_apscheduler_astimezone(tz):
        # This patched version tries to be robust for APScheduler 3.x
        if tz is None: # Indicates get_localzone() might be called by APScheduler
            try:
                import tzlocal
                local_tz = tzlocal.get_localzone()
                # If tzlocal returns a pytz object (older tzlocal versions might), use it
                if isinstance(local_tz, pytz.BaseTzInfo):
                    # _early_logger.info(f"APS_PATCH: tzlocal returned pytz object: {local_tz}")
                    return local_tz
                # If tzlocal returns a zoneinfo object (newer tzlocal/Python 3.9+),
                # APScheduler 3.x's astimezone will fail. Force UTC.
                # _early_logger.warning(f"APS_PATCH: tzlocal returned non-pytz: {local_tz}. Forcing UTC.")
                return pytz.utc
            except Exception as e_tzlocal:
                # _early_logger.error(f"APS_PATCH: tzlocal.get_localzone() failed: {e_tzlocal}. Forcing UTC.")
                return pytz.utc
        # If tz is already a pytz object, the original astimezone should handle it
        if isinstance(tz, pytz.BaseTzInfo):
            # _early_logger.info(f"APS_PATCH: tz is already pytz: {tz}. Using original.")
            return _original_apscheduler_astimezone(tz)
        
        # If tz is not None and not a pytz object (e.g., datetime.timezone.utc, or a string)
        # try original, if it fails with TypeError (because it wants pytz), then force UTC.
        # _early_logger.warning(f"APS_PATCH: tz is non-None, non-pytz: {tz} (type: {type(tz)}). Trying original, then UTC.")
        try:
            return _original_apscheduler_astimezone(tz)
        except TypeError:
            # _early_logger.error(f"APS_PATCH: Original astimezone failed with TypeError. Forcing UTC.")
            return pytz.utc
        except Exception as e_orig_other: # Catch any other error from original
            # _early_logger.error(f"APS_PATCH: Original astimezone failed with other error: {e_orig_other}. Forcing UTC.")
            return pytz.utc

    apscheduler.util.astimezone = _patched_apscheduler_astimezone
    _early_logger.info("APS_PATCH: apscheduler.util.astimezone has been monkeypatched.")
except ImportError:
    _early_logger.error("APS_PATCH_ERROR: APScheduler or pytz not found for monkeypatch. Bot might fail if JobQueue is used.")
except AttributeError:
    _early_logger.error("APS_PATCH_ERROR: 'apscheduler.util' has no attribute 'astimezone'. Is APScheduler version different than expected (e.g., v4+)?")
except Exception as e_patch:
    _early_logger.error(f"APS_PATCH_ERROR: Unexpected error during APScheduler monkeypatching: {e_patch}", exc_info=True)
# --- END OF MONKEYPATCH ---

try:
    from dotenv import load_dotenv
    if os.path.exists(".env"): load_dotenv(); _early_logger.info("Loaded .env file for PDF Bot.")
    else: _early_logger.info(".env file not found. Proceeding with environment variables.")
except ImportError: _early_logger.info("python-dotenv not found. Proceeding with environment variables.")

import requests
import asyncio

from telegram import Update, Bot, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue # Will be used by ApplicationBuilder
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TimedOut, NetworkError, Forbidden

import database_manager as dbm # database_manager will now use DATABASE_DIR

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_CHAT_ID_STR = os.environ.get("ADMIN_CHAT_ID")
DAILY_SEND_TIME_UTC_STR = os.environ.get("DAILY_SEND_TIME_UTC", "08:00") # Default to 08:00 UTC
GITHUB_OWNER = os.environ.get("GITHUB_OWNER")
GITHUB_REPO_NAME = os.environ.get("GITHUB_REPO_NAME")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
PDF_GITHUB_FOLDER_PATH = os.environ.get("PDF_GITHUB_FOLDER_PATH")

# --- Path for temporary PDF download ---
# Uses DATABASE_DIR env var (from .env, used by database_manager) or defaults to current dir "."
# This ensures temp PDF is also on persistent disk if DATABASE_DIR is set (e.g., /data on Render)
DATA_DIR_FOR_BOT = os.environ.get("DATABASE_DIR", ".") 
PDF_LOCAL_FILENAME = "current_daily_document.pdf" # The actual name of the temp file
PDF_LOCAL_PATH = os.path.join(DATA_DIR_FOR_BOT, PDF_LOCAL_FILENAME) # Full path to the temp file
# --- End Path for temporary PDF download ---

GET_PDF_COOLDOWN_SECONDS = 15 # Cooldown for /get_pdf command per user

ADMIN_CHAT_ID = 0
if ADMIN_CHAT_ID_STR:
    try: ADMIN_CHAT_ID = int(ADMIN_CHAT_ID_STR)
    except ValueError: _early_logger.error(f"Invalid ADMIN_CHAT_ID: '{ADMIN_CHAT_ID_STR}'.") # Use early logger

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
    level=logging.INFO, handlers=[logging.StreamHandler()], force=True 
)
logger = logging.getLogger(__name__) # Main application logger

if not TELEGRAM_BOT_TOKEN: logger.critical("CRITICAL: TELEGRAM_BOT_TOKEN not found."); exit(1)
if not all([GITHUB_OWNER, GITHUB_REPO_NAME, PDF_GITHUB_FOLDER_PATH]):
    logger.critical("Missing GitHub config (OWNER, REPO_NAME, PDF_FOLDER_PATH)."); exit(1)
if ADMIN_CHAT_ID == 0: logger.warning("ADMIN_CHAT_ID not set. Admin reports limited.")
else: logger.info(f"Admin Chat ID configured: {ADMIN_CHAT_ID}")
logger.info(f"Daily PDF send time (UTC): {DAILY_SEND_TIME_UTC_STR}")
logger.info(f"Path for temporary PDF downloads: {PDF_LOCAL_PATH}")
logger.info(f"Path for SQLite DB (from db_manager): {dbm.DATABASE_NAME}")


# --- Helper Functions ---
def construct_pdf_url_for_date(date_obj: datetime.date):
    if not all([GITHUB_OWNER, GITHUB_REPO_NAME, PDF_GITHUB_FOLDER_PATH]):
        logger.error("Missing GitHub configuration for URL construction.")
        return None, None
    date_str = date_obj.strftime("%d-%m-%Y")
    folder_path_cleaned = PDF_GITHUB_FOLDER_PATH.strip('/')
    filename = f"Today ({date_str}).pdf"
    pdf_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO_NAME}/{GITHUB_BRANCH}/{folder_path_cleaned}/{filename}"
    return pdf_url, filename

def attempt_download_pdf(pdf_url: str, original_filename: str):
    # Ensure directory for PDF_LOCAL_PATH exists before trying to open the file
    pdf_dir = os.path.dirname(PDF_LOCAL_PATH)
    # Check if pdf_dir is not empty (e.g. not just ".") and then if it exists
    if pdf_dir and not os.path.exists(pdf_dir): 
        try:
            os.makedirs(pdf_dir, exist_ok=True)
            logger.info(f"Created directory for temporary PDF: {pdf_dir}")
        except OSError as e:
            logger.error(f"Failed to create directory for temporary PDF {pdf_dir}: {e}")
            return False # Cannot save PDF if directory can't be made

    if not pdf_url: return False
    try:
        logger.info(f"Attempting to download PDF '{original_filename}' from {pdf_url} to {PDF_LOCAL_PATH}...")
        response = requests.get(pdf_url, stream=True, timeout=30)
        response.raise_for_status()
        with open(PDF_LOCAL_PATH, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
        logger.info(f"PDF '{original_filename}' downloaded successfully to {PDF_LOCAL_PATH}")
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404: logger.warning(f"PDF '{original_filename}' not found (404) at {pdf_url}.")
        else: logger.error(f"HTTP error downloading '{original_filename}': {e.response.status_code} - {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Network/request error downloading '{original_filename}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading '{original_filename}': {e}", exc_info=True)
        return False

def get_and_download_pdf():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    pdf_url_today, filename_today = construct_pdf_url_for_date(today)
    if pdf_url_today and attempt_download_pdf(pdf_url_today, filename_today):
        return filename_today
    logger.info("Today's PDF not found or failed. Trying yesterday's PDF.")
    pdf_url_yesterday, filename_yesterday = construct_pdf_url_for_date(yesterday)
    if pdf_url_yesterday and attempt_download_pdf(pdf_url_yesterday, filename_yesterday):
        return filename_yesterday
    logger.error("Failed to download PDF for today and yesterday.")
    return None

async def send_admin_notification(bot: Bot, message: str, use_html=False):
    if ADMIN_CHAT_ID:
        try:
            parse_mode_to_use = ParseMode.HTML if use_html else None
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=message, parse_mode=parse_mode_to_use)
            logger.info(f"Admin notification sent.")
        except Exception as e: logger.error(f"Failed to send admin notification: {e}")
    else: logger.debug("ADMIN_CHAT_ID not set. Skipping admin notification.")

# --- Telegram Command Handlers ---
COMMAND_LIST_MESSAGE = (
    "Welcome to the PDF Bot! Available commands:\n"
    "/start - Register or re-activate daily PDFs.\n"
    f"/get_pdf - Get PDF (cooldown: {GET_PDF_COOLDOWN_SECONDS}s).\n"
    "/stop - Stop receiving daily PDFs.\n"
    "/help - Show this message."
)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user; chat_id = user.id
    username = user.username or ""; first_name = user.first_name or "User"
    user_db_record = await asyncio.to_thread(dbm.get_user, chat_id)
    reply_message = ""
    if user_db_record:
        if not user_db_record['receives_daily_pdf']:
            await asyncio.to_thread(dbm.set_user_receives_daily_pdf, chat_id, True)
            reply_message = f"Welcome back, <b>{user.mention_html()}</b>! Daily PDFs re-enabled."
        else: reply_message = f"Hello again, <b>{user.mention_html()}</b>! Already set for daily PDFs."
    else:
        await asyncio.to_thread(dbm.add_user, chat_id, username, first_name)
        reply_message = f"Hello <b>{user.mention_html()}</b>! Registered for daily PDF."
    await update.message.reply_html(reply_message)
    await update.message.reply_text(COMMAND_LIST_MESSAGE)
    logger.info(f"User {chat_id} ({username or 'NoUsername'}) used /start.")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_user.id
    user_db_record = await asyncio.to_thread(dbm.get_user, chat_id)
    if user_db_record:
        if user_db_record['receives_daily_pdf']:
            await asyncio.to_thread(dbm.set_user_receives_daily_pdf, chat_id, False)
            await update.message.reply_text("Daily PDFs stopped. Use /start to re-enable.")
            logger.info(f"User {chat_id} opted out of daily PDFs.")
        else: await update.message.reply_text("Already unsubscribed from daily PDFs.")
    else: await update.message.reply_text("Not registered. Use /start.")

async def get_pdf_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id 
    
    now = datetime.datetime.now(datetime.timezone.utc)
    user_data_key = f"get_pdf_last_call_{user_id}"
    last_call_timestamp_utc_iso = context.user_data.get(user_data_key)
    
    if last_call_timestamp_utc_iso:
        last_call_dt_utc = datetime.datetime.fromisoformat(last_call_timestamp_utc_iso)
        seconds_since_last_call = (now - last_call_dt_utc).total_seconds()
        if seconds_since_last_call < GET_PDF_COOLDOWN_SECONDS:
            remaining_cooldown = GET_PDF_COOLDOWN_SECONDS - seconds_since_last_call
            await update.message.reply_text(f"Please wait {remaining_cooldown:.0f} more seconds before requesting again.")
            logger.info(f"User {user_id} /get_pdf rate limited. Cooldown: {remaining_cooldown:.0f}s")
            return
            
    context.user_data[user_data_key] = now.isoformat()

    logger.info(f"User {user_id} requested PDF via /get_pdf.")
    await update.message.reply_text("Fetching the latest PDF, please wait...")
    
    downloaded_pdf_name = await asyncio.to_thread(get_and_download_pdf)
    
    if downloaded_pdf_name and os.path.exists(PDF_LOCAL_PATH):
        try:
            with open(PDF_LOCAL_PATH, 'rb') as pdf_file_obj:
                await context.bot.send_document(chat_id=chat_id, document=InputFile(pdf_file_obj, filename=downloaded_pdf_name))
            logger.info(f"PDF '{downloaded_pdf_name}' sent to {user_id} on demand.")
        except Exception as e:
            logger.error(f"Error sending on-demand PDF to {user_id}: {e}")
            await update.message.reply_text("Sorry, I couldn't send the PDF due to an error.")
        finally:
            if os.path.exists(PDF_LOCAL_PATH):
                try: os.remove(PDF_LOCAL_PATH)
                except OSError as e_rem: logger.error(f"Error removing temp PDF {PDF_LOCAL_PATH}: {e_rem}")
    else: 
        await update.message.reply_text("Sorry, I couldn't find or download the PDF.")
        context.user_data.pop(user_data_key, None) 
        logger.warning(f"/get_pdf failed for user {user_id}, download issue. Cooldown reset.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(COMMAND_LIST_MESSAGE)
async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Sorry, I didn't understand that command. {COMMAND_LIST_MESSAGE}")

# --- Daily Tasks Job Function (for JobQueue) ---
async def perform_daily_tasks(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    logger.info("JobQueue: Starting daily tasks: PDF sending and Admin Report...")
    downloaded_pdf_name = await asyncio.to_thread(get_and_download_pdf)
    users_sent_count, users_blocked_count, users_failed_count = 0, 0, 0
    if downloaded_pdf_name and os.path.exists(PDF_LOCAL_PATH):
        users_to_send_pdf = await asyncio.to_thread(dbm.get_users_for_daily_pdf)
        if users_to_send_pdf:
            logger.info(f"JobQueue: Attempting PDF '{downloaded_pdf_name}' to {len(users_to_send_pdf)} users.")
            for chat_id_db in users_to_send_pdf:
                try:
                    with open(PDF_LOCAL_PATH, 'rb') as pdf_file_obj:
                        await bot.send_document(chat_id=chat_id_db, document=InputFile(pdf_file_obj, filename=downloaded_pdf_name), read_timeout=60, write_timeout=60)
                    await asyncio.to_thread(dbm.update_pdf_sent_date, chat_id_db)
                    users_sent_count += 1
                except Forbidden: users_blocked_count += 1; logger.warning(f"User {chat_id_db} blocked bot (Forbidden).")
                except Exception as e: users_failed_count += 1; logger.error(f"JobQueue: Error sending PDF to {chat_id_db}: {e}")
            logger.info(f"JobQueue: Daily PDF send complete. Sent: {users_sent_count}, Blocked: {users_blocked_count}, Failed: {users_failed_count}")
        else: logger.info("JobQueue: No users eligible for daily PDF.")
        if os.path.exists(PDF_LOCAL_PATH):
            try: os.remove(PDF_LOCAL_PATH); logger.info(f"JobQueue: Local PDF removed.")
            except OSError as e_rem: logger.error(f"JobQueue: Error removing PDF: {e_rem}")
    else:
        logger.error(f"JobQueue: PDF could not be downloaded for daily send.")
        await send_admin_notification(bot, "<b>PDF Bot Alert (JobQueue):</b> Daily PDF could not be downloaded. No PDFs sent.", use_html=True)
    try:
        total_users = await asyncio.to_thread(dbm.get_all_users_count)
        yesterday_for_report = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        new_users_list = await asyncio.to_thread(dbm.get_new_users_since, yesterday_for_report)
        num_new_users = len(new_users_list)
        report_message = "<b>PDF Bot - Daily Admin Report (JobQueue)</b>\n\n"
        report_message += f"Total Users: {total_users}\nNew Users (24h): {num_new_users}\n"
        if num_new_users > 0:
            report_message += "New User Details:\n"
            for user_row in new_users_list[:10]:
                uname, fname = user_row['username'] or "N/A", user_row['first_name'] or "N/A"
                report_message += f"- <code>{user_row['chat_id']}</code> - {uname} - {fname}\n"
            if num_new_users > 10: report_message += f"...and {num_new_users - 10} more.\n"
        report_message += f"\nPDFs Sent: {users_sent_count}\nBlocked: {users_blocked_count}\nFailures: {users_failed_count}\n"
        report_message += f"PDF Sent: {downloaded_pdf_name or 'None (Download Failed)'}\n"
        await send_admin_notification(bot, report_message, use_html=True)
    except Exception as e:
        logger.error(f"JobQueue: Error generating admin report: {e}", exc_info=True)
        await send_admin_notification(bot, "<b>PDF Bot Error (JobQueue):</b> Failed admin report. Check logs.", use_html=True)

# --- Main Application Setup and Run ---
def main() -> None: 
    dbm.initialize_db() 
    if not TELEGRAM_BOT_TOKEN: logger.critical("TELEGRAM_BOT_TOKEN missing."); exit(1)

    # This is the polling mode. No --send-daily argument in this hybrid version.
    if not all([GITHUB_OWNER, GITHUB_REPO_NAME, PDF_GITHUB_FOLDER_PATH]):
        logger.critical("Missing GitHub config for PDF Bot. Exiting.")
        if ADMIN_CHAT_ID:
            temp_bot = Bot(token=TELEGRAM_BOT_TOKEN)
            asyncio.run(send_admin_notification(temp_bot, "<b>PDF Bot CRITICAL:</b> Missing GitHub config. Bot cannot start polling fully.", use_html=True))
        exit(1)
        
    logger.info("Initializing Application with default JobQueue, relying on monkeypatched astimezone.")
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    try:
        time_parts = DAILY_SEND_TIME_UTC_STR.split(':')
        send_hour = int(time_parts[0])
        send_minute = int(time_parts[1])
        job_time = datetime.time(hour=send_hour, minute=send_minute, tzinfo=pytz.utc)
        if application.job_queue:
            application.job_queue.run_daily(perform_daily_tasks, time=job_time, name="daily_pdf_and_report_job")
            logger.info(f"Scheduled daily tasks job to run at {job_time.strftime('%H:%M')} UTC.")
        else: 
            logger.error("JobQueue is NOT available after Application build. Internal daily scheduling FAILED.")
            if ADMIN_CHAT_ID and application.bot : 
                asyncio.run(send_admin_notification(application.bot, "<b>PDF Bot CRITICAL:</b> JobQueue not available post-build. Internal daily scheduling failed.", use_html=True))
    except ValueError:
        logger.error(f"Invalid DAILY_SEND_TIME_UTC: '{DAILY_SEND_TIME_UTC_STR}'. Daily job not scheduled.")
        if ADMIN_CHAT_ID and hasattr(application, 'bot') and application.bot:
            asyncio.run(send_admin_notification(application.bot, f"<b>PDF Bot Config Error:</b> Invalid DAILY_SEND_TIME_UTC: {DAILY_SEND_TIME_UTC_STR}. Daily job not scheduled.", use_html=True))
        else: logger.error("Cannot send admin notification for bad time, application.bot not ready or ADMIN_CHAT_ID not set.")
    except Exception as e_jq_schedule: 
        logger.error(f"Failed to schedule daily job: {e_jq_schedule}", exc_info=True)
        if ADMIN_CHAT_ID and hasattr(application, 'bot') and application.bot:
            asyncio.run(send_admin_notification(application.bot, "<b>PDF Bot CRITICAL:</b> Failed to schedule internal daily job. Check logs.", use_html=True))
        else: logger.error("Cannot send admin notification for JQ schedule fail, application.bot not ready or ADMIN_CHAT_ID not set.")

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("get_pdf", get_pdf_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    
    logger.info("PDF Bot started polling...")
    if ADMIN_CHAT_ID and hasattr(application, 'bot') and application.bot: 
        asyncio.create_task(send_admin_notification(application.bot, "PDF Bot (Hybrid Mode) started successfully.", use_html=True))
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("PDF Bot polling stopped.")
    if ADMIN_CHAT_ID and hasattr(application, 'bot') and application.bot :
        async def _notify_shutdown(): await send_admin_notification(application.bot, "PDF Bot (Hybrid Mode) polling has stopped.", use_html=True)
        try: 
            loop = asyncio.get_event_loop_policy().get_event_loop()
            if loop.is_running(): asyncio.create_task(_notify_shutdown())
            else: asyncio.run(_notify_shutdown())
        except RuntimeError: logger.warning("Could not send shutdown notification; event loop issue.")

if __name__ == '__main__':
    main()