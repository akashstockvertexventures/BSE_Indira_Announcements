import os
import subprocess
import time
import threading
import requests
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TARGET_SCRIPT = os.getenv("TARGET_SCRIPT", "main.py")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 20))
FREEZE_TIMEOUT = int(os.getenv("FREEZE_TIMEOUT", 60))
RESTART_DELAY = int(os.getenv("RESTART_DELAY", 30))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", 7))

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

HEARTBEAT_FILE = BASE_DIR / "heartbeat.status"
PIPELINE_OUT_LOG = LOG_DIR / "pipeline_out.log"
PIPELINE_ERR_LOG = LOG_DIR / "pipeline_err.log"

logger = logging.getLogger("supervisor")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    LOG_DIR / "supervisor.log",
    when="midnight",
    interval=1,
    backupCount=LOG_RETENTION_DAYS,
    encoding="utf-8"
)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)

def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("Telegram not configured. Skipped message.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        )
    except Exception as e:
        logger.error("Telegram send error: %s", e)

def start_heartbeat_writer():
    def loop():
        while True:
            try:
                HEARTBEAT_FILE.write_text(str(time.time()))
            except Exception as e:
                logger.error("Heartbeat write error: %s", e)
            time.sleep(HEARTBEAT_INTERVAL)
    threading.Thread(target=loop, daemon=True).start()

def start_heartbeat_monitor():
    def loop():
        last_alert = 0
        while True:
            try:
                if HEARTBEAT_FILE.exists():
                    ts = float(HEARTBEAT_FILE.read_text())
                    age = time.time() - ts
                    if age > FREEZE_TIMEOUT and (time.time() - last_alert > 120):
                        msg = f"⚠ Pipeline frozen — no heartbeat for {int(age)}s."
                        send_telegram(msg)
                        logger.warning(msg)
                        last_alert = time.time()
                else:
                    logger.warning("Heartbeat file missing.")
            except Exception as e:
                logger.error("Heartbeat monitor error: %s", e)
            time.sleep(10)
    threading.Thread(target=loop, daemon=True).start()

def run_supervisor():
    send_telegram("🚀 Supervisor started")
    logger.info("Supervisor started.")
    start_heartbeat_writer()
    start_heartbeat_monitor()
    while True:
        send_telegram(f"▶ Starting `{TARGET_SCRIPT}`")
        logger.info("Starting %s", TARGET_SCRIPT)
        out_f = open(PIPELINE_OUT_LOG, "a", encoding="utf-8")
        err_f = open(PIPELINE_ERR_LOG, "a", encoding="utf-8")
        try:
            proc = subprocess.Popen(
                ["python", TARGET_SCRIPT],
                stdout=out_f,
                stderr=err_f,
                text=True
            )
        except Exception as e:
            msg = f"❌ Failed to start: {e}"
            send_telegram(msg)
            logger.error(msg)
            out_f.close()
            err_f.close()
            time.sleep(RESTART_DELAY)
            continue

        return_code = proc.wait()
        out_f.close()
        err_f.close()

        try:
            with open(PIPELINE_ERR_LOG, "r", encoding="utf-8") as f:
                err_lines = f.readlines()[-40:]
                last_error = "".join(err_lines).strip()
        except:
            last_error = "Could not read error log."

        if return_code != 0:
            msg = (
                f"💥 *Pipeline crashed*\n"
                f"Exit code: `{return_code}`\n\n"
                f"*Error Log:*\n```\n{last_error}\n```"
            )
            send_telegram(msg)
            logger.error(msg)
        else:
            msg = (
                "⚠ Pipeline exited unexpectedly.\n"
                f"*Error Log:*\n```\n{last_error}\n```"
            )
            send_telegram(msg)
            logger.warning(msg)

        time.sleep(RESTART_DELAY)

if __name__ == "__main__":
    run_supervisor()
