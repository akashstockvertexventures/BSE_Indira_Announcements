import os
import sys
import time
import signal
import threading
import subprocess
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from core.logger import get_logger
load_dotenv()

# ==========================================================
# CONFIG
# ==========================================================
TARGET_SCRIPT = os.getenv("TARGET_SCRIPT", "main.py")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 20))
FREEZE_TIMEOUT = int(os.getenv("FREEZE_TIMEOUT", 60))
RESTART_DELAY = int(os.getenv("RESTART_DELAY", 30))

BAT_FILE_PATH = os.getenv("BAT_FILE_PATH", r"D:/Completed_Codes/NewsImpactDashboard/AutoMatic_Run.bat")
BAT_TRIGGER_TIME = os.getenv("BAT_TRIGGER_TIME", "23:00") 

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_FILE = LOG_DIR / "heartbeat.status"

# ==========================================================
# LOGGING
# ==========================================================
supervisor_logger = get_logger("supervisor", save_time_logs=True)
error_logger = get_logger("error", save_time_logs=True)
output_logger = get_logger("output", save_time_logs=True)

# ==========================================================
# TELEGRAM
# ==========================================================
def send_telegram(msg: str):
    """Send alert messages to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        supervisor_logger.warning("Telegram not configured. Skipped message.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=5,
        )
    except Exception as e:
        error_logger.exception("Telegram send error: %s", e)

# ==========================================================
# BAT EXECUTION (simple double-click trigger)
# ==========================================================
_last_bat_run_date = None

def run_bat_if_time(bat_path: str, trigger_time: str):
    """
    Silently runs a .bat file (like double-click) when system time matches trigger_time (HH:MM).
    Runs hidden (no console) and executes only once per day.
    """
    global _last_bat_run_date
    try:
        bat_path = os.path.abspath(bat_path)
        if not os.path.exists(bat_path):
            supervisor_logger.warning(f"⚠ .bat file not found: {bat_path}")
            return

        now = datetime.now()
        if now.strftime("%H:%M") == trigger_time and _last_bat_run_date != now.date():
            _last_bat_run_date = now.date()
            supervisor_logger.info(f"🕒 Trigger matched — running .bat file: {bat_path}")
            send_telegram(f"🕒 Running scheduled .bat task at {now.strftime('%H:%M')}")
            subprocess.Popen(
                f'start /min "" "{bat_path}"',
                shell=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            supervisor_logger.info("✅ .bat file executed successfully.")
    except Exception as e:
        error_logger.exception("Error running .bat file: %s", e)
        send_telegram(f"❌ Error running .bat file: {e}")

# ==========================================================
# HEARTBEAT
# ==========================================================
def start_heartbeat_writer():
    """Writes timestamp periodically and checks time for .bat trigger."""
    def loop():
        while True:
            try:
                HEARTBEAT_FILE.write_text(str(time.time()))
                run_bat_if_time(BAT_FILE_PATH, BAT_TRIGGER_TIME)
            except Exception as e:
                error_logger.exception("Heartbeat write error: %s", e)
            time.sleep(HEARTBEAT_INTERVAL)

    threading.Thread(target=loop, daemon=True).start()


def start_heartbeat_monitor():
    """Monitors heartbeat to detect frozen processes."""
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
                        supervisor_logger.warning(msg)
                        last_alert = time.time()
                else:
                    supervisor_logger.warning("Heartbeat file missing.")
            except Exception as e:
                error_logger.exception("Heartbeat monitor error: %s", e)
            time.sleep(10)

    threading.Thread(target=loop, daemon=True).start()

# ==========================================================
# SUPERVISOR LOOP
# ==========================================================
def run_supervisor():
    """Main supervisor control loop."""
    send_telegram("🚀 Supervisor started")
    supervisor_logger.info("Supervisor started.")
    start_heartbeat_writer()
    start_heartbeat_monitor()

    proc = None

    # --- graceful shutdown handler ---
    def handle_exit(signum=None, frame=None):
        supervisor_logger.warning("Received termination signal — shutting down safely.")
        send_telegram("🛑 Supervisor shutting down gracefully.")
        try:
            if proc and proc.poll() is None:
                supervisor_logger.info("Terminating child process...")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    supervisor_logger.warning("Child unresponsive. Forcing kill.")
                    proc.kill()
            supervisor_logger.info("Shutdown complete.")
        except Exception as e:
            error_logger.exception("Error during graceful shutdown: %s", e)
        sys.exit(0)

    # Bind SIGINT (Ctrl+C) and SIGTERM (system kill)
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    while True:
        try:
            send_telegram(f"▶ Starting `{TARGET_SCRIPT}`")
            supervisor_logger.info("Starting %s", TARGET_SCRIPT)

            err_path = LOG_DIR / "error.log"
            out_path = LOG_DIR / "output.log"

            err_f = open(err_path, "a", encoding="utf-8")
            out_f = open(out_path, "a", encoding="utf-8")

            proc = subprocess.Popen(
                ["python", TARGET_SCRIPT],
                stdout=out_f,
                stderr=err_f,
                text=True,
            )

            return_code = proc.wait()
            out_f.close()
            err_f.close()

            try:
                with open(err_path, "r", encoding="utf-8") as f:
                    err_lines = f.readlines()[-40:]
                    last_error = "".join(err_lines).strip()
            except Exception as e:
                error_logger.exception("Error reading error log: %s", e)
                last_error = "Could not read error log."

            if return_code != 0:
                msg = (
                    f"💥 *Pipeline crashed*\n"
                    f"Exit code: `{return_code}`\n\n"
                    f"*Error Log:*\n```\n{last_error}\n```"
                )
                send_telegram(msg)
                supervisor_logger.error(msg)
            else:
                msg = (
                    "⚠ Pipeline exited unexpectedly.\n"
                    f"*Error Log:*\n```\n{last_error}\n```"
                )
                send_telegram(msg)
                supervisor_logger.warning(msg)

        except KeyboardInterrupt:
            handle_exit(signal.SIGINT, None)
        except Exception as e:
            error_logger.exception("Supervisor runtime error: %s", e)

        time.sleep(RESTART_DELAY)

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    try:
        run_supervisor()
    except Exception as e:
        error_logger.exception("FATAL: Supervisor crashed: %s", e)
        send_telegram(f"💀 *FATAL:* Supervisor crashed\n```\n{e}\n```")
