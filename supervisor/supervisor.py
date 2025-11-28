import os
import time
import signal
import socket
import queue
import threading
import subprocess
import requests
import re
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from core.logger import get_logger
from rapidfuzz import fuzz


# ==========================================================
# CONFIG
# ==========================================================
class Config:
    def __init__(self):
        load_dotenv()
        base = Path(__file__).resolve().parent.parent

        # Directories
        self.LOG_DIR = base / "logs"
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)

        # Environment
        self.TARGET_SCRIPT = os.getenv("TARGET_SCRIPT", "main.py")
        self.BOT_TOKEN = os.getenv("BOT_TOKEN")
        self.CHAT_ID = os.getenv("CHAT_ID")

        # Timing
        self.RESTART_DELAY = int(os.getenv("RESTART_DELAY", 10))
        self.INTERNET_CHECK_INTERVAL = int(os.getenv("INTERNET_CHECK_INTERVAL", 10))
        self.ERROR_MSG_INTERVAL = int(os.getenv("ERROR_MSG_INTERVAL", 60))
        self.STREAM_POLL_INTERVAL = float(os.getenv("STREAM_POLL_INTERVAL", 0.2))
        self.DNS_TEST_TIMEOUT = int(os.getenv("DNS_TEST_TIMEOUT", 2))
        self.ERROR_MSG_NO_INTERVAL = int(os.getenv("ERROR_MSG_NO_INTERVAL", 10))

        # Heartbeat (JSON status file)
        self.HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 15))
        self.HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "files/supervisor_status.json")


# ==========================================================
# SUPERVISOR V2.1
# ==========================================================
class SupervisorV2_1:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.logger = get_logger("supervisor", save_time_logs=True)
        self.output_log = get_logger("output", save_time_logs=True)

        self.internet_online = False
        self.error_pattern = re.compile(r"(ERROR|Exception|Traceback|failed|timeout)", re.I)

        self.stop_flag = threading.Event()
        self.current_proc: subprocess.Popen | None = None

        # Heartbeat / status
        self.status_lock = threading.Lock()
        self.heartbeat_path = Path(self.cfg.HEARTBEAT_FILE).resolve()
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)

        now = self._now()
        self.status = {
            "supervisor_pid": os.getpid(),
            "supervisor_start_time": now,
            "supervisor_running": True,
            "restart_count": 0,
            "internet_online": False,
            "last_internet_change": None,
            "child_running": False,
            "child_exit_code": None,
            "last_child_start": None,
            "last_child_exit": None,
        }

        self._register_signals()

    # ------------------------------------------------------
    def _now(self) -> str:
        """Return UTC time as clean string: YYYY-MM-DD HH:MM:SS"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------
    # SIGNAL HANDLING
    # ------------------------------------------------------
    def _register_signals(self):
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
        except Exception as e:
            self.logger.warning(f"Failed to register SIGINT handler: {e}")

        if hasattr(signal, "SIGHUP"):
            try:
                signal.signal(signal.SIGHUP, self._signal_handler)
            except Exception:
                pass

    def _signal_handler(self, signum, frame):
        self.event_log(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop_flag.set()

        proc = self.current_proc
        if proc is not None and proc.poll() is None:
            try:
                self._terminate_process(proc)
            except Exception as e:
                self.logger.error(f"terminating child on signal {signum}: {e}")

        with self.status_lock:
            self.status["supervisor_running"] = False
            self.status["child_running"] = False
            self.status["last_child_exit"] = self._now()

        self._write_heartbeat()

    # ------------------------------------------------------
    # HEARTBEAT
    # ------------------------------------------------------
    def _write_heartbeat(self):
        with self.status_lock:
            data = dict(self.status)
        try:
            with open(self.heartbeat_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to write heartbeat: {e}")

    def heartbeat_loop(self):
        self.event_log(f"🫀 Heartbeat writer started -> {self.heartbeat_path}")
        while not self.stop_flag.is_set():
            self._write_heartbeat()
            time.sleep(self.cfg.HEARTBEAT_INTERVAL)
        self._write_heartbeat()
        self.event_log("🫀 Heartbeat writer stopped.")

    # ------------------------------------------------------
    # INTERNET CHECK
    # ------------------------------------------------------
    def is_internet(self) -> bool:
        try:
            socket.create_connection(("8.8.8.8", 53), self.cfg.DNS_TEST_TIMEOUT)
            return True
        except OSError:
            try:
                requests.head("https://www.google.com", timeout=3)
                return True
            except Exception:
                return False

    # ------------------------------------------------------
    # TELEGRAM
    # ------------------------------------------------------
    def send_telegram_async(self, msg: str):
        if not (self.internet_online and self.cfg.BOT_TOKEN and self.cfg.CHAT_ID):
            return
        threading.Thread(target=self._send_telegram_safe, args=(msg,), daemon=True).start()

    def _send_telegram_safe(self, msg: str):
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.cfg.BOT_TOKEN}/sendMessage",
                data={"chat_id": self.cfg.CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=5,
            )
        except Exception as e:
            self.logger.warning(f"Telegram send failed: {e}")

    # ------------------------------------------------------
    def event_log(self, msg: str):
        self.logger.info(msg)
        self.send_telegram_async(msg)

    # ------------------------------------------------------
    # OUTPUT STREAMING
    # ------------------------------------------------------
    def enqueue_output(self, pipe, q: queue.Queue, _):
        for line in iter(pipe.readline, ''):
            text = line.strip()
            if not text:
                continue

            if " - WARNING - " in text:
                level = "warning"
            elif " - INFO - " in text:
                level = "info"
            else:
                level = "error"
            q.put((level, text))
        pipe.close()


    def stream_output(self, proc: subprocess.Popen):
        q = queue.Queue()
        threading.Thread(target=self.enqueue_output, args=(proc.stderr, q, "error"), daemon=True).start()

        self.event_log("🧠 Error-only monitor thread started.")

        last_error_send = time.time()
        last_internet_check = last_error_send
        errors_to_report: list[str] = []

        while proc.poll() is None and not self.stop_flag.is_set():
            try:
                while not q.empty():
                    level, line = q.get_nowait()
                    if not line:
                        continue

                    if level == "info":
                        self.logger.info(line)
                    elif level == "warning":
                        self.logger.warning(line)
                        errors_to_report.append(line)
                    else:
                        self.logger.error(line)
                        errors_to_report.append(line)

            except queue.Empty:
                pass
            except Exception as e:
                self.logger.error(f"Error processing output queue: {e}")

            now = time.time()

            # --- Internet check ---
            if now - last_internet_check >= self.cfg.INTERNET_CHECK_INTERVAL:
                try:
                    online = self.is_internet()
                    if online != self.internet_online:
                        self.internet_online = online
                        with self.status_lock:
                            self.status["internet_online"] = online
                            self.status["last_internet_change"] = self._now()

                        if not online:
                            self.event_log("🌐 Internet lost — stopping main.py.")
                            self._terminate_process(proc)
                            return
                        else:
                            self.event_log("✅ Internet restored.")
                except Exception as e:
                    self.logger.error(f"Internet check failed: {e}")
                last_internet_check = now

            # --- Telegram error batching ---
            if now - last_error_send >= self.cfg.ERROR_MSG_INTERVAL:
                if errors_to_report:
                    try:
                        unique_errs = list(set(errors_to_report))
                        seen_error = set()
                        for err in unique_errs:
                            try:
                                if not any(fuzz.ratio(err, s) >= 90 for s in seen_error):
                                    self.send_telegram_async(f"⚠ {err}")
                                    seen_error.add(err)
                            except Exception as e:
                                self.logger.error(f"Error sending log '{err}': {e}")
                        self.logger.info(f"Sent {len(seen_error)} unique error logs.")
                        errors_to_report.clear()
                    except Exception as e:
                        self.logger.error(f"Error in error batching: {e}")
                last_error_send = now

            time.sleep(self.cfg.STREAM_POLL_INTERVAL)
    # ------------------------------------------------------
    # PROCESS mgmt
    # ------------------------------------------------------
    def _terminate_process(self, proc: subprocess.Popen):
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.logger.warning("Graceful shutdown timed out. Forcing kill...")
            proc.kill()
        except Exception as e:
            self.logger.error(f"while terminating process: {e}")

    def wait_for_stable_internet(self):
        stable_count = 0
        while not self.stop_flag.is_set() and stable_count < 3:
            if self.is_internet():
                stable_count += 1
            else:
                stable_count = 0
            time.sleep(self.cfg.INTERNET_CHECK_INTERVAL)

        self.internet_online = True
        with self.status_lock:
            self.status["internet_online"] = True
            self.status["last_internet_change"] = self._now()
        self.event_log("✅ Internet stable again. Restarting main.py.")

    # ------------------------------------------------------
    # MAIN LOOP
    # ------------------------------------------------------
    def run(self):
        self.event_log("🚀 SupervisorV2.1 started.")

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        try:
            while not self.stop_flag.is_set():
                if not self.is_internet():
                    self.logger.warning("🌐 No internet. Waiting for stability...")
                    self.wait_for_stable_internet()
                    if self.stop_flag.is_set():
                        break

                self.internet_online = True
                with self.status_lock:
                    self.status["internet_online"] = True
                    self.status["last_internet_change"] = self._now()

                self.event_log(f"▶ Starting {self.cfg.TARGET_SCRIPT}")

                proc = subprocess.Popen(
                    ["python", self.cfg.TARGET_SCRIPT],
                    stdout=subprocess.DEVNULL,  
                    stderr=subprocess.PIPE,   
                    text=True,
                    bufsize=1,
                )
                self.current_proc = proc

                with self.status_lock:
                    self.status["restart_count"] += 1
                    self.status["child_running"] = True
                    self.status["child_exit_code"] = None
                    self.status["last_child_start"] = self._now()

                threading.Thread(target=self.stream_output, args=(proc,), daemon=True).start()

                while proc.poll() is None and not self.stop_flag.is_set():
                    time.sleep(0.5)

                if proc.poll() is None and self.stop_flag.is_set():
                    try:
                        self._terminate_process(proc)
                    except Exception as e:
                        self.logger.error(f"terminating child on shutdown: {e}")

                exit_code = proc.poll()
                with self.status_lock:
                    self.status["child_running"] = False
                    self.status["child_exit_code"] = exit_code
                    self.status["last_child_exit"] = self._now()

                if self.stop_flag.is_set():
                    break

                if not self.internet_online:
                    self.wait_for_stable_internet()
                    continue

                msg = (
                    f"💥 {self.cfg.TARGET_SCRIPT} crashed (exit {exit_code})"
                    if exit_code != 0
                    else f"⚠ {self.cfg.TARGET_SCRIPT} exited normally (unexpected)."
                )
                self.event_log(msg)
                time.sleep(self.cfg.RESTART_DELAY)

        except KeyboardInterrupt:
            self.logger.warning("🛑 Supervisor interrupted by KeyboardInterrupt.")
            self.stop_flag.set()
            proc = self.current_proc
            if proc is not None and proc.poll() is None:
                try:
                    self._terminate_process(proc)
                except Exception as e:
                    self.logger.error(f"Error terminating child on KeyboardInterrupt: {e}")
        finally:
            with self.status_lock:
                self.status["supervisor_running"] = False
                self.status["child_running"] = False
                self.status["last_child_exit"] = self._now()
            self._write_heartbeat()
            self.logger.info("👋 SupervisorV2.1 shutting down.")


# ==========================================================
# MAIN ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    try:
        cfg = Config()
        SupervisorV2_1(cfg).run()
    except Exception as e:
        print(f"💀 FATAL SUPERVISOR CRASH: {e}")
        try:
            load_dotenv()
            token, chat = os.getenv("BOT_TOKEN"), os.getenv("CHAT_ID")
            if token and chat:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={
                        "chat_id": chat,
                        "text": f'💀 *FATAL:* Supervisor crashed\n```\n{e}\n```',
                        "parse_mode": "Markdown",
                    },
                    timeout=5,
                )
        except Exception:
            pass









# import os
# import time
# import subprocess
# import threading
# import requests
# import socket
# import re
# from datetime import datetime
# from pathlib import Path
# from dotenv import load_dotenv
# from core.logger import get_logger


# # ==========================================================
# # CONFIG
# # ==========================================================
# class Config:
#     def __init__(self):
#         load_dotenv()
#         base = Path(__file__).resolve().parent.parent

#         # Directories
#         self.LOG_DIR = base / "logs"
#         self.LOG_DIR.mkdir(parents=True, exist_ok=True)

#         # Target + Telegram
#         self.TARGET_SCRIPT = os.getenv("TARGET_SCRIPT", "main.py")
#         self.BOT_TOKEN = os.getenv("BOT_TOKEN")
#         self.CHAT_ID = os.getenv("CHAT_ID")

#         # Timing controls (configurable from .env)
#         self.RESTART_DELAY = int(os.getenv("RESTART_DELAY", 10))
#         self.INTERNET_CHECK_INTERVAL = int(os.getenv("INTERNET_CHECK_INTERVAL", 10))
#         self.ERROR_MSG_INTERVAL = int(os.getenv("ERROR_MSG_INTERVAL", 60))
#         self.STREAM_POLL_INTERVAL = float(os.getenv("STREAM_POLL_INTERVAL", 0.2))
#         self.DNS_TEST_TIMEOUT = int(os.getenv("DNS_TEST_TIMEOUT", 2))
#         self.ERROR_MSG_NO_INTERVAL = 10


# # ==========================================================
# # SUPERVISOR
# # ==========================================================
# class Supervisor:
#     def __init__(self, cfg: Config):
#         self.cfg = cfg
#         self.logger = get_logger("supervisor", save_time_logs=True)
#         self.output_log = get_logger("output", save_time_logs=True)
#         self.internet_online = False
#         self.error_pattern = re.compile(r"(ERROR|Exception|Traceback|failed|timeout)", re.I)

#     # ------------------------------------------------------
#     def is_internet(self) -> bool:
#         """Ping 8.8.8.8 to test internet connection."""
#         try:
#             socket.create_connection(("8.8.8.8", 53), self.cfg.DNS_TEST_TIMEOUT)
#             return True
#         except OSError:
#             return False

#     # ------------------------------------------------------
#     def send_telegram(self, msg: str):
#         if not (self.internet_online and self.cfg.BOT_TOKEN and self.cfg.CHAT_ID):
#             return
#         try:
#             requests.post(
#                 f"https://api.telegram.org/bot{self.cfg.BOT_TOKEN}/sendMessage",
#                 data={"chat_id": self.cfg.CHAT_ID, "text": msg, "parse_mode": "Markdown"},
#                 timeout=5,
#             )
#         except Exception as e:
#             self.event_log(f"Telegram send failed: {e}")

#     # ------------------------------------------------------
#     def event_log(self, msg: str):
#         self.logger.info(msg)
#         self.send_telegram(msg)

#     # ------------------------------------------------------
#     def stream_output(self, proc: subprocess.Popen):
#         self.event_log("🧠 Output/Monitor thread started.")
#         last_telegram_time = time.time()
#         last_internet_time = last_telegram_time
#         errorlines = []
#         errorsend = set()

#         while proc.poll() is None:
#             try:
#                 infoline = proc.stdout.readline()
#                 errorline = proc.stderr.readline()
#             except Exception as e:
#                 self.event_log(f"💥 stream_output due to read error: {e}")
#                 continue
            
#             if infoline and infoline.strip():
#                 self.output_log.info(infoline.strip())
#             if errorline and errorline.strip():
#                 self.output_log.error(errorline.strip())
#                 errorlines.append(errorline.strip())
            
#             if time.time() - last_internet_time >= self.cfg.INTERNET_CHECK_INTERVAL:
#                 online = self.is_internet()
#                 self.internet_online = online
#                 if not online:
#                     self.event_log("🌐 Internet lost — stopping main.py.")
#                     try:
#                         proc.terminate()
#                         proc.wait(timeout=20)
#                     except subprocess.TimeoutExpired as e:
#                         self.logger.error(f"Failed to terminate main.py gracefully: {e}")
#                         proc.kill()
#                     except Exception as e:
#                         self.logger.error(f"Failed to terminate main.py using kill: {e}")
#                     return 
#                 last_internet_time = time.time()

#             if time.time() - last_telegram_time >= self.cfg.ERROR_MSG_INTERVAL:
#                 if errorlines:
#                     count = 0
#                     for error in errorlines:
#                         if error not in errorsend:
#                             self.send_telegram(f"{error}")
#                             errorsend.add(error)
#                             count += 1
#                         if count > self.cfg.ERROR_MSG_NO_INTERVAL:
#                             break
#                     errorlines = []
#                     errorsend = set()
#                 else:
#                     self.logger.info(f"No Errorlog found after {last_telegram_time}")
#                 last_telegram_time = time.time()
#             time.sleep(self.cfg.STREAM_POLL_INTERVAL)


#     # ------------------------------------------------------
#     def run(self):
#         self.event_log("🚀 Supervisor started.")

#         while True:
#             try:
#                 if self.is_internet():
#                     self.internet_online = True
#                     self.event_log(f"▶ Starting {self.cfg.TARGET_SCRIPT}")
                    
#                     proc = subprocess.Popen(
#                         ["python", self.cfg.TARGET_SCRIPT],
#                         stderr=subprocess.PIPE,
#                         stdout=subprocess.PIPE,
#                         text=True,
#                         bufsize=1,
#                     )

#                     threading.Thread(target=self.stream_output, args=(proc,), daemon=True).start()
#                     proc.wait()

#                     if not self.internet_online:
#                         self.logger.warning("🌐 Internet offline — waiting before restart.")        
#                         while not self.is_internet():
#                             self.logger.warning("🌐 No internet. Waiting before starting main.py...")
#                             time.sleep(self.cfg.INTERNET_CHECK_INTERVAL)
#                         self.internet_online = True
#                         self.event_log("✅ Internet restored — restarting main.py.")
#                         continue

#                     msg = (
#                         f"💥 {self.cfg.TARGET_SCRIPT} crashed (exit {proc.returncode})"
#                         if proc.returncode != 0
#                         else f"⚠ {self.cfg.TARGET_SCRIPT} exited normally (unexpected)."
#                     )
#                     self.event_log(msg)
#                     time.sleep(self.cfg.RESTART_DELAY)

#             except Exception as e:
#                 self.event_log(f"❌ Supervisor runtime error: {e}")
#                 time.sleep(self.cfg.RESTART_DELAY)


# # ==========================================================
# # MAIN ENTRY POINT
# # ==========================================================
# if __name__ == "__main__":
#     try:
#         cfg = Config()
#         Supervisor(cfg).run()
#     except Exception as e:
#         print(f"💀 FATAL SUPERVISOR CRASH: {e}")
#         try:
#             load_dotenv()
#             token, chat = os.getenv("BOT_TOKEN"), os.getenv("CHAT_ID")
#             if token and chat:
#                 requests.post(
#                     f"https://api.telegram.org/bot{token}/sendMessage",
#                     data={
#                         "chat_id": chat,
#                         "text": f'💀 *FATAL:* Supervisor crashed\n```\n{e}\n```',
#                         "parse_mode": "Markdown",
#                     },
#                     timeout=5,
#                 )
#         except Exception:
#             pass
