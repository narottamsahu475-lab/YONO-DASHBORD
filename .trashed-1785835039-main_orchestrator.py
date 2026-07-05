import asyncio, aiohttp, requests, time, re, os, logging, json
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("PantherOrchestrator")

BASE_DIR = Path(__file__).resolve().parent
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- CONFIGURATION CREDENTIALS ---
FOUR_SIM_API_KEY = "b3c8e0cccaba35d807bd30bf7d7cb897"
PANTHERS_API_KEY = "panthers_59T9iL6hei22g7W_uPij_k56ncRBG8nEW8mY6Q"
BASE_URL = "https://panthers.accbazaar.shop"
HEADERS = {"X-API-Key": PANTHERS_API_KEY, "Content-Type": "application/json"}
http_session = None

# --- FULLY COMPATIBLE DATA MATRIX FOR PANEL.HTML ---
live_stats = {
    "total_targeted": 0, "active_threads": 0, "success_otps": 0, "already_registered": 0,
    "cancelled_orders": 0, "total_secured": 0, "completed": 0, "remaining": 0, "progress": 0,
    "eta": "--", "system_status": "Ready to Start", "engine_state": "Idle", "pipeline_running": False,
    "success_records": [], "recent_activity": [], "registration_summary": {}, "activity_timeline": [],
    "game_analytics": {}, "health_check": {"internet": "Connected", "4sim": "Connected", "panther": "Connected"}
}

stats_lock, buy_lock, stop_event = asyncio.Lock(), asyncio.Lock(), asyncio.Event()
graceful_stop, running_workers, success_buy_count = False, 0, 0
input_total_accounts, active_task_counter, global_service_id = 0, 0, "1929"
logged_cancels, active_already_tasks = set(), set()

def mask_sensitive(d): return f"+91 {str(d)[:3]}****{str(d)[-3:]}" if len(str(d)) > 6 else "****"

async def increment_completed_metric():
    async with stats_lock:
        live_stats["completed"] += 1
        live_stats["remaining"] = max(0, live_stats["total_targeted"] - live_stats["completed"])
        live_stats["progress"] = int((live_stats["completed"] / live_stats["total_targeted"]) * 100) if live_stats["total_targeted"] > 0 else 0
        live_stats["eta"] = f'{live_stats["remaining"] * 35} sec'

async def production_health_checker_loop():
    loop = asyncio.get_event_loop()
    while True:
        try:
            try: 
                r_net = await loop.run_in_executor(None, lambda: requests.get("https://1.1.1.1", timeout=3))
                n_st = "Connected" if r_net.status_code == 200 else "Offline"
            except Exception: n_st = "Offline"
            
            try: 
                r_sim = await loop.run_in_executor(None, lambda: requests.get(f"https://api.4sim.st/getBalanceAndRating?apikey={FOUR_SIM_API_KEY}", timeout=5))
                s_st = "Connected" if r_sim.status_code in [200, 400, 401] else "Offline"
            except Exception: s_st = "Offline"
            
            try: 
                r_pan = await loop.run_in_executor(None, lambda: requests.get(BASE_URL, headers=HEADERS, timeout=5))
                p_st = "Connected" if r_pan.status_code in [200, 401, 403, 404, 405] else "Offline"
            except Exception: p_st = "Offline"
            
            async with stats_lock: live_stats["health_check"] = {"internet": n_st, "4sim": s_st, "panther": p_st}
        except asyncio.CancelledError: break
        except Exception as e: logger.error(f"Health fault tracker error: {e}")
        await asyncio.sleep(20)

@app.on_event("startup")
async def app_startup_handler():
    global http_session
    http_session = aiohttp.ClientSession()
    asyncio.create_task(production_health_checker_loop())

@app.on_event("shutdown")
async def app_shutdown_handler():
    global http_session
    if http_session: await http_session.close()

async def add_activity(phone, text, color_val="🟢"):
    async with stats_lock:
        live_stats["recent_activity"].insert(0, {"phone": phone, "current_game": "Bingo_game", "status": text, "balance": "0 INR", "retry": 1, "progress": 100, "thread_color": color_val})
        if len(live_stats["recent_activity"]) > 100: live_stats["recent_activity"].pop()

async def add_timeline_event(phone, stage, current_game="System"):
    async with stats_lock:
        live_stats["activity_timeline"].insert(0, {"time": datetime.now().strftime("%H:%M:%S"), "phone": phone, "current_game": current_game, "stage": stage})
        if len(live_stats["activity_timeline"]) > 200: live_stats["activity_timeline"].pop()

async def update_registration_summary(game):
    async with stats_lock:
        if game not in live_stats["registration_summary"]: live_stats["registration_summary"][game] = 0
        live_stats["registration_summary"][game] += 1

async def log_game_metric(game_name, status="success"):
    async with stats_lock:
        if game_name not in live_stats["game_analytics"]: live_stats["game_analytics"][game_name] = {"success": 0, "already": 0, "failed": 0}
        if status == "success": live_stats["game_analytics"][game_name]["success"] += 1
        elif status == "already": live_stats["game_analytics"][game_name]["already"] += 1
        else: live_stats["game_analytics"][game_name]["failed"] += 1

async def update_live_status(phone, current_game, status_text, balance_text="0 INR", retry_val=1, progress_val=0, color_val="🟢"):
    async with stats_lock:
        for n in live_stats["recent_activity"]:
            if n["phone"] == phone:
                n["current_game"], n["status"] = current_game, status_text
                n["balance"], n["retry"], n["progress"], n["thread_color"] = balance_text, retry_val, progress_val, color_val; break

async def remove_worker_node(phone):
    async with stats_lock: live_stats["recent_activity"] = [w for w in live_stats["recent_activity"] if w["phone"] != phone]

async def fetch_smart_otp_async(txn_id, used_otps):
    if http_session is None or http_session.closed: return None
    url = f"https://api.4sim.st/checkSms?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
    try:
        async with http_session.get(url, timeout=5) as resp:
            if resp.status != 200: return None
            res = await resp.json(content_type=None)
            sms_text = str(res.get("sms") or res.get("code") or "")
            if sms_text:
                all_codes = re.findall(r'\b\d{4}\b|\b\d{6}\b', sms_text)
                new_codes = [c for c in all_codes if c not in used_otps]
                if new_codes: return new_codes[-1]
    except Exception as e: logger.error(f"OTP Fetch err: {e}")
    return None

async def terminate_4sim_order_async(txn_id, otp_received, phone, force_cancel=False):
    if http_session is None or http_session.closed: return "Session Uninitialized"
    async with stats_lock:
        if txn_id in logged_cancels: return "Already Handled"
    url = f"https://api.4sim.st/{'cancelNumber' if (force_cancel or not otp_received) else 'finishOrder'}?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
    if force_cancel or not otp_received:
        async with stats_lock: live_stats["cancelled_orders"] += 1
    final_status = "Failed Completely"
    for _ in range(3):
        try:
            async with http_session.get(url, timeout=10) as resp:
                raw_body = await resp.text(); raw_body_lower = raw_body.lower()
                if "already cancelled" in raw_body_lower or "not found" in raw_body_lower: final_status = "Already Cancelled"; break
                if "error" in raw_body_lower or "fail" in raw_body_lower: final_status = f"API Err: {raw_body[:20]}"; break
                if resp.status in [200, 400]: final_status = "Cancelled Successfully" if (force_cancel or not otp_received) else "Finished Successfully"; break
        except Exception: pass
        await asyncio.sleep(2)
    if final_status in ["Cancelled Successfully", "Finished Successfully", "Already Cancelled"]:
        async with stats_lock: logged_cancels.add(txn_id)
    return final_status

async def handle_already_number(phone, txn_id, otp_flag):
    active_already_tasks.add(asyncio.current_task())
    try:
        remaining_seconds = 140
        while remaining_seconds > 0:
            if stop_event.is_set(): break
            await update_live_status(phone, "System Queue", f"Release Hold: {remaining_seconds}s", "0 INR", 1, int((remaining_seconds/140)*100), "🟠")
            await asyncio.sleep(2); remaining_seconds -= 2
        status = await terminate_4sim_order_async(txn_id, otp_flag, phone, force_cancel=True)
        await add_activity(phone, f"Queue Cleared: {status}", "🔴")
    except Exception: pass
    finally:
        active_already_tasks.discard(asyncio.current_task()); await remove_worker_node(phone); await increment_completed_metric()

async def handle_failed_number(phone, txn_id):
    try:
        await update_live_status(phone, "Retry Manager", "Proxy Block Cool-off", "0 INR", 3, 0, "🔴")
        await asyncio.sleep(140)
        status = await terminate_4sim_order_async(txn_id, False, phone, force_cancel=True)
        await add_activity(phone, f"Failed Released: {status}", "🔴")
    except Exception: pass
    finally: await remove_worker_node(phone); await increment_completed_metric()

async def run_game_step_async(phone, txn_id, app_name, used_otps, is_sub_game=False, active_wallet_ref="0 INR"):
    if http_session is None or http_session.closed: return "failed", active_wallet_ref
    send_attempt, otp_sent_successfully = 1, False
    while send_attempt <= 3:
        try:
            await update_live_status(phone, app_name, "Running Request", active_wallet_ref, send_attempt, 15, "🟢")
            await add_timeline_event(mask_sensitive(phone), f"OTP Request Triggered", app_name)
            async with http_session.post(f"{BASE_URL}/v1/register/send_otp", headers=HEADERS, json={"phone": phone, "app_name": str(app_name)}, timeout=12) as response:
                raw_t = await response.text()
                if response.status != 200: send_attempt += 1; await asyncio.sleep(3); continue
                try: v_res = json.loads(raw_t)
                except Exception: send_attempt += 1; await asyncio.sleep(3); continue
                error_msg = str(v_res.get("message", "")).lower()
                if "rate limit" in error_msg:
                    await update_live_status(phone, app_name, "Rate Limit Wait", active_wallet_ref, send_attempt, 5, "🟡")
                    await asyncio.sleep(15); continue
                if v_res.get("status") == "success":
                    task_id = v_res.get("task_id")
                    if not task_id: send_attempt += 1; await asyncio.sleep(3); continue
                    otp_sent_successfully = True
                    for elapsed in range(0, 131, 5):
                        if stop_event.is_set(): return "timeout", active_wallet_ref
                        await update_live_status(phone, app_name, f"Waiting OTP ({elapsed}s)", active_wallet_ref, send_attempt, int((elapsed/130)*80), "🟡")
                        await asyncio.sleep(5)
                        otp = await fetch_smart_otp_async(txn_id, used_otps)
                        if otp:
                            await update_live_status(phone, app_name, "Verifying Balance", active_wallet_ref, send_attempt, 95, "🔵")
                            await add_timeline_event(mask_sensitive(phone), f"OTP Arrived: {otp}", app_name)
                            async with http_session.post(f"{BASE_URL}/v1/register/verify_otp", headers=HEADERS, json={"task_id": str(task_id), "otp": str(otp)}, timeout=15) as v_resp:
                                if v_resp.status != 200: return "failed", active_wallet_ref
                                raw_v = await v_resp.text()
                                try: verify_res = json.loads(raw_v)
                                except Exception: return "failed", active_wallet_ref
                                if verify_res.get("status") == "success":
                                    active_wallet_ref = f"{str(int(float(verify_res.get('data', {}).get('account_balance', 0))))} INR"
                                    await update_live_status(phone, app_name, "SUCCESS ✓", active_wallet_ref, send_attempt, 100, "✅")
                                    await add_timeline_event(mask_sensitive(phone), "Registration Success", app_name)
                                    used_otps.add(otp); await log_game_metric(app_name, "success"); await update_registration_summary(app_name)
                                    return True, active_wallet_ref
                                else:
                                    await update_live_status(phone, app_name, "Verification Failed", active_wallet_ref, send_attempt, 100, "🔴")
                                    used_otps.add(otp); await log_game_metric(app_name, "failed")
                                    return False, active_wallet_ref
                    await update_live_status(phone, app_name, "OTP Timeout (130s)", active_wallet_ref, send_attempt, 100, "🔴")
                    return "timeout", active_wallet_ref
                if "already" in error_msg: await log_game_metric(app_name, "already"); return "already", active_wallet_ref
            send_attempt += 1; await asyncio.sleep(3)
        except Exception: send_attempt += 1; await asyncio.sleep(3)
        if otp_sent_successfully: break
    await update_live_status(phone, app_name, "Engine Failed", active_wallet_ref, send_attempt, 100, "🔴")
    await log_game_metric(app_name, "failed"); return "failed", active_wallet_ref

async def process_single_registration():
    global success_buy_count, active_task_counter, input_total_accounts, global_service_id, running_workers, graceful_stop, http_session
    worker_registered, phone = False, None
    async with stats_lock:
        if stop_event.is_set() or graceful_stop or (live_stats["completed"] >= input_total_accounts): return
        running_workers += 1; worker_registered = True
    try:
        if http_session is None or http_session.closed: return
        used_otps = set(); games_registered, already_registered_count, last_balance = 0, 0, "0 INR"; worker_start_time = time.time()
        async with buy_lock:
            async with stats_lock:
                if live_stats["completed"] >= input_total_accounts or stop_event.is_set(): return
                live_stats["system_status"] = "Securing Stock..."
            buy_url = f"https://api.4sim.st/buyNumber?apikey={FOUR_SIM_API_KEY}&id={global_service_id}&country=22"
            try:
                async with http_session.get(buy_url, timeout=12) as response:
                    if response.status != 200: return
                    raw_b = await response.text()
                    try: buy_res = json.loads(raw_b)
                    except Exception: logger.error("JSON parse failure."); return
                    phone = str(buy_res.get("number", ""))[-10:]; txn_id = buy_res.get("tid") or buy_res.get("id")
                    if not phone: return
                    async with stats_lock: 
                        success_buy_count += 1; active_task_counter += 1
                        live_stats["total_secured"] = success_buy_count
            except Exception: return
        
        async with stats_lock:
            live_stats["recent_activity"].insert(0, {"phone": phone, "current_game": "567slot_game", "status": "Running Request", "balance": last_balance, "retry": 1, "progress": 5, "thread_color": "🟢"})
            if len(live_stats["recent_activity"]) > 100: live_stats["recent_activity"].pop()
        
        main_res, last_balance = await run_game_step_async(phone, txn_id, "567slot_game", used_otps, False, last_balance)
        if main_res == "already":
            async with stats_lock: live_stats["already_registered"] += 1
            asyncio.create_task(handle_already_number(phone, txn_id, True)); return
        elif main_res in ["timeout", "failed", False]:
            asyncio.create_task(handle_failed_number(phone, txn_id)); return
        elif main_res is True:
            async with stats_lock: live_stats["success_otps"] += 1
            games_registered = 1; await asyncio.sleep(1)
            other_games = ["Yono_vip", "spincrush_game", "yonoslot_game", "789jackpot_game", "hirummy_game", "Yonogame_game", "mbmbet_game"]
            for game in other_games:
                if stop_event.is_set(): break
                sub_res, last_balance = await run_game_step_async(phone, txn_id, game, used_otps, True, last_balance)
                if sub_res is True: games_registered += 1
                elif sub_res == "already": already_registered_count += 1
                await asyncio.sleep(1.5)
            elapsed = round(time.time() - worker_start_time, 1)
            async with stats_lock:
                live_stats["success_records"].insert(0, {"phone": phone, "games_registered": games_registered, "already_registered": already_registered_count, "wallet": last_balance, "status": "Completed", "execution_time": f"{elapsed} sec", "time": datetime.now().strftime("%H:%M:%S")})
                if len(live_stats["success_records"]) > 150: live_stats["success_records"].pop()
            await terminate_4sim_order_async(txn_id, True, phone)
        await increment_completed_metric()
    except Exception as exc: logger.exception(f"Primary thread executor error: {exc}")
    finally:
        if worker_registered:
            async with stats_lock: running_workers -= 1
        if phone: await remove_worker_node(phone)

async def dynamic_pipeline_runner(semaphore):
    while not stop_event.is_set() and not graceful_stop:
        async with stats_lock:
            if live_stats["completed"] >= input_total_accounts: break
        async with semaphore: await process_single_registration()
        await asyncio.sleep(0.5)

async def core_engine_orchestrator(target, threads):
    global live_stats, success_buy_count, active_task_counter, input_total_accounts, logged_cancels, graceful_stop, running_workers
    success_buy_count, active_task_counter, input_total_accounts = 0, 0, target
    logged_cancels.clear(); active_already_tasks.clear()
    if stop_event.is_set(): stop_event.clear()
    graceful_stop = False
    async with stats_lock:
        live_stats.update({
            "total_targeted": target, "active_threads": threads, "pipeline_running": True,
            "success_otps": 0, "already_registered": 0, "cancelled_orders": 0, "total_secured": 0,
            "completed": 0, "remaining": target, "progress": 0, "eta": f"{target * 35} sec",
            "system_status": "Running | Active Loop", "engine_state": "Running",
            "success_records": [], "recent_activity": [], "registration_summary": {}, "activity_timeline": [], "game_analytics": {}
        })
    semaphore = asyncio.Semaphore(threads)
    workers = [asyncio.create_task(dynamic_pipeline_runner(semaphore)) for _ in range(threads)]
    while not stop_event.is_set() and not graceful_stop:
        async with stats_lock:
            if live_stats["completed"] >= input_total_accounts: break
            live_stats["active_threads"] = len([w for w in workers if not w.done()])
        await asyncio.sleep(1)
    while True:
        async with stats_lock:
            if not (graceful_stop and running_workers > 0): break
            live_stats["system_status"] = f"Waiting Workers ({running_workers})"
        await asyncio.sleep(1)
    stop_event.set(); await asyncio.gather(*workers, return_exceptions=True)
    while True:
        async with stats_lock:
            if len(active_already_tasks) == 0: break
            live_stats["system_status"] = f"Awaiting {len(active_already_tasks)} delay-cancels..."
        await asyncio.sleep(1)
    async with stats_lock: live_stats.update({"pipeline_running": False, "system_status": "Completed", "engine_state": "Finished", "active_threads": 0})

@app.post("/api/start")
async def api_start(request: Request):
    global global_service_id
    data = await request.json()
    global_service_id = str(data.get('service_id', '1929')).strip()
    asyncio.create_task(core_engine_orchestrator(int(data.get('target', 10)), int(data.get('threads', 2))))
    return {"status": "success"}

@app.post("/api/stop")
async def api_stop():
    global graceful_stop; graceful_stop = True
    async with stats_lock: live_stats.update({"pipeline_running": False, "engine_state": "Stopping", "system_status": "Graceful Stop Requested"})
    return {"status": "graceful_stop"}

@app.get("/api/logs")
async def api_logs(): return JSONResponse(content=live_stats)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    try:
        with open(BASE_DIR / "panel.html", "r", encoding="utf-8") as f: return f.read()
    except Exception as ex: return f"<h3>panel.html resolution error: {ex}</h3>"