import asyncio
import aiohttp
import requests
import time
import re
import json
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- BASE DIRECTORY CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GLOBAL TRACKING STATES ---
FOUR_SIM_API_KEY = "12b75fd7d82310767694a9aa8cd3ccc"
PANTHERS_API_KEY = "panthers_59T9iL6hei22g7W_uPij_k56ncRBG8nEW8mY6Q"
BASE_URL = "https://panthers.accbazaar.shop"
HEADERS = {"X-API-Key": PANTHERS_API_KEY, "Content-Type": "application/json"}

live_stats = {
    "total_targeted": 0,
    "active_threads": 0,
    "success_otps": 0,
    "already_registered": 0,
    "cancelled_orders": 0,
    "total_secured": 0,
    "system_status": "Ready to Start",
    "pipeline_running": False,
    "progress": 0,
    "eta": "---",
    "success_records": [],
    "recent_activity": [],
    "game_analytics": {},
    "registration_summary": {},        # BUG 2 FIX: Added missing structural key
    "activity_timeline": [],          # Added for Frontend Live Activity Timeline
    "health_check": {                  # Added for Infrastructure Diagnostic Health
        "internet": "Connected",
        "4sim": "Connected",
        "panther": "Connected"
    }
}

stats_lock = asyncio.Lock()
buy_lock = asyncio.Lock()
stop_event = asyncio.Event()

logged_cancels = set()
active_already_tasks = set()

success_buy_count = 0
input_total_accounts = 0
active_task_counter = 0
global_service_id = "1929" 

# Dynamic Game Analytics Helper Injection Module
async def log_game_metric(game_name, status="success"):
    async with stats_lock:
        if game_name not in live_stats["game_analytics"]:
            live_stats["game_analytics"][game_name] = {"success": 0, "failed": 0, "already": 0}
        if status == "success":
            live_stats["game_analytics"][game_name]["success"] += 1
        elif status == "already":
            live_stats["game_analytics"][game_name]["already"] += 1
        else:
            live_stats["game_analytics"][game_name]["failed"] += 1

async def add_timeline_event(phone, stage):
    async with stats_lock:
        live_stats["activity_timeline"].insert(0, {
            "time": datetime.now().strftime("%H:%M:%S"),
            "phone": phone,
            "stage": stage
        })
        if len(live_stats["activity_timeline"]) > 30:
            live_stats["activity_timeline"].pop()

async def update_live_status(phone, status_text, balance_text=None, log_type="active", target_game=None, retry_idx=None, progress_val=None):
    async with stats_lock:
        for num_entry in live_stats["recent_activity"]:
            if num_entry["phone"] == phone:
                num_entry["status"] = status_text
                num_entry["log_type"] = log_type
                if balance_text is not None:
                    num_entry["balance"] = balance_text
                if target_game is not None:
                    num_entry["current_game"] = target_game # BUG 1 FIX: Update current game context dynamic
                if retry_idx is not None:
                    num_entry["retry"] = retry_idx
                if progress_val is not None:
                    num_entry["progress"] = progress_val
                break

async def fetch_smart_otp_async(txn_id, used_otps):
    url = f"https://api.4sim.st/checkSms?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                res = await response.json()
                sms_text = str(res.get("sms") or res.get("code") or "")
                if sms_text:
                    all_codes = re.findall(r'\b\d{4}\b|\b\d{6}\b', sms_text)
                    new_codes = [c for c in all_codes if c not in used_otps]
                    if new_codes: return new_codes[-1]
    except: pass
    return None

async def terminate_4sim_order_async(txn_id, otp_received, phone, force_cancel=False):
    global logged_cancels
    
    async with stats_lock:
        if txn_id in logged_cancels:
            return "Already Handled"

    if force_cancel or not otp_received:
        url = f"https://api.4sim.st/cancelNumber?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
        max_retries = 6
        is_cancel = True
    else:
        url = f"https://api.4sim.st/finishOrder?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
        max_retries = 1
        is_cancel = False

    if is_cancel:
        async with stats_lock: 
            live_stats["cancelled_orders"] += 1

    loop = asyncio.get_event_loop()
    final_status = "Failed Completely"
    
    for attempt in range(1, max_retries + 1):
        try:
            response = await loop.run_in_executor(None, lambda: requests.get(
                url, headers={"Connection": "close"}, timeout=10
            ))
            status_code = response.status_code
            raw_text = response.text.strip()

            if status_code == 200 or status_code == 400:
                if "error" in raw_text.lower() or "fail" in raw_text.lower() or "please wait" in raw_text.lower():
                    if "please wait" in raw_text.lower():
                        match = re.search(r"[\d.]+", raw_text)
                        wait_seconds = int(float(match.group())) if match else 30
                        
                        await update_live_status(phone, f"Released Asset Hold", log_type="already")
                        await asyncio.sleep(wait_seconds + 5)
                        continue
                    final_status = f"API_Err: {raw_text[:25]}"
                    break
                final_status = "Released" if is_cancel else "Finished Successfully"
                break
            else:
                if "already cancelled" in raw_text.lower() or "not found" in raw_text.lower():
                    final_status = "Released"
                    break
                if "please wait" in raw_text.lower():
                    match = re.search(r"[\d.]+", raw_text)
                    wait_seconds = int(float(match.group())) if match else 30
                    await update_live_status(phone, f"Terminating Assets", log_type="already")
                    await asyncio.sleep(wait_seconds + 5)
                    continue
                final_status = f"HTTP_{status_code}: {raw_text[:20]}"
        except:
            pass
        await asyncio.sleep(3)
                
    if final_status in ["Released", "Finished Successfully", "Already Cancelled"]:
        async with stats_lock:
            logged_cancels.add(txn_id)
    return final_status

async def handle_already_number(phone, txn_id, otp_flag):
    current_task = asyncio.current_task()
    active_already_tasks.add(current_task)
    try:
        remaining_seconds = 140
        await update_live_status(phone, "Release Hold", log_type="already")
        await add_timeline_event(phone, "Moved to Delayed Queue (Already Reg)")
        
        while remaining_seconds > 0:
            await asyncio.sleep(2)
            remaining_seconds -= 2
            
        status = await terminate_4sim_order_async(txn_id, otp_flag, phone, force_cancel=True)
        await update_live_status(phone, "Cool-off Completed", log_type="cancel")
    except:
        pass
    finally:
        active_already_tasks.discard(current_task)

async def run_game_step_async(phone, txn_id, app_name, used_otps, is_sub_game=False):
    max_attempts = 7 if is_sub_game else 26
    send_attempt = 1
    loop = asyncio.get_event_loop()
    
    while send_attempt <= 3:
        try:
            await update_live_status(phone, "Sending OTP...", target_game=app_name, retry_idx=send_attempt, progress_val=15)
            response_raw = await loop.run_in_executor(None, lambda: requests.post(
                f"{BASE_URL}/v1/register/send_otp", headers=HEADERS, json={"phone": phone, "app_name": str(app_name)}, verify=False, timeout=12
            ))
            v_res = response_raw.json()
            error_msg = str(v_res.get("message", "")).lower()
            
            if "rate limit reached" in error_msg or "active tasks" in error_msg:
                await update_live_status(phone, "Rate Limit (Wait 30s)", progress_val=5)
                await asyncio.sleep(30)
                continue
                
            if v_res.get("status") == "success":
                task_id = v_res.get("task_id")
                await update_live_status(phone, "Waiting OTP...", progress_val=40)
                
                otp_found_flag = False
                for attempt in range(1, max_attempts + 1):
                    await asyncio.sleep(5)
                    otp = await fetch_smart_otp_async(txn_id, used_otps)
                    if otp:
                        otp_found_flag = True
                        await update_live_status(phone, "Submitting OTP...", progress_val=75)
                        verify_res = await loop.run_in_executor(None, lambda: requests.post(
                            f"{BASE_URL}/v1/register/verify_otp", headers=HEADERS, json={"task_id": str(task_id), "otp": str(otp)}, verify=False, timeout=15
                        ).json())
                        
                        if verify_res.get("status") == "success":
                            bal = str(int(float(verify_res.get("data", {}).get("account_balance", 0)))) + " INR"
                            await update_live_status(phone, "SUCCESS", balance_text=bal, progress_val=100)
                            used_otps.add(otp)
                            
                            # BUG 2 FIX: Real-time update into registration summary node
                            async with stats_lock:
                                if app_name not in live_stats["registration_summary"]:
                                    live_stats["registration_summary"][app_name] = 0
                                live_stats["registration_summary"][app_name] += 1
                                
                            await log_game_metric(app_name, "success")
                            await add_timeline_event(phone, f"Success Registered -> {app_name}")
                            return True, True
                        else:
                            await update_live_status(phone, "Wrong OTP", progress_val=90)
                            used_otps.add(otp)
                            await log_game_metric(app_name, "failed")
                            return False, True
                            
                await update_live_status(phone, "Timeout", progress_val=0)
                await log_game_metric(app_name, "failed")
                return "timeout", otp_found_flag
                
            if "already" in error_msg:
                await log_game_metric(app_name, "already")
                return "already", False
                
            send_attempt += 1
            await asyncio.sleep(4)
        except:
            send_attempt += 1
            await asyncio.sleep(4)
            
    await update_live_status(phone, "Failed", progress_val=0)
    await log_game_metric(app_name, "failed")
    return "failed", False

async def process_single_registration():
    global success_buy_count, active_task_counter, input_total_accounts, global_service_id
    
    if stop_event.is_set() or success_buy_count >= input_total_accounts: 
        return
    
    phone, txn_id = None, None
    used_otps = set()
    otp_received_anywhere = False
    success_chains_count = 0
    already_chains_count = 0
    last_known_balance = "0 INR"
    
    async with buy_lock:
        if stop_event.is_set() or success_buy_count >= input_total_accounts: return
        async with stats_lock: live_stats["system_status"] = f"Securing Stock..."
        
        buy_url = f"https://api.4sim.st/buyNumber?apikey={FOUR_SIM_API_KEY}&id={global_service_id}&country=22"
        try:
            loop = asyncio.get_event_loop()
            buy_res = await loop.run_in_executor(None, lambda: requests.get(buy_url, timeout=12).json())
            phone = str(buy_res.get("number", ""))[-10:]
            txn_id = buy_res.get("tid") or buy_res.get("id")
            if not phone: return
            success_buy_count += 1
            active_task_counter += 1
        except: return

    # BUG 1 FIX: Injected fully qualified tracking schema with zero values cleanly
    async with stats_lock:
        live_stats["total_secured"] = success_buy_count
        live_stats["recent_activity"].insert(0, {
            "phone": phone,
            "current_game": "567slot_game",
            "status": "Initializing...",
            "balance": "0 INR",
            "retry": 1,
            "progress": 0,
            "thread_color": "🟢",
            "log_type": "active"
        })
    
    await add_timeline_event(phone, "Acquired New Number from 4Sim")

    main_res, m_otp_flag = await run_game_step_async(phone, txn_id, "567slot_game", used_otps, is_sub_game=False)
    if m_otp_flag: otp_received_anywhere = True
    
    if main_res == "already":
        async with stats_lock: live_stats["already_registered"] += 1
        asyncio.create_task(handle_already_number(phone, txn_id, otp_received_anywhere))
        return
        
    elif main_res in ["timeout", "failed", False]:
        await update_live_status(phone, "Released", log_type="cancel")
        c_status = await terminate_4sim_order_async(txn_id, otp_received_anywhere, phone)
        return
        
    elif main_res is True:
        async with stats_lock: live_stats["success_otps"] += 1
        success_chains_count += 1
        await asyncio.sleep(2) 
        
        other_games = ["mbmbet_game", "yonoslot_game", "789jackpot_game", "spincrush_game", "hirummy_game", "Yonogame_game", "indslot_game"]
        for game in other_games:
            sub_res, s_otp_flag = await run_game_step_async(phone, txn_id, game, used_otps, is_sub_game=True)
            if s_otp_flag: otp_received_anywhere = True
            
            if sub_res is True:
                success_chains_count += 1
                async with stats_lock:
                    for item in live_stats["recent_activity"]:
                        if item["phone"] == phone and "INR" in str(item["balance"]):
                            last_known_balance = item["balance"]
                await asyncio.sleep(2)
            elif sub_res == "already":
                already_chains_count += 1

        fin_status = await terminate_4sim_order_async(txn_id, otp_received_anywhere, phone)
        
        # BUG 3 FIX: Structural mapping cleanly aligned with frontend key architecture requirements
        async with stats_lock:
            live_stats["success_records"].insert(0, {
                "phone": phone,
                "games_registered": f"{success_chains_count} Games",
                "already_registered": f"{already_chains_count} Games",
                "wallet": last_known_balance,
                "status": "Fully Finished",
                "time": datetime.now().strftime("%H:%M:%S")
            })
            # Progress Engine Percent Calc
            live_stats["progress"] = int((success_buy_count / input_total_accounts) * 100)
            
        await update_live_status(phone, "Released", log_type="cancel")

async def dynamic_pipeline_runner(semaphore):
    while not stop_event.is_set() and success_buy_count < input_total_accounts:
        async with semaphore:
            await process_single_registration()
        await asyncio.sleep(1)

async def core_engine_orchestrator(target, threads):
    global live_stats, success_buy_count, active_task_counter, input_total_accounts, logged_cancels
    
    success_buy_count = 0
    active_task_counter = 0
    input_total_accounts = target
    
    logged_cancels.clear()
    active_already_tasks.clear()
    
    async with stats_lock:
        live_stats["total_targeted"] = target
        live_stats["active_threads"] = threads
        live_stats["pipeline_running"] = True
        live_stats["success_otps"] = 0
        live_stats["already_registered"] = 0
        live_stats["cancelled_orders"] = 0
        live_stats["total_secured"] = 0
        live_stats["progress"] = 0
        live_stats["eta"] = "Calculating..."
        live_stats["recent_activity"] = []
        live_stats["game_analytics"] = {} 
        live_stats["registration_summary"] = {}
        live_stats["activity_timeline"] = []
    
    semaphore = asyncio.Semaphore(threads)
    workers = [asyncio.create_task(dynamic_pipeline_runner(semaphore)) for _ in range(threads)]
    
    start_time = time.time()
    while success_buy_count < input_total_accounts and not stop_event.is_set():
        async with stats_lock:
            live_stats["active_threads"] = len([w for w in workers if not w.done()])
            live_stats["system_status"] = f"Running | Active Pipeline Loop"
            
            # Real-time Engine ETA Engine Calculator
            elapsed = time.time() - start_time
            if success_buy_count > 0:
                avg_time = elapsed / success_buy_count
                rem_acc = input_total_accounts - success_buy_count
                eta_secs = int(avg_time * rem_acc)
                live_stats["eta"] = f"{eta_secs // 60}m {eta_secs % 60}s"
                
        await asyncio.sleep(1)
        
    async with stats_lock:
        live_stats["system_status"] = "Stop Received. Completing active runs..."
    
    await asyncio.gather(*workers, return_exceptions=True)
    
    while len(active_already_tasks) > 0:
        async with stats_lock:
            live_stats["system_status"] = f"Awaiting {len(active_already_tasks)} delay-cancels..."
        await asyncio.sleep(1)
        
    async with stats_lock:
        live_stats["pipeline_running"] = False
        live_stats["active_threads"] = 0
        live_stats["system_status"] = "Pipeline Finished / Idle"
        live_stats["eta"] = "---"

@app.post("/api/start")
async def api_start(request: Request):
    global global_service_id
    if stop_event.is_set(): stop_event.clear()
    data = await request.json()
    target = int(data.get('target', 10))
    threads = int(data.get('threads', 2))
    global_service_id = str(data.get('service_id', '1929')).strip()
    
    asyncio.create_task(core_engine_orchestrator(target, threads))
    return {"status": "success"}

@app.post("/api/stop")
async def api_stop():
    stop_event.set()
    return {"status": "graceful_stop_initiated"}

@app.get("/api/logs")
async def api_logs():
    return JSONResponse(content=live_stats)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    try:
        with open(BASE_DIR / "panel.html", "r", encoding="utf-8") as f: return f.read()
    except:
        return "<h3>panel.html file missing inside target app space</h3>"