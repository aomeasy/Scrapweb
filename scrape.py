import os, json, base64, asyncio
import sys
from io import StringIO
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone
import pandas as pd
import hashlib
import requests

from playwright.async_api import async_playwright
import logging

# ---- Google Sheets (gspread) ----
import gspread
from google.oauth2.service_account import Credentials

# ตั้งค่า logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --------- Config ----------
BASE = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"
TABS: List[int] = [13, 14, 15, 8, 7, 11]

# Tab names mapping
TAB_NAMES = {
    13: "InProgress_Jobs",
    14: "Pending_Jobs", 
    15: "Completed_Jobs",
    8: "Urgent_Jobs",
    7: "Review_Jobs",
    11: "Archive_Jobs"
}

USER = os.getenv("EDOCLITE_USER", "").strip()
PASS = os.getenv("EDOCLITE_PASS", "").strip()

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SVC_JSON_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SVC_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

# LINE Notify Config
LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "").strip()

# AI Config
EMBEDDING_API_URL = os.getenv("EMBEDDING_API_URL", "http://209.15.123.47:11434/api/embeddings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text:latest")
CHAT_API_URL = os.getenv("CHAT_API_URL", "http://209.15.123.47:11434/api/generate")
CHAT_MODEL = os.getenv("CHAT_MODEL", "Qwen3:14b")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Master sheet configuration
MASTER_SHEET_NAME = "Master_Data"
SUMMARY_SHEET_NAME = "Data_Summary"
LOG_SHEET_NAME = "Sync_Logs"

# --------- LINE Notify helpers ----------
def send_line_notify(message: str, token: str = None) -> bool:
    """ส่งข้อความแจ้งเตือนผ่าน LINE Notify"""
    if not token and not LINE_NOTIFY_TOKEN:
        logger.warning("⚠️  ไม่พบ LINE_NOTIFY_TOKEN")
        return False
    
    try:
        token = token or LINE_NOTIFY_TOKEN
        url = "https://notify-api.line.me/api/notify"
        headers = {"Authorization": f"Bearer {token}"}
        data = {"message": message}
        
        response = requests.post(url, headers=headers, data=data, timeout=10)
        
        if response.status_code == 200:
            logger.info("📱 ส่ง LINE Notify สำเร็จ")
            return True
        else:
            logger.error(f"❌ LINE Notify ล้มเหลว: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ LINE Notify error: {e}")
        return False

# --------- AI helpers ----------
def get_text_embedding(text: str) -> Optional[List[float]]:
    """สร้าง text embedding สำหรับข้อความ"""
    try:
        payload = {
            "model": EMBEDDING_MODEL,
            "prompt": text
        }
        response = requests.post(EMBEDDING_API_URL, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json().get("embedding")
    except Exception as e:
        logger.error(f"❌ Embedding API error: {e}")
    return None

def analyze_job_changes(old_data: Dict, new_data: Dict, job_no: str) -> str:
    """วิเคราะห์การเปลี่ยนแปลงของ Job โดยใช้ AI"""
    try:
        changes = []
        for key in set(old_data.keys()) | set(new_data.keys()):
            old_val = old_data.get(key, "N/A")
            new_val = new_data.get(key, "N/A")
            if old_val != new_val:
                changes.append(f"{key}: {old_val} → {new_val}")
        
        if not changes:
            return "ไม่มีการเปลี่ยนแปลง"
        
        prompt = f"""
        วิเคราะห์การเปลี่ยนแปลงของ Job No: {job_no}
        การเปลี่ยนแปลง:
        {chr(10).join(changes)}
        
        โปรดสรุปการเปลี่ยนแปลงที่สำคัญและผลกระทบที่อาจเกิดขึ้น (ตอบเป็นภาษาไทย):
        """
        
        payload = {
            "model": CHAT_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.3,
                "top_p": 0.8
            }
        }
        
        response = requests.post(CHAT_API_URL, json=payload, timeout=60)
        if response.status_code == 200:
            result = response.json()
            return result.get("response", "ไม่สามารถวิเคราะห์ได้")
    except Exception as e:
        logger.error(f"❌ AI Analysis error: {e}")
    
    return "ไม่สามารถวิเคราะห์การเปลี่ยนแปลงได้"

# --------- Google Sheets helpers ----------
def get_gspread_client() -> gspread.Client:
    """รับ Service Account และสร้าง gspread client"""
    info = None
    
    try:
        if SVC_JSON_B64:
            logger.info("กำลังใช้ Service Account จาก Base64...")
            decoded_bytes = base64.b64decode(SVC_JSON_B64)
            decoded_str = decoded_bytes.decode("utf-8")
            info = json.loads(decoded_str)
        elif SVC_JSON_RAW:
            logger.info("กำลังใช้ Service Account จาก JSON ดิบ...")
            info = json.loads(SVC_JSON_RAW)
        elif os.path.exists("service_account.json"):
            logger.info("กำลังใช้ Service Account จากไฟล์ service_account.json...")
            with open("service_account.json", "r", encoding="utf-8") as f:
                info = json.load(f)

        if not info:
            raise RuntimeError("❌ ไม่พบ Google Service Account JSON")

        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        for field in required_fields:
            if field not in info:
                raise RuntimeError(f"❌ ไม่พบฟิลด์ {field} ใน Service Account JSON")

        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        client = gspread.authorize(creds)
        logger.info("✅ เชื่อมต่อ Google Sheets API สำเร็จ")
        return client
        
    except Exception as e:
        logger.error(f"❌ Google Sheets connection error: {e}")
        raise RuntimeError(f"ไม่สามารถเชื่อมต่อ Google Sheets: {e}")

def get_or_create_worksheet(sh: gspread.Spreadsheet, title: str, headers: List[str] = None) -> gspread.Worksheet:
    """สร้างหรือดึง worksheet มา พร้อมตั้งค่า header"""
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"📄 สร้างชีตใหม่ '{title}'...")
        ws = sh.add_worksheet(title=title, rows=1000, cols=20)
        
        if headers:
            ws.update("A1", [headers], value_input_option="RAW")
            logger.info(f"✅ ตั้งค่า headers สำหรับชีต '{title}'")
    
    return ws

def get_existing_data(ws: gspread.Worksheet) -> Dict[str, Dict]:
    """ดึงข้อมูลที่มีอยู่ใน worksheet และจัดเก็บตาม Job No."""
    try:
        records = ws.get_all_records()
        job_data = {}
        
        for record in records:
            job_no = record.get('Job_No', '').strip()
            if job_no:
                job_data[job_no] = record
        
        logger.info(f"📊 ดึงข้อมูลที่มีอยู่: {len(job_data)} jobs")
        return job_data
    except Exception as e:
        logger.error(f"❌ ไม่สามารถดึงข้อมูลที่มีอยู่: {e}")
        return {}

def create_data_hash(data: Dict) -> str:
    """สร้าง hash สำหรับข้อมูล เพื่อตรวจสอบการเปลี่ยนแปลง"""
    # เอาข้อมูลที่สำคัญมา hash (ไม่รวม timestamp)
    relevant_data = {k: v for k, v in data.items() 
                    if k not in ['Source_Tab', 'Last_Updated', 'Data_Hash']}
    data_str = json.dumps(relevant_data, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

def update_master_data(sh: gspread.Spreadsheet, tab_data: Dict[int, pd.DataFrame]) -> Tuple[int, int, int]:
    """อัปเดตข้อมูลใน Master sheet และส่งคืนสถิติ"""
    ws = get_or_create_worksheet(sh, MASTER_SHEET_NAME, [
        'Job_No', 'Source_Tab', 'Tab_Name', 'Last_Updated', 'First_Seen', 
        'Update_Count', 'Data_Hash', 'Status', 'Priority', 'Description',
        'AI_Summary', 'Change_Log'
    ])
    
    existing_data = get_existing_data(ws)
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    
    new_jobs = 0
    updated_jobs = 0
    unchanged_jobs = 0
    
    all_records = []
    notifications = []
    
    for tab_num, df in tab_data.items():
        if df.empty:
            continue
            
        tab_name = TAB_NAMES.get(tab_num, f"Tab_{tab_num}")
        
        for _, row in df.iterrows():
            # หา Job No. column (อาจจะมีชื่อต่างกัน)
            job_no = None
            for col in df.columns:
                if 'job' in col.lower() and ('no' in col.lower() or 'number' in col.lower()):
                    job_no = str(row[col]).strip()
                    break
            
            if not job_no or job_no == 'nan' or job_no == '':
                continue
            
            # เตรียมข้อมูลใหม่
            new_record = {
                'Job_No': job_no,
                'Source_Tab': tab_num,
                'Tab_Name': tab_name,
                'Last_Updated': current_time,
            }
            
            # เพิ่มข้อมูลจาก DataFrame
            for col, val in row.items():
                if col != 'Job_No':  # ไม่ซ้ำ
                    new_record[col] = str(val) if pd.notna(val) else ''
            
            data_hash = create_data_hash(new_record)
            new_record['Data_Hash'] = data_hash
            
            if job_no in existing_data:
                # Job ที่มีอยู่แล้ว
                old_record = existing_data[job_no]
                old_hash = old_record.get('Data_Hash', '')
                
                if old_hash != data_hash:
                    # มีการเปลี่ยนแปลง
                    new_record['First_Seen'] = old_record.get('First_Seen', current_time)
                    new_record['Update_Count'] = int(old_record.get('Update_Count', 0)) + 1
                    
                    # AI Analysis
                    ai_analysis = analyze_job_changes(old_record, new_record, job_no)
                    new_record['AI_Summary'] = ai_analysis
                    new_record['Change_Log'] = f"{old_record.get('Change_Log', '')} | {current_time}: Updated from {tab_name}"
                    
                    updated_jobs += 1
                    notifications.append(f"🔄 Job {job_no} updated in {tab_name}\n{ai_analysis}")
                    logger.info(f"🔄 Job {job_no} updated")
                else:
                    # ไม่มีการเปลี่ยนแปลง แต่อัปเดต timestamp
                    new_record.update(old_record)
                    new_record['Last_Updated'] = current_time
                    unchanged_jobs += 1
            else:
                # Job ใหม่
                new_record['First_Seen'] = current_time
                new_record['Update_Count'] = 1
                new_record['AI_Summary'] = f"New job detected in {tab_name}"
                new_record['Change_Log'] = f"{current_time}: First seen in {tab_name}"
                
                new_jobs += 1
                notifications.append(f"🆕 New Job {job_no} found in {tab_name}")
                logger.info(f"🆕 New Job {job_no} found")
            
            all_records.append(new_record)
    
    # อัปเดตข้อมูลใน sheet
    if all_records:
        # เตรียม headers
        all_keys = set()
        for record in all_records:
            all_keys.update(record.keys())
        headers = sorted(all_keys)
        
        # เตรียมข้อมูล
        values = [headers]
        for record in all_records:
            row = [record.get(h, '') for h in headers]
            values.append(row)
        
        # อัปเดต sheet
        ws.clear()
        ws.resize(rows=len(values), cols=len(headers))
        ws.update("A1", values, value_input_option="RAW")
        
        logger.info(f"✅ อัปเดต Master sheet: {len(all_records)} jobs")
    
    # ส่งการแจ้งเตือน
    if notifications and LINE_NOTIFY_TOKEN:
        summary = f"📊 Job Data Update Summary:\n🆕 New: {new_jobs}\n🔄 Updated: {updated_jobs}\n⏸️ Unchanged: {unchanged_jobs}"
        send_line_notify(summary)
        
        # ส่งแจ้งเตือนแยกสำหรับ jobs สำคัญ
        important_notifications = [n for n in notifications if "🆕" in n or "🔄" in n][:5]
        for notification in important_notifications:
            send_line_notify(notification)
    
    return new_jobs, updated_jobs, unchanged_jobs

def update_summary_sheet(sh: gspread.Spreadsheet, stats: Dict[str, Any]) -> None:
    """อัปเดต summary sheet ด้วยสถิติการทำงาน"""
    ws = get_or_create_worksheet(sh, SUMMARY_SHEET_NAME, [
        'Timestamp', 'Total_Jobs', 'New_Jobs', 'Updated_Jobs', 'Unchanged_Jobs',
        'Successful_Tabs', 'Failed_Tabs', 'Processing_Time', 'Status'
    ])
    
    # เพิ่มแถวใหม่ที่ด้านบน
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    new_row = [
        current_time,
        stats.get('total_jobs', 0),
        stats.get('new_jobs', 0),
        stats.get('updated_jobs', 0),
        stats.get('unchanged_jobs', 0),
        ','.join(map(str, stats.get('successful_tabs', []))),
        ','.join(map(str, stats.get('failed_tabs', []))),
        f"{stats.get('processing_time', 0):.2f}s",
        stats.get('status', 'Unknown')
    ]
    
    ws.insert_row(new_row, 2)  # แทรกที่แถวที่ 2 (หลัง header)
    logger.info("✅ อัปเดต Summary sheet")

def log_sync_activity(sh: gspread.Spreadsheet, activity: str, details: str = "") -> None:
    """บันทึก log การทำงาน"""
    try:
        ws = get_or_create_worksheet(sh, LOG_SHEET_NAME, [
            'Timestamp', 'Activity', 'Details', 'Status'
        ])
        
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        ws.insert_row([current_time, activity, details, 'Success'], 2)
    except Exception as e:
        logger.error(f"❌ ไม่สามารถบันทึก log: {e}")

def upsert_worksheet(sh: gspread.Spreadsheet, title: str, df: pd.DataFrame) -> None:
    """สร้างหรืออัปเดต worksheet ใน Google Sheets (เก็บไว้เพื่อความเข้ากันได้)"""
    title = str(title)[:99] if title else "Sheet"
    
    try:
        ws = get_or_create_worksheet(sh, title)
        ws.clear()
        
        if df is None or df.empty:
            ws.update("A1", [["NO DATA"]])
            logger.warning(f"⚠️ ไม่มีข้อมูลในชีต '{title}'")
            return

        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        values = [list(df.columns)] + df.astype(object).where(pd.notna(df), "").values.tolist()

        rows, cols = len(values), max(len(r) for r in values) if values else 1
        ws.resize(rows=rows, cols=cols)
        ws.update("A1", values, value_input_option="RAW")
        
        logger.info(f"✅ อัปโหลดสำเร็จ: {len(df)} แถว, {len(df.columns)} คอลัมน์")
        
    except Exception as e:
        logger.error(f"❌ ไม่สามารถอัปเดตชีต '{title}': {e}")
        raise

# --------- Scrape helpers ----------
async def login_with_ui(context) -> Tuple[bool, "Page"]:
    """ล็อกอินเข้าระบบ"""
    page = await context.new_page()
    
    try:
        logger.info("🌐 เปิดหน้าล็อกอิน...")
        await page.goto(LOGIN, wait_until="domcontentloaded", timeout=30_000)

        logger.info("👤 กรอกข้อมูลล็อกอิน...")
        await page.fill('input[name="username"]', USER, timeout=10_000)
        await page.fill('input[name="password"]', PASS, timeout=10_000)

        logger.info("🔑 กำลังล็อกอิน...")
        await page.click('button[name="login__username"], input[name="login__username"]')

        await page.wait_for_load_state("networkidle", timeout=30_000)
        await page.goto(INDEX, wait_until="domcontentloaded", timeout=30_000)

        current_url = page.url.lower()
        ok = ("login" not in current_url)
        
        if ok:
            logger.info(f"✅ ล็อกอินสำเร็จ: {page.url}")
        else:
            logger.error(f"❌ ล็อกอินไม่สำเร็จ: {page.url}")
            
        return ok, page
        
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการล็อกอิน: {e}")
        return False, page

async def extract_tables_from_dom(page, tab: int) -> pd.DataFrame:
    """เปิดหน้าแท็บแล้วดึงทุก <table> บนหน้าเป็น DataFrame เดียว"""
    try:
        url = f"{INDEX}?tab={tab}"
        logger.info(f"🌐 เปิด {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

        await page.wait_for_timeout(2000)

        # ปรับ DataTables page length
        length_sel = page.locator('select[name$="_length"], select.dt-input')
        if await length_sel.count() > 0:
            try:
                logger.info("⚙️ ปรับ DataTables page length...")
                opts = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
                for v in ["-1", "1000", "500", "250", "100"]:
                    if v in opts:
                        await length_sel.first.select_option(v)
                        await page.wait_for_load_state("networkidle", timeout=10_000)
                        logger.info(f"✅ ตั้งค่าแสดง {v} แถวต่อหน้า")
                        break
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถปรับ page length: {e}")

        # รวมทุก <table> เป็น DataFrame เดียว
        tables = await page.locator("table").all()
        logger.info(f"🔍 พบตาราง {len(tables)} ตาราง")
        
        frames = []
        for i, t in enumerate(tables):
            try:
                html = await t.evaluate("(el)=>el.outerHTML")
                dfs = pd.read_html(StringIO(html))
                for df in dfs:
                    if not df.empty:
                        df.columns = [str(c).strip() for c in df.columns]
                        frames.append(df)
                        logger.info(f"   📊 ตาราง {i+1}: {len(df)} แถว, {len(df.columns)} คอลัมน์")
            except Exception as e:
                logger.warning(f"   ⚠️ ไม่สามารถดึงข้อมูลจากตาราง {i+1}: {e}")

        if frames:
            result = pd.concat(frames, ignore_index=True)
            logger.info(f"✅ รวมข้อมูลทั้งหมด: {len(result)} แถว, {len(result.columns)} คอลัมน์")
            return result
        else:
            logger.warning("⚠️ ไม่พบตารางที่มีข้อมูล")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลแท็บ {tab}: {e}")
        return pd.DataFrame()

# --------- Main ----------
async def main():
    """ฟังก์ชันหลัก"""
    start_time = datetime.now()
    logger.info("🚀 เริ่มต้นโปรแกรม Enhanced Web Scraper")
    
    # ตรวจสอบ environment variables
    if not USER or not PASS:
        logger.error("❌ กรุณาตั้งค่า EDOCLITE_USER และ EDOCLITE_PASS")
        raise RuntimeError("Missing login credentials")

    if not SHEET_ID:
        logger.error("❌ กรุณาตั้งค่า GOOGLE_SHEET_ID")
        raise RuntimeError("Missing Google Sheet ID")

    # เตรียม client ของ Google Sheets
    try:
        logger.info("🔗 เชื่อมต่อ Google Sheets...")
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID)
        logger.info(f"✅ เชื่อมต่อ Google Sheets สำเร็จ: '{sh.title}'")
        
        log_sync_activity(sh, "Start", "เริ่มต้นการซิงค์ข้อมูล")
    except Exception as e:
        logger.error(f"❌ ไม่สามารถเชื่อมต่อ Google Sheets: {e}")
        raise

    # เริ่ม Playwright
    try:
        async with async_playwright() as p:
            logger.info("🌐 เริ่มต้น Browser...")
            
            launch_options = {
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage", 
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor",
                    "--disable-extensions",
                    "--disable-plugins",
                    "--disable-images",
                    "--no-first-run",
                    "--disable-default-apps",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-backgrounding-occluded-windows"
                ]
            }
            
            browser = await p.chromium.launch(**launch_options)
            context = await browser.new_context(
                viewport={"width": 1400, "height": 2000},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                timezone_id="Asia/Bangkok",
                locale="th-TH",
                ignore_https_errors=True,
            )

            # ล็อกอิน
            logger.info("🔐 กำลังล็อกอิน...")
            ok, page = await login_with_ui(context)
            if not ok:
                logger.error("❌ ล็อกอินไม่สำเร็จ")
                if LINE_NOTIFY_TOKEN:
                    send_line_notify("❌ ล็อกอินไม่สำเร็จ - กรุณาตรวจสอบ username/password")
                await browser.close()
                raise SystemExit(1)

            logger.info("✅ ล็อกอินสำเร็จ")

            # ดึงข้อมูลจากทุกแท็บ
            successful_tabs = []
            failed_tabs = []
            tab_data = {}
            
            for tab_num in TABS:
                try:
                    logger.info(f"➡️ ดึงข้อมูลแท็บ {tab_num} ({TAB_NAMES.get(tab_num, f'Tab_{tab_num}')})")
                    df = await extract_tables_from_dom(page, tab_num)
                    
                    # บันทึกข้อมูลแท็บแยก (เพื่อความเข้ากันได้เดิม)
                    sheet_title = f"Tab{tab_num}_{TAB_NAMES.get(tab_num, 'Unknown')}"
                    upsert_worksheet(sh, sheet_title, df)
                    
                    # เก็บไว้สำหรับ master data
                    tab_data[tab_num] = df
                    
                    rows_count = len(df) if not df.empty else 0
                    logger.info(f"✅ แท็บ {tab_num} สำเร็จ ({rows_count} แถว)")
                    successful_tabs.append(tab_num)
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"❌ แท็บ {tab_num} ล้มเหลว: {e}")
                    failed_tabs.append(tab_num)
                    log_sync_activity(sh, "Tab Error", f"แท็บ {tab_num} ล้มเหลว: {str(e)}")

            await browser.close()
            
            # อัปเดต Master Data
            logger.info("📊 กำลังอัปเดต Master Data...")
            new_jobs, updated_jobs, unchanged_jobs = update_master_data(sh, tab_data)
            
            # คำนวณเวลาที่ใช้
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # เตรียมสถิติ
            stats = {
                'total_jobs': new_jobs + updated_jobs + unchanged_jobs,
                'new_jobs': new_jobs,
                'updated_jobs': updated_jobs,
                'unchanged_jobs': unchanged_jobs,
                'successful_tabs': successful_tabs,
                'failed_tabs': failed_tabs,
                'processing_time': processing_time,
                'status': 'Success' if not failed_tabs else 'Partial Success'
            }
            
            # อัปเดต Summary Sheet
            update_summary_sheet(sh, stats)
            
            # บันทึก log
            log_details = f"Jobs: {new_jobs} new, {updated_jobs} updated, {unchanged_jobs} unchanged. Tabs: {len(successful_tabs)} success, {len(failed_tabs)} failed"
            log_sync_activity(sh, "Sync Complete", log_details)
            
            # สรุปผลและส่ง LINE Notify
            logger.info("🎉 เสร็จสิ้นการทำงาน")
            logger.info(f"📊 สถิติ:")
            logger.info(f"   🆕 Jobs ใหม่: {new_jobs}")
            logger.info(f"   🔄 Jobs อัปเดต: {updated_jobs}")
            logger.info(f"   ⏸️  Jobs ไม่เปลี่ยนแปลง: {unchanged_jobs}")
            logger.info(f"   ✅ แท็บสำเร็จ: {len(successful_tabs)} {successful_tabs}")
            if failed_tabs:
                logger.warning(f"   ❌ แท็บล้มเหลว: {len(failed_tabs)} {failed_tabs}")
            logger.info(f"   ⏱️  เวลาที่ใช้: {processing_time:.2f} วินาที")
            
            # ส่งสรุปผลทาง LINE
            if LINE_NOTIFY_TOKEN:
                summary_msg = f"""
🎉 Job Sync Complete!
📊 สถิติ:
🆕 Jobs ใหม่: {new_jobs}
🔄 Jobs อัปเดต: {updated_jobs} 
⏸️ Jobs ไม่เปลี่ยนแปลง: {unchanged_jobs}
✅ แท็บสำเร็จ: {len(successful_tabs)}/{len(TABS)}
⏱️ เวลาที่ใช้: {processing_time:.1f}s

📋 Sheet: {sh.title}
🔗 Link: https://docs.google.com/spreadsheets/d/{SHEET_ID}
                """.strip()
                send_line_notify(summary_msg)
            
            return len(failed_tabs) == 0
            
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ เกิดข้อผิดพลาดร้ายแรง: {e}")
        
        # ส่งแจ้งเตือนข้อผิดพลาด
        if LINE_NOTIFY_TOKEN:
            error_msg = f"❌ Job Sync Failed!\nError: {str(e)}\nTime: {processing_time:.1f}s"
            send_line_notify(error_msg)
        
        try:
            log_sync_activity(sh, "Error", f"ข้อผิดพลาด: {str(e)}")
        except:
            pass
        
        raise

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("⏹️ ผู้ใช้หยุดโปรแกรม")
        if LINE_NOTIFY_TOKEN:
            send_line_notify("⏹️ Job Sync ถูกหยุดโดยผู้ใช้")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 โปรแกรมหยุดทำงานด้วยข้อผิดพลาด: {e}")
        if LINE_NOTIFY_TOKEN:
            send_line_notify(f"💥 Job Sync Crashed: {str(e)}")
        sys.exit(1)
