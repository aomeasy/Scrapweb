import os, json, base64, asyncio
import sys
from io import StringIO
from typing import List, Tuple, Optional
import pandas as pd

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

USER = os.getenv("EDOCLITE_USER", "").strip()
PASS = os.getenv("EDOCLITE_PASS", "").strip()

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SVC_JSON_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SVC_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# --------- Google Sheets helpers ----------
def get_gspread_client() -> gspread.Client:
    """
    รับ Service Account จาก:
    - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64)
    - หรือ GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON ทั้งก้อน)
    - หรือไฟล์ service_account.json (เผื่อไว้เวลารัน local)
    """
    info = None
    
    try:
        if SVC_JSON_B64:
            logger.info("กำลังใช้ Service Account จาก Base64...")
            logger.info(f"Base64 length: {len(SVC_JSON_B64)}")
            
            # ทดสอบ decode base64
            try:
                decoded_bytes = base64.b64decode(SVC_JSON_B64)
                logger.info(f"Decoded bytes length: {len(decoded_bytes)}")
                decoded_str = decoded_bytes.decode("utf-8")
                logger.info("✅ Base64 decode สำเร็จ")
                info = json.loads(decoded_str)
                logger.info("✅ JSON parse สำเร็จ")
            except Exception as decode_error:
                logger.error(f"❌ Base64 decode error: {decode_error}")
                raise RuntimeError(f"Base64 decode failed: {decode_error}")
                
        elif SVC_JSON_RAW:
            logger.info("กำลังใช้ Service Account จาก JSON ดิบ...")
            info = json.loads(SVC_JSON_RAW)
        elif os.path.exists("service_account.json"):
            logger.info("กำลังใช้ Service Account จากไฟล์ service_account.json...")
            with open("service_account.json", "r", encoding="utf-8") as f:
                info = json.load(f)

        if not info:
            raise RuntimeError(
                "❌ ไม่พบ Google Service Account JSON\n"
                "กรุณาตั้งค่า GOOGLE_SERVICE_ACCOUNT_JSON หรือ GOOGLE_SERVICE_ACCOUNT_JSON_B64"
            )

        # ตรวจสอบข้อมูลสำคัญ
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        for field in required_fields:
            if field not in info:
                raise RuntimeError(f"❌ ไม่พบฟิลด์ {field} ใน Service Account JSON")
        
        logger.info(f"📧 Service Account Email: {info['client_email']}")
        logger.info(f"🆔 Project ID: {info['project_id']}")

        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        client = gspread.authorize(creds)
        logger.info("✅ เชื่อมต่อ Google Sheets API สำเร็จ")
        return client
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error: {e}")
        raise RuntimeError(f"Service Account JSON ไม่ถูกต้อง: {e}")
    except Exception as e:
        logger.error(f"❌ Google Sheets connection error: {e}")
        raise RuntimeError(f"ไม่สามารถเชื่อมต่อ Google Sheets: {e}")

def upsert_worksheet(sh: gspread.Spreadsheet, title: str, df: pd.DataFrame) -> None:
    """สร้างหรืออัปเดต worksheet ใน Google Sheets"""
    # ชื่อชีตต้องยาวไม่เกิน 100 ตัวอักษร และห้ามซ้ำ
    title = str(title)[:99] if title else "Sheet"
    
    try:
        try:
            ws = sh.worksheet(title)
            logger.info(f"   📝 พบชีต '{title}' แล้ว กำลังอัปเดต...")
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"   📄 สร้างชีตใหม่ '{title}'...")
            ws = sh.add_worksheet(title=title, rows="1", cols="1")

        # เคลียร์แล้วอัปโหลดตารางใหม่
        ws.clear()
        
        if df is None or df.empty:
            ws.update("A1", [["NO DATA"]])
            logger.warning(f"   ⚠️  ไม่มีข้อมูลในชีต '{title}'")
            return

        # เตรียม values เป็น list of lists
        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        values = [list(df.columns)] + df.astype(object).where(pd.notna(df), "").values.tolist()

        # resize แล้วอัปเดต
        rows, cols = len(values), max(len(r) for r in values) if values else 1
        ws.resize(rows=rows, cols=cols)
        ws.update("A1", values, value_input_option="RAW")
        
        logger.info(f"   ✅ อัปโหลดสำเร็จ: {len(df)} แถว, {len(df.columns)} คอลัมน์")
        
    except Exception as e:
        logger.error(f"   ❌ ไม่สามารถอัปเดตชีต '{title}': {e}")
        raise

# --------- Scrape helpers ----------
async def login_with_ui(context) -> Tuple[bool, "Page"]:
    """
    กรอก user/pass ในฟอร์มหน้า LOGIN แล้วกด 'ตกลง'
    จากนั้นลองเปิดหน้าหลักเพื่อเช็คว่าไม่ถูกเด้งกลับหน้า login
    """
    page = await context.new_page()
    
    try:
        logger.info("🌐 เปิดหน้าล็อกอิน...")
        await page.goto(LOGIN, wait_until="domcontentloaded", timeout=30_000)

        # กรอก username/password โดยอิง name ของ input ตาม DOM ที่ให้มาจากหน้า
        logger.info("👤 กรอกข้อมูลล็อกอิน...")
        await page.fill('input[name="username"]', USER, timeout=10_000)
        await page.fill('input[name="password"]', PASS, timeout=10_000)

        # คลิกปุ่ม submit ที่มี name="login__username"
        logger.info("🔑 กำลังล็อกอิน...")
        await page.click('button[name="login__username"], input[name="login__username"]')

        # รอให้ network เงียบ แล้วเปิดหน้า index เพื่อตรวจสอบสถานะ
        await page.wait_for_load_state("networkidle", timeout=30_000)
        await page.goto(INDEX, wait_until="domcontentloaded", timeout=30_000)

        # ถ้า URL ยังมีคำว่า login แสดงว่ายังไม่ผ่าน
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

        # รอให้หน้าโหลดเสร็จสมบูรณ์
        await page.wait_for_timeout(2000)

        # พยายามปรับ page length ของ DataTables ให้แสดงทั้งหมด (ถ้ามี)
        length_sel = page.locator('select[name$="_length"], select.dt-input')
        if await length_sel.count() > 0:
            try:
                logger.info("⚙️  ปรับ DataTables page length...")
                opts = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
                for v in ["-1", "1000", "500", "250", "100"]:
                    if v in opts:
                        await length_sel.first.select_option(v)
                        await page.wait_for_load_state("networkidle", timeout=10_000)
                        logger.info(f"✅ ตั้งค่าแสดง {v} แถวต่อหน้า")
                        break
            except Exception as e:
                logger.warning(f"⚠️  ไม่สามารถปรับ page length: {e}")

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
                        # ทำความสะอาดหัวตารางเล็กน้อย
                        df.columns = [str(c).strip() for c in df.columns]
                        frames.append(df)
                        logger.info(f"   📊 ตาราง {i+1}: {len(df)} แถว, {len(df.columns)} คอลัมน์")
            except Exception as e:
                logger.warning(f"   ⚠️  ไม่สามารถดึงข้อมูลจากตาราง {i+1}: {e}")

        if frames:
            result = pd.concat(frames, ignore_index=True)
            logger.info(f"✅ รวมข้อมูลทั้งหมด: {len(result)} แถว, {len(result.columns)} คอลัมน์")
            return result
        else:
            logger.warning("⚠️  ไม่พบตารางที่มีข้อมูล")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลแท็บ {tab}: {e}")
        return pd.DataFrame()

# --------- Main ----------
async def main():
    """ฟังก์ชันหลัก"""
    logger.info("🚀 เริ่มต้นโปรแกรม Web Scraper")
    
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
    except Exception as e:
        logger.error(f"❌ ไม่สามารถเชื่อมต่อ Google Sheets: {e}")
        raise

    # เริ่ม Playwright
    try:
        async with async_playwright() as p:
            logger.info("🌐 เริ่มต้น Browser...")
            
            # ปรับ launch options สำหรับ CI/CD environment
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
                await browser.close()
                raise SystemExit(1)

            logger.info("✅ ล็อกอินสำเร็จ")

            # ดึงและอัปเดต Google Sheet ตามหมายเลขแท็บ
            successful_tabs = []
            failed_tabs = []
            
            for tab_num in TABS:
                try:
                    logger.info(f"➡️  ดึงข้อมูลแท็บ {tab_num}...")
                    df = await extract_tables_from_dom(page, tab_num)
                    
                    sheet_title = f"Tab{tab_num}"
                    upsert_worksheet(sh, sheet_title, df)
                    
                    rows_count = len(df) if not df.empty else 0
                    logger.info(f"✅ แท็บ {tab_num} สำเร็จ ({rows_count} แถว)")
                    successful_tabs.append(tab_num)
                    
                    # รอระหว่างแท็บเพื่อไม่ให้โหลดเซิร์ฟเวอร์หนัก
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"❌ แท็บ {tab_num} ล้มเหลว: {e}")
                    failed_tabs.append(tab_num)

            await browser.close()
            
            # สรุปผล
            logger.info("🎉 เสร็จสิ้นการทำงาน")
            logger.info(f"✅ สำเร็จ: {len(successful_tabs)} แท็บ {successful_tabs}")
            if failed_tabs:
                logger.warning(f"❌ ล้มเหลว: {len(failed_tabs)} แท็บ {failed_tabs}")
            
            return len(failed_tabs) == 0  # return True ถ้าทุกแท็บสำเร็จ
            
    except Exception as e:
        logger.error(f"❌ เกิดข้อผิดพลาดร้ายแรง: {e}")
        raise

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("⏹️  ผู้ใช้หยุดโปรแกรม")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 โปรแกรมหยุดทำงานด้วยข้อผิดพลาด: {e}")
        sys.exit(1)
