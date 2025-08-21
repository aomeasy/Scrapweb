# scrape.py
import os, json, base64, asyncio
from io import StringIO
from typing import List, Tuple
import pandas as pd

from playwright.async_api import async_playwright

# ---- Google Sheets (gspread) ----
import gspread
from google.oauth2.service_account import Credentials

# --------- Config ----------
BASE  = "https://jobm.edoclite.com/jobManagement"
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
    if SVC_JSON_B64:
        info = json.loads(base64.b64decode(SVC_JSON_B64).decode("utf-8"))
    elif SVC_JSON_RAW:
        info = json.loads(SVC_JSON_RAW)
    elif os.path.exists("service_account.json"):
        with open("service_account.json", "r", encoding="utf-8") as f:
            info = json.load(f)

    if not info:
        raise RuntimeError("No Google Service Account JSON found. Provide GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_JSON_B64.")

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def upsert_worksheet(sh: gspread.Spreadsheet, title: str, df: pd.DataFrame) -> None:
    # ชื่อชีตต้องยาวไม่เกิน 100 ตัวอักษร และห้ามซ้ำ
    title = str(title)[:99] if title else "Sheet"
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="1", cols="1")

    # เคลียร์แล้วอัปโหลดตารางใหม่
    ws.clear()
    if df is None or df.empty:
        ws.update("A1", [["NO DATA"]])
        return

    # เตรียม values เป็น list of lists
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    values = [list(df.columns)] + df.astype(object).where(pd.notna(df), "").values.tolist()

    # resize แล้วอัปเดต
    ws.resize(rows=len(values), cols=max(len(r) for r in values))
    ws.update("A1", values, value_input_option="RAW")

# --------- Scrape helpers ----------
async def login_with_ui(context) -> Tuple[bool, "Page"]:
    """
    กรอก user/pass ในฟอร์มหน้า LOGIN แล้วกด 'ตกลง'
    จากนั้นลองเปิดหน้าหลักเพื่อเช็คว่าไม่ถูกเด้งกลับหน้า login
    """
    page = await context.new_page()
    await page.goto(LOGIN, wait_until="domcontentloaded")

    # กรอก username/password โดยอิง name ของ input ตาม DOM ที่ให้มาจากหน้า
    await page.fill('input[name="username"]', USER, timeout=10_000)
    await page.fill('input[name="password"]', PASS, timeout=10_000)

    # คลิกปุ่ม submit ที่มี name="login__username"
    await page.click('button[name="login__username"], input[name="login__username"]')

    # รอให้ network เงียบ แล้วเปิดหน้า index เพื่อตรวจสอบสถานะ
    await page.wait_for_load_state("networkidle")
    await page.goto(INDEX, wait_until="domcontentloaded")

    # ถ้า URL ยังมีคำว่า login แสดงว่ายังไม่ผ่าน
    ok = ("login" not in page.url.lower())
    return ok, page

async def extract_tables_from_dom(page, tab: int) -> pd.DataFrame:
    """เปิดหน้าแท็บแล้วดึงทุก <table> บนหน้าเป็น DataFrame เดียว"""
    await page.goto(f"{INDEX}?tab={tab}", wait_until="domcontentloaded")

    # พยายามปรับ page length ของ DataTables ให้แสดงทั้งหมด (ถ้ามี)
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count() > 0:
        try:
            opts = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
            for v in ["-1", "1000", "500", "250", "100"]:
                if v in opts:
                    await length_sel.first.select_option(v)
                    await page.wait_for_load_state("networkidle")
                    break
        except:
            pass

    # รวมทุก <table> เป็น DataFrame เดียว
    tables = await page.locator("table").all()
    frames = []
    for t in tables:
        try:
            html = await t.evaluate("(el)=>el.outerHTML")
            for df in pd.read_html(StringIO(html)):
                # ทำความสะอาดหัวตารางเล็กน้อย
                df.columns = [str(c).strip() for c in df.columns]
                frames.append(df)
        except:
            pass

    if frames:
        return pd.concat(frames, ignore_index=True)

    # กรณีไม่พบตาราง – ส่ง DataFrame ว่างกลับไป
    return pd.DataFrame()

# --------- Main ----------
async def main():
    if not USER or not PASS:
        raise RuntimeError("Please set EDOCLITE_USER and EDOCLITE_PASS.")

    if not SHEET_ID:
        raise RuntimeError("Please set GOOGLE_SHEET_ID (the target Google Sheet ID).")

    # เตรียม client ของ Google Sheets
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 2000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            timezone_id="Asia/Bangkok",
            locale="th-TH",
        )

        print("🔐 กำลังล็อกอิน…")
        ok, page = await login_with_ui(context)
        if not ok:
            print("❌ LOGIN_STATUS: FAIL")
            await browser.close()
            raise SystemExit(1)

        print("✅ LOGIN_STATUS: OK")

        # ดึงและอัปเดต Google Sheet ตามหมายเลขแท็บ
        for t in TABS:
            print(f"➡️  ดึงแท็บ {t} …", end="", flush=True)
            df = await extract_tables_from_dom(page, t)
            upsert_worksheet(sh, f"Tab{t}", df)
            print(f" ส่งขึ้นชีตแล้ว (rows={len(df) if not df.empty else 0})")

        await browser.close()
        print("🎉 เสร็จสิ้น")

if __name__ == "__main__":
    asyncio.run(main())
