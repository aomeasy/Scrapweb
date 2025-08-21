# scrape.py
import os, sys, json, asyncio
from io import StringIO
from pathlib import Path

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

# ---------- CONFIG ----------
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"
TABS  = [13, 14, 15, 8, 7, 11]          # ลำดับที่ต้องการดึง
TAB_SHEET_NAME = lambda t: f"TAB_{t}"   # ชื่อชีตต่อแท็บ

USER = os.getenv("EDOCLITE_USER", "").strip()
PASS = os.getenv("EDOCLITE_PASS", "").strip()

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SVC_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

OUT = Path("output"); OUT.mkdir(exist_ok=True)  # ใช้เก็บ HTML debug (หากอยากเปิดดู)

# ---------- Google Sheets ----------
def get_gspread_client():
    if not SVC_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is empty.")
    info = json.loads(SVC_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

def upsert_worksheet(spreadsheet, title, rows, cols):
    try:
        ws = spreadsheet.worksheet(title)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

def df_to_values(df: pd.DataFrame):
    if df.empty:
        return [["NO DATA"]]
    # ทำให้หัวคอลัมน์เป็น str
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
    return values

def write_df_to_sheet(spreadsheet, sheet_title, df: pd.DataFrame):
    values = df_to_values(df)
    # กันกรณีคอลัมน์เยอะ
    max_cols = max(len(r) for r in values)
    ws = upsert_worksheet(spreadsheet, sheet_title, rows=max(1000, len(values)+10), cols=max(20, max_cols+2))
    ws.clear()
    ws.update("A1", values, value_input_option="RAW")

# ---------- Playwright helpers ----------
async def dump_debug(name, page):
    # เก็บ HTML/Screenshot เผื่อ debug
    try:
        OUT.mkdir(exist_ok=True)
        (OUT/f"{name}.html").write_text(await page.content(), encoding="utf-8")
    except: pass
    try:
        await page.screenshot(path=str(OUT/f"{name}.png"), full_page=True)
    except: pass

async def try_login(page):
    # กรอกฟอร์ม login (ใช้ name="username", name="password", ปุ่ม name="login__username")
    await page.goto(LOGIN, wait_until="domcontentloaded")
    await dump_debug("login_page", page)

    # เผื่อหน้าเลือกแท็บ "พนักงาน/OS" — ค่าเริ่มต้นคือพนักงานอยู่แล้ว
    # กรอก
    await page.fill('input[name="username"]', USER, timeout=5000)
    await page.fill('input[name="password"]', PASS, timeout=5000)

    # คลิกปุ่ม "ตกลง"
    # ปุ่มหน้าเว็บใช้ <button type="submit" name="login__username">ตกลง</button>
    await page.click('button[name="login__username"]', timeout=5000)

    # รอโหลด แล้วลองเปิดหน้าหลัก
    await page.wait_for_load_state("networkidle")
    await page.goto(INDEX, wait_until="domcontentloaded")
    await dump_debug("after_login_landing", page)

    # ตรวจสอบยังอยู่หน้า login หรือไม่
    if "login" in page.url.lower():
        return False
    return True

async def show_all_rows_if_datatables(page):
    """ถ้าตารางเป็น DataTables พยายามเลือกแสดง All/-1/1000…"""
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count() > 0:
        try:
            opts = await length_sel.first.evaluate(
                "(el)=>Array.from(el.options).map(o=>o.value)"
            )
            for v in ["-1","1000","500","250","100"]:
                if v in opts:
                    await length_sel.first.select_option(v)
                    await page.wait_for_load_state("networkidle")
                    break
        except:
            pass

    # เผื่อยังมี pagination ให้กด next สุด
    next_btn = page.locator('a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
    for _ in range(200):
        if await next_btn.count()==0 or not await next_btn.first.is_enabled():
            break
        try:
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle")
        except:
            break

async def extract_tables_from_dom(page) -> pd.DataFrame:
    await show_all_rows_if_datatables(page)
    tables = await page.locator("table").all()
    dfs = []
    for t in tables:
        try:
            html = await t.evaluate("(el)=>el.outerHTML")
            for df in pd.read_html(StringIO(html)):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except:
            pass
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

async def fetch_tab_to_df(page, tab: int) -> pd.DataFrame:
    url = f"{INDEX}?tab={tab}"
    await page.goto(url, wait_until="domcontentloaded")
    # ถ้าเด้งกลับ login แสดงว่ายังไม่ล็อกอิน
    if "login" in page.url.lower():
        await dump_debug(f"tab_{tab}_redirected_login", page)
        return pd.DataFrame()
    await page.wait_for_load_state("networkidle")
    await dump_debug(f"tab_{tab}", page)
    return await extract_tables_from_dom(page)

# ---------- MAIN ----------
async def main():
    if not USER or not PASS:
        print("❌ EDOCLITE_USER / EDOCLITE_PASS ไม่ถูกตั้งค่า", file=sys.stderr)
        sys.exit(1)
    if not SHEET_ID:
        print("❌ GOOGLE_SHEET_ID ไม่ถูกตั้งค่า", file=sys.stderr)
        sys.exit(1)

    # เตรียม Google Sheets
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    print("🚀 เริ่มทำงาน…")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 2000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            java_script_enabled=True,
            timezone_id="Asia/Bangkok",
            locale="th-TH",
        )
        # ตัด image/font ออก เพื่อให้เร็วขึ้น
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image","font"} else route.continue_())

        page = await context.new_page()

        print("🔐 ล็อกอิน…")
        ok = await try_login(page)
        if not ok:
            print("❌ LOGIN_STATUS: FAIL", file=sys.stderr)
            await browser.close()
            sys.exit(1)
        print("✅ LOGIN_STATUS: OK")

        # ไล่ดึงทีละแท็บ ลงชีตตามชื่อ
        for t in TABS:
            print(f"📄 TAB {t} …")
            df = await fetch_tab_to_df(page, t)
            title = TAB_SHEET_NAME(t)
            write_df_to_sheet(sh, title, df)
            print(f"   ↳ rows={len(df)} cols={len(df.columns)} -> อัปเดตชีต '{title}' สำเร็จ")

        await browser.close()
    print("🎉 เสร็จแล้ว")

if __name__ == "__main__":
    asyncio.run(main())
