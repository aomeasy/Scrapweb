# scrape.py — เวอร์ชันแก้ล็อกอิน + ดึงแท็บ
import asyncio, json, os, re, sys
from io import StringIO
from pathlib import Path
import pandas as pd
import requests
from playwright.async_api import async_playwright

# ===== Config / Constants =====
USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN_PAGE = f"{BASE}/pages/login"
LOGIN_POST = f"{BASE}/pages/login_db"
INDEX      = f"{BASE}/pages/index"
TABS       = [13, 14, 15, 8, 7, 11]

OUT = Path("output"); OUT.mkdir(exist_ok=True)

# ฟิลเตอร์ XHR ที่ดูเหมือน endpoint ข้อมูล (สำหรับเก็บเพิ่ม ถ้ามี)
XHR_ALLOW_HOST = "jobm.edoclite.com"
URL_INCLUDE_RE = re.compile(r"(counter|api|ajax|list|data|table|report|online)\.(php|aspx|json)|/(api|ajax|data)/", re.I)


# ===== Utilities =====
def save_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

async def dump_page(name, page):
    try: save_text(OUT / f"{name}.html", await page.content())
    except: pass

# ===== 1) Requests login (แนะนำ) =====
def requests_login_and_test(user: str, pw: str) -> requests.Session | None:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": LOGIN_PAGE,
        "Origin": "https://jobm.edoclite.com",
    })
    # ส่งตามฟอร์มจริง: username, password, และปุ่ม name=login__username
    payload = {
        "username": user,
        "password": pw,
        "login__username": "ตกลง",
    }
    r = s.post(LOGIN_POST, data=payload, allow_redirects=True, timeout=30)
    save_text(OUT / "after_login_requests.html", r.text)

    # ถ้ายังเห็น 'เข้าสู่ระบบ' หรือ URL กลับไป /login?error=… แปลว่ายังไม่ผ่าน
    if ("เข้าสู่ระบบ" in r.text) or ("/login" in r.url):
        return None

    # ทดสอบเข้า index
    t = s.get(f"{INDEX}?tab=13", allow_redirects=True, timeout=30)
    save_text(OUT / "test_index_tab13_requests.html", t.text)
    if ("เข้าสู่ระบบ" in t.text) or ("/login" in t.url):
        return None
    return s

# ===== 2) Inject cookies จาก requests -> Playwright =====
async def inject_cookies_from_requests(context, sess: requests.Session):
    from urllib.parse import urlparse
    u = urlparse(BASE)
    cookies = []
    for c in sess.cookies:
        cookies.append({
            "name": c.name,
            "value": c.value,
            "domain": u.hostname,
            "path": "/",
            "httpOnly": False,
            "secure": True,
        })
    if cookies:
        await context.add_cookies(cookies)

# ===== 3) UI login (สำรองถ้า requests ไม่ผ่าน) =====
async def ui_login(page, user: str, pw: str) -> bool:
    await page.goto(LOGIN_PAGE, wait_until="domcontentloaded")

    # กรอกฟิลด์แบบเจาะจง
    await page.fill('input[name="username"]', user)
    await page.fill('input[name="password"]', pw)

    # คลิกปุ่ม submit ที่ถูกตัว
    # (บนหน้า login จริงปุ่มคือ <button name="login__username">ตกลง</button>)
    await page.click('button[name="login__username"]')
    await page.wait_for_load_state("networkidle")

    # บันทึกหลัง login
    await dump_page("after_ui_login", page)

    # ลองเข้า index เพื่อเช็ค
    await page.goto(INDEX, wait_until="networkidle")
    html = await page.content()
    await dump_page("after_ui_index", page)

    if ("เข้าสู่ระบบ" in html) or ("pages/login" in page.url):
        return False
    return True

# ===== 4) ดึงตารางจาก DOM (รวมหลายหน้าให้มากสุด) =====
async def extract_tables_from_dom(page, tab) -> pd.DataFrame:
    # พยายามเพิ่ม page length
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count() > 0:
        try:
            opts = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
            for v in ["-1","1000","500","250","100"]:
                if v in opts:
                    await length_sel.first.select_option(v)
                    await page.wait_for_load_state("networkidle")
                    break
        except: pass

    # กด next ให้สุด (กันตกหล่น)
    next_btn = page.locator('a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
    for _ in range(200):
        if await next_btn.count()==0 or not await next_btn.first.is_enabled(): break
        try:
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle")
        except:
            break

    # อ่านทุกตาราง
    tables = await page.locator("table").all()
    htmls  = [await t.evaluate("(el)=>el.outerHTML") for t in tables]
    dfs = []
    for h in htmls:
        try:
            for df in pd.read_html(StringIO(h)):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except: pass

    if dfs:
        out = pd.concat(dfs, ignore_index=True)
        out.to_csv(OUT / f"tab_{tab}.csv", index=False)
        return out

    # เก็บร่างเพื่อตรวจ
    await dump_page(f"tab_{tab}_body", page)
    return pd.DataFrame()

# ===== 5) Main =====
async def main():
    # 5.1 ล็อกอินด้วย Requests ก่อน
    sess = requests_login_and_test(USER, PASS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"]
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 2000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            timezone_id="Asia/Bangkok",
            locale="th-TH",
        )
        page = await context.new_page()

        login_ok = False

        if sess:
            # 5.2 ถ้า Requests ผ่าน → ฉีดคุกกี้แล้วทดสอบเข้า index
            await inject_cookies_from_requests(context, sess)
            await page.goto(f"{INDEX}?tab=13", wait_until="networkidle")
            html = await page.content()
            await dump_page("after_cookie_inject", page)
            login_ok = ("เข้าสู่ระบบ" not in html) and ("pages/login" not in page.url)

        if not login_ok:
            # 5.3 ถ้า Requests ไม่ผ่าน → ลอง UI login แบบเจาะจง selector
            login_ok = await ui_login(page, USER, PASS)

        print("LOGIN_STATUS:", "OK" if login_ok else "FAIL")
        if not login_ok:
            # บังคับล้ม เพื่อให้เห็น artifacts ชัด ๆ
            await browser.close()
            sys.exit(1)

        # 5.4 ดึงทุกแท็บ
        for t in TABS:
            await page.goto(f"{INDEX}?tab={t}", wait_until="networkidle")
            if "login" in page.url.lower():
                await dump_page(f"tab_{t}_redirected_to_login", page)
                print(f"TAB {t}: redirected to login")
                continue

            df = await extract_tables_from_dom(page, t)
            print(f"TAB {t} -> rows={len(df)} cols={len(df.columns)}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
