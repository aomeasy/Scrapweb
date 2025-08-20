import asyncio, json, os, re, sys
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ===================== CONFIG =====================
USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"
TABS  = [13, 14, 15, 8, 7, 11]

# login selectors (ลองหลายแบบ)
USER_CANDIDATES  = ['input[name="username"]', '#username', 'input[type="text"]', 'input[name*="user" i]']
PASS_CANDIDATES  = ['input[name="password"]', '#password', 'input[type="password"]', 'input[name*="pass" i]']
SUBMIT_CANDIDATES= ['button[type="submit"]', 'input[type="submit"]', 'button:has-text("เข้าสู่ระบบ")', 'button:has-text("Login")']

# ฟิลเตอร์ URL XHR/Fetch ที่ “น่าจะเป็นข้อมูล”
XHR_ALLOW_HOST = "jobm.edoclite.com"
URL_INCLUDE_RE = re.compile(r"(counter|api|ajax|list|data|table|report|online)\.(php|aspx|json)|/(api|ajax|data)/", re.I)

# Google Sheets (ใส่คีย์ไว้ถ้าต้องการ)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

OUT = Path("output"); OUT.mkdir(exist_ok=True)
# ==================================================

# ---------- Google Sheets ----------
def get_gspread_client():
    if not (GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON):
        return None, None
    import json as _json, tempfile, gspread
    from google.oauth2.service_account import Credentials
    data = _json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        _json.dump(data, f)
        key_path = f.name
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return gc, sh

def write_df_to_gsheet(sh, sheet_name, df: pd.DataFrame):
    if sh is None or df.empty: return
    import gspread
    try:
        ws = sh.worksheet(sheet_name); ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(len(df)+10,1000)), cols=str(max(len(df.columns)+5,26)))
    header = [str(c) for c in df.columns]
    values = [header] + df.fillna("").astype(str).values.tolist()
    ws.update("A1", values)

# ---------- Helpers ----------
async def is_logged_in(page) -> bool:
    # 1) url ไม่ใช่หน้า login
    if "login" in page.url.lower():
        return False
    # 2) มีลิงก์/พารามิเตอร์ที่บ่งชี้หน้า index
    try:
        content = await page.content()
        if "tab=" in content or "/pages/index" in content:
            return True
    except:
        pass
    return True  # ถ้าสงสัย ให้ถือว่า OK ไปก่อน

async def login_ui(page):
    await page.goto(LOGIN, wait_until="domcontentloaded")
    # เซฟหน้า login ไว้เทียบ
    (OUT / "login_page.html").write_text(await page.content(), encoding="utf-8")

    # เติม user/pass
    filled_user = False
    for sel in USER_CANDIDATES:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(USER); filled_user = True; break
        except: pass

    filled_pass = False
    for sel in PASS_CANDIDATES:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(PASS); filled_pass = True; break
        except: pass

    # ส่งฟอร์ม: กดปุ่มหรือกด Enter ที่ช่อง password
    submitted = False
    for sel in SUBMIT_CANDIDATES:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click(); submitted = True; break
        except: pass
    if not submitted and filled_pass:
        try:
            await page.keyboard.press("Enter"); submitted = True
        except: pass

    await page.wait_for_load_state("networkidle")
    # ลองเข้า index ตรง ๆ อีกรอบ
    await page.goto(INDEX, wait_until="networkidle")

    # เก็บหลักฐาน
    await page.screenshot(path=str(OUT / "after_ui_login.png"), full_page=True)
    (OUT / "after_ui_login.html").write_text(await page.content(), encoding="utf-8")

    return await is_logged_in(page)

def discover_login_form(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    form = soup.find("form")
    if not form:
        return None, None, {}
    action = form.get("action") or base_url
    method = (form.get("method") or "post").lower()
    # payload เบื้องต้นจาก hidden/input ทั้งหมด
    payload = {}
    for inp in form.find_all("input"):
        name = inp.get("name"); 
        if not name: continue
        payload[name] = inp.get("value","")
    # ใส่ user/pass ให้ลงคีย์ที่น่าจะใช่
    user_keys = ["username","user","userid","login","j_username","txtUser","userName"]
    pass_keys = ["password","pass","j_password","txtPass","pwd","userPass"]
    def put(keys, value):
        for k in keys:
            if k in payload: payload[k]=value; return
        payload[keys[0]] = value
    put(user_keys, USER); put(pass_keys, PASS)
    return action, method, payload

async def login_requests_and_inject_cookies(context):
    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0", "Referer": LOGIN})
    # 1) GET login page
    r = s.get(LOGIN, timeout=30); r.raise_for_status()
    (OUT / "login_requests.html").write_text(r.text, encoding="utf-8")

    # 2) discover form + POST
    from urllib.parse import urljoin
    action, method, payload = discover_login_form(r.text, LOGIN)
    if not action:
        return False
    action_url = urljoin(LOGIN, action)

    if method == "post":
        r2 = s.post(action_url, data=payload, timeout=30, allow_redirects=True)
    else:
        r2 = s.get(action_url, params=payload, timeout=30, allow_redirects=True)

    # 3) ตรวจสอบเข้า index ได้ไหม
    test = s.get(INDEX, timeout=30, allow_redirects=True)
    (OUT / "after_requests_login.html").write_text(test.text, encoding="utf-8")
    if "login" in test.url.lower() or "password" in test.text.lower():
        return False

    # 4) ฉีดคุกกี้เข้าสู่ browser context
    cookies = []
    from urllib.parse import urlparse
    u = urlparse(BASE)
    for c in s.cookies:
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
    return True

async def extract_tables_from_dom(page, tab) -> pd.DataFrame:
    # พยายามเพิ่ม page length เป็น All/สูงสุด แล้วค่อยอ่าน table
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count() > 0:
        try:
            options = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
            for candidate in ["-1","1000","500","250","100"]:
                if candidate in options:
                    await length_sel.first.select_option(candidate)
                    await page.wait_for_load_state("networkidle"); break
        except: pass

    # กด next จนสุด (กัน infinite loop)
    next_btn = page.locator('a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
    for _ in range(200):
        if await next_btn.count()==0 or not await next_btn.first.is_enabled(): break
        try:
            await next_btn.first.click(); await page.wait_for_load_state("networkidle")
        except: break

    # อ่านทุก table
    tables = await page.locator("table").all()
    htmls  = [await t.evaluate("(el)=>el.outerHTML") for t in tables]
    dfs=[]
    for h in htmls:
        try:
            for df in pd.read_html(StringIO(h)):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except ValueError: pass
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(OUT / f"tab_{tab}_dom.csv", index=False)
        return df
    # เก็บ body ไว้ดีบัก
    (OUT / f"tab_{tab}_body.html").write_text(await page.content(), encoding="utf-8")
    return pd.DataFrame()

async def hit_same_request(ctx, req):
    method  = req.method.upper()
    url     = req.url
    headers = {k:v for k,v in (await req.all_headers()).items() if k.lower() not in {"host","content-length"}}
    if "referer" not in {k.lower() for k in headers}:
        headers["Referer"] = req.headers.get("referer", INDEX)
    post_data = await req.post_data() if method != "GET" else None
    if method == "GET":
        return await ctx.request.get(url, headers=headers)
    else:
        ctype = req.headers.get("content-type","").lower()
        if "json" in ctype and post_data:
            try:
                return await ctx.request.post(url, headers=headers, json=json.loads(post_data))
            except Exception:
                pass
        return await ctx.request.post(url, headers=headers, data=post_data)

async def process_tab(ctx, page, tab) -> pd.DataFrame:
    url = f"{INDEX}?tab={tab}"
    hits = []

    def on_response(resp):
        try:
            rq = resp.request
            if rq.resource_type in ("xhr","fetch"):
                u = resp.url
                if XHR_ALLOW_HOST in u and URL_INCLUDE_RE.search(u):
                    hits.append((resp, rq))
        except: pass

    page.on("response", on_response)
    await page.goto(url, wait_until="networkidle")
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass

    frames=[]
    if "login" in page.url.lower():
        # ยังโดนเด้งกลับ login → เซฟ html แล้วคืนค่าว่าง
        (OUT / f"tab_{tab}_redirected_to_login.html").write_text(await page.content(), encoding="utf-8")
        return pd.DataFrame()

    if hits:
        for idx, (resp, rq) in enumerate(hits, 1):
            try:
                dup = await hit_same_request(ctx, rq)
                if not dup.ok: continue
                # JSON ก่อน
                try:
                    data = await dup.json()
                    (OUT / f"tab_{tab}_api_{idx}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                    df = pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame([{"raw": data}])
                    df.to_csv(OUT / f"tab_{tab}_api_{idx}.csv", index=False)
                    frames.append(df); continue
                except: pass
                # Text/HTML
                txt = await dup.text()
                (OUT / f"tab_{tab}_api_{idx}.txt").write_text(txt, encoding="utf-8")
                try:
                    for df in pd.read_html(StringIO(txt)):
                        frames.append(df)
                except: pass
            except Exception as e:
                (OUT / f"tab_{tab}_api_{idx}_err.txt").write_text(str(e), encoding="utf-8")

    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        df_all.to_csv(OUT / f"tab_{tab}_api_merged.csv", index=False)
        return df_all

    return await extract_tables_from_dom(page, tab)

async def main():
    gc, sh = get_gspread_client() if (GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON) else (None, None)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        ctx = await browser.new_context(viewport={"width":1400,"height":2000})
        page = await ctx.new_page()

        # ---------- 1) พยายามล็อกอินแบบ UI ----------
        ui_ok = await login_ui(page)

        # ---------- 2) ถ้า UI ไม่ผ่าน ลอง Requests + inject cookies ----------
        if not ui_ok:
            req_ok = await login_requests_and_inject_cookies(ctx)
            # ทดสอบอีกครั้ง
            await page.goto(INDEX, wait_until="networkidle")
            await page.screenshot(path=str(OUT / "after_cookie_inject.png"), full_page=True)
            (OUT / "after_cookie_inject.html").write_text(await page.content(), encoding="utf-8")
            ui_ok = await is_logged_in(page)

        print("LOGIN_STATUS:", "OK" if ui_ok else "FAIL")

        # ถ้ายัง FAIL ให้หยุดเลย (เพื่อดู debug จาก artifacts)
        if not ui_ok:
            sys.exit(1)

        # ---------- 3) ดึงข้อมูลแต่ละแท็บ ----------
        any_data = False
        for t in TABS:
            df = await process_tab(ctx, page, t)
            print(f"TAB {t} -> rows={len(df)} cols={len(df.columns)}")
            if not df.empty:
                any_data = True
                df.to_csv(OUT / f"tab_{t}.csv", index=False)
                if sh:
                    write_df_to_gsheet(sh, f"TAB_{t}", df)

        if not any_data:
            (OUT / "NO_DATA.txt").write_text("All tabs were empty or redirected to login.", encoding="utf-8")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
