import asyncio
import json
import os
import re
from io import StringIO
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# ===================== CONFIG =====================
USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"

# แท็บที่ต้องดึง
TABS  = [13, 14, 15, 8, 7, 11]

# selector หน้า login (ถ้าไม่ตรงค่าย ให้แก้ 3 ตัวนี้)
USER_SEL    = 'input[name="username"], #username'
PASS_SEL    = 'input[name="password"], #password'
SUBMIT_SEL  = 'button[type="submit"], input[type="submit"]'

# ฟิลเตอร์ URL XHR/Fetch ที่ “น่าจะเป็นข้อมูล”
XHR_ALLOW_HOST = "jobm.edoclite.com"
URL_INCLUDE_RE = re.compile(r"(counter|api|ajax|list|data|table|report|online)\.(php|aspx|json)|/(api|ajax|data)/", re.I)

# Google Sheets (ถ้าให้ค่า จะเขียนขึ้นชีตอัตโนมัติ)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# โฟลเดอร์เก็บผล/ดีบัก
OUT = Path("output")
OUT.mkdir(exist_ok=True)
# ==================================================


# ---------- Google Sheets Helpers ----------
def get_gspread_client():
    if not (GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON):
        return None, None
    import json as _json, tempfile, gspread
    from google.oauth2.service_account import Credentials
    data = _json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as f:
        _json.dump(data, f)
        key_path = f.name
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return gc, sh

def write_df_to_gsheet(sh, sheet_name, df: pd.DataFrame):
    if sh is None:
        return
    import gspread
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=sheet_name,
            rows=str(max(len(df) + 10, 1000)),
            cols=str(max(len(df.columns) + 5, 26))
        )
    header = [str(c) for c in df.columns]
    values = [header] + df.fillna("").astype(str).values.tolist()
    ws.update("A1", values)


# ---------- Scrape Helpers ----------
async def login(page):
    await page.goto(LOGIN, wait_until="domcontentloaded")
    await page.locator(USER_SEL).first.fill(USER)
    await page.locator(PASS_SEL).first.fill(PASS)
    await page.locator(SUBMIT_SEL).first.click()
    # รอให้โหลดเสร็จ และทดสอบเข้า index ได้
    await page.wait_for_load_state("networkidle")
    await page.goto(f"{BASE}/pages/index", wait_until="networkidle")

async def extract_tables_from_dom(page, tab) -> pd.DataFrame:
    """พยายามดึงทุกแถวจาก DataTables: เปลี่ยน page length และกด next จนครบ จากนั้นอ่าน <table>"""
    # 1) พยายามคลิกเปลี่ยน page length ให้มากที่สุด
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count() > 0:
        try:
            # พยายามเลือก "All" หรือจำนวนมาก ๆ
            options = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
            # ลอง "1000", "All", "9999" ตามลำดับ
            for candidate in ["-1", "1000", "500", "250", "100"]:
                if candidate in options:
                    await length_sel.first.select_option(candidate)
                    await page.wait_for_load_state("networkidle")
                    break
        except:
            pass

    # 2) กด next จนสุด (ถ้ามี paginate)
    next_btn = page.locator('a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
    # loop จำกัดรอบกัน infinite (เผื่อเว็บ bug)
    for _ in range(200):
        if await next_btn.count() == 0 or not await next_btn.first.is_enabled():
            break
        try:
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle")
        except:
            break

    # 3) อ่านทุก table
    tables = await page.locator("table").all()
    htmls = [await t.evaluate("(el)=>el.outerHTML") for t in tables]
    dfs = []
    for h in htmls:
        try:
            for df in pd.read_html(StringIO(h)):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except ValueError:
            pass

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(OUT / f"tab_{tab}_dom.csv", index=False)
        return df

    # 4) ถ้าไม่พบตารางเลย เก็บ body text ไว้ดีบัก
    body = await page.locator("body").inner_text()
    (OUT / f"tab_{tab}_body.txt").write_text(body, encoding="utf-8")
    return pd.DataFrame()

async def hit_same_request(ctx, req):
    """ยิงซ้ำ request (GET/POST) ด้วย session/context เดิม เพื่อดึง payload เต็ม"""
    method  = req.method.upper()
    url     = req.url
    headers = {k: v for k, v in (await req.all_headers()).items() if k.lower() not in {"host", "content-length"}}
    if "referer" not in {k.lower() for k in headers}:
        headers["Referer"] = req.headers.get("referer", f"{BASE}/pages/index")
    post_data = await req.post_data() if method != "GET" else None

    if method == "GET":
        return await ctx.request.get(url, headers=headers)
    else:
        ctype = req.headers.get("content-type", "").lower()
        if "json" in ctype and post_data:
            try:
                return await ctx.request.post(url, headers=headers, data=None, json=json.loads(post_data))
            except Exception:
                pass
        return await ctx.request.post(url, headers=headers, data=post_data)

async def process_tab(ctx, page, tab) -> pd.DataFrame:
    """เปิดแท็บ → ฟัง XHR/Fetch → พยายามดึง JSON/HTML จาก endpoint ที่ยิงจริง; ถ้าไม่ได้ค่อย fallback DOM"""
    url = f"{BASE}/pages/index?tab={tab}"
    hits = []

    def on_response(resp):
        try:
            rq = resp.request
            if rq.resource_type in ("xhr", "fetch"):
                u = resp.url
                if XHR_ALLOW_HOST in u and URL_INCLUDE_RE.search(u):
                    hits.append((resp, rq))
        except:
            pass

    page.on("response", on_response)
    await page.goto(url, wait_until="networkidle")
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass

    # ถ้ามี XHR ที่เข้าข่าย → ยิงซ้ำและแปลงเป็น DataFrame
    frames = []
    if hits:
        for idx, (resp, rq) in enumerate(hits, 1):
            try:
                dup = await hit_same_request(ctx, rq)
                if not dup.ok:
                    continue
                # 1) ลอง parse JSON ก่อน
                try:
                    data = await dup.json()
                    # เก็บ raw JSON ไว้ตรวจ
                    (OUT / f"tab_{tab}_api_{idx}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                    df = pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame([{"raw": data}])
                    df.to_csv(OUT / f"tab_{tab}_api_{idx}.csv", index=False)
                    frames.append(df)
                    continue
                except Exception:
                    pass

                # 2) ถ้าไม่ใช่ JSON → เก็บเป็น text + พยายามอ่าน table
                txt = await dup.text()
                (OUT / f"tab_{tab}_api_{idx}.txt").write_text(txt, encoding="utf-8")
                try:
                    for df in pd.read_html(StringIO(txt)):
                        frames.append(df)
                except:
                    pass
            except Exception as e:
                (OUT / f"tab_{tab}_api_{idx}_err.txt").write_text(str(e), encoding="utf-8")

    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        df_all.to_csv(OUT / f"tab_{tab}_api_merged.csv", index=False)
        return df_all

    # ไม่เจอ/ยิง API ไม่ผ่าน → fallback DOM (พร้อมพยายามโหลดทุกหน้า)
    return await extract_tables_from_dom(page, tab)

async def main():
    # เตรียม Google Sheet (ถ้ามี secret)
    gc, sh = get_gspread_client() if (GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON) else (None, None)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        ctx = await browser.new_context(viewport={"width": 1400, "height": 2000})
        page = await ctx.new_page()

        # 1) Login
        await login(page)

        # 2) วนทุกแท็บ → ดึงข้อมูล + บันทึกผล
        for t in TABS:
            df = await process_tab(ctx, page, t)
            print(f"TAB {t} -> rows={len(df)} cols={len(df.columns)}")
            # Save สำรอง
            if not df.empty:
                df.to_csv(OUT / f"tab_{t}.csv", index=False)
            # เขียนขึ้น Google Sheet (ถ้ามี)
            if sh and not df.empty:
                write_df_to_gsheet(sh, f"TAB_{t}", df)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
