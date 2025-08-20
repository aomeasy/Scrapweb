import asyncio, json, os, re, sys
from io import StringIO
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright

USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"
TABS  = [13,14,15,8,7,11]

OUT = Path("output"); OUT.mkdir(exist_ok=True)

# ตัวกรอง XHR ที่น่าจะเป็นข้อมูล (ไว้ใช้ตอนดึงแท็บ)
XHR_ALLOW_HOST = "jobm.edoclite.com"
URL_INCLUDE_RE = re.compile(r"(counter|api|ajax|list|data|table|report|online)\.(php|aspx|json)|/(api|ajax|data)/", re.I)

# ——— Utils ———
async def dump_page(name, page):
    try:
        (OUT/f"{name}.html").write_text(await page.content(), encoding="utf-8")
    except: pass
    try:
        await page.screenshot(path=str(OUT/f"{name}.png"), full_page=True)
    except: pass

async def dump_forms_and_iframes(page, prefix):
    # forms ในหน้า/เฟรม
    try:
        forms = await page.evaluate("""
() => Array.from(document.forms).map(f => ({
  action: f.action || null,
  method: (f.method || 'GET').toUpperCase(),
  inputs: Array.from(f.querySelectorAll('input,button,select,textarea')).map(i => ({
    tag: i.tagName.toLowerCase(),
    type: i.type || null,
    name: i.name || null,
    id: i.id || null,
    value: i.type==='password' ? '***' : (i.value || null)
  }))
}))
""")
        (OUT/f"{prefix}_forms.json").write_text(json.dumps(forms, ensure_ascii=False, indent=2), encoding="utf-8")
    except: pass

    # รายชื่อ iframe + เก็บ HTML ของแต่ละเฟรม
    frames_info = []
    for i, fr in enumerate(page.frames):
        try:
            frames_info.append({"name": fr.name, "url": fr.url})
            html = await fr.content()
            (OUT/f"{prefix}_frame_{i}.html").write_text(html, encoding="utf-8")
        except: pass
    (OUT/f"{prefix}_frames.json").write_text(json.dumps(frames_info, ensure_ascii=False, indent=2), encoding="utf-8")

def looks_logged_in(page_url, html_text: str) -> bool:
    u = page_url.lower()
    if "login" in u: 
        return False
    # ถ้าเนื้อหาเจอ /pages/index หรือคำว่า tab=
    if html_text and ("pages/index" in html_text or "tab=" in html_text):
        return True
    return True  # เดาว่าผ่านถ้าไม่พบตัวชี้ชัดว่า login

# ——— Login ———
USER_SEL_CAND = ['input[name="username"]','#username','input[name*="user" i]','input[type="text"]']
PASS_SEL_CAND = ['input[name="password"]','#password','input[name*="pass" i]','input[type="password"]']
SUBMIT_CAND   = ['button[type="submit"]','input[type="submit"]','button:has-text("เข้าสู่ระบบ")','button:has-text("Login")','button:has-text("Sign in")']

async def try_fill_and_submit(page):
    filled_u = False
    for sel in USER_SEL_CAND:
        loc = page.locator(sel).first
        if await loc.count()>0:
            try: await loc.fill(USER); filled_u=True; break
            except: pass

    filled_p = False
    for sel in PASS_SEL_CAND:
        loc = page.locator(sel).first
        if await loc.count()>0:
            try: await loc.fill(PASS); filled_p=True; break
            except: pass

    submitted = False
    for sel in SUBMIT_CAND:
        loc = page.locator(sel).first
        if await loc.count()>0:
            try: await loc.click(); submitted=True; break
            except: pass
    if not submitted and filled_p:
        try:
            await page.keyboard.press("Enter"); submitted=True
        except: pass

async def login_with_ui(context):
    page = await context.new_page()

    # บันทึก request/response ระหว่าง login
    req_log = []
    async def on_request(req):
        try:
            if req.method in ("POST","PUT","PATCH"):
                # เก็บ **เมตะ** (ไม่บันทึกรหัสผ่าน)
                body = await req.post_data() if req.method!="GET" else None
                req_log.append({"method": req.method, "url": req.url, "headers": await req.all_headers(), "body_len": len(body or "")})
        except: pass
    async def on_response(resp):
        try:
            if resp.request.method in ("POST","PUT","PATCH"):
                req = resp.request
                req_log.append({"RESPONSE_OF": req.url, "status": resp.status})
        except: pass
    page.on("request", on_request)
    page.on("response", on_response)

    await page.goto(LOGIN, wait_until="domcontentloaded")
    await dump_page("login_page", page)
    await dump_forms_and_iframes(page, "login_page")

    # ถ้าอยู่ใน iframe ให้ไล่ทำในทุกเฟรม
    candidates = [page] + [fr for fr in page.frames if fr != page.main_frame]
    for target in candidates:
        try:
            await try_fill_and_submit(target)
        except: pass

    await page.wait_for_load_state("networkidle")
    # เปิดหน้าหลักเพื่อยืนยัน
    await page.goto(INDEX, wait_until="networkidle")
    html = await page.content()
    await dump_page("after_ui_login", page)

    # เซฟ log network login
    (OUT/"login_requests_log.json").write_text(json.dumps(req_log, ensure_ascii=False, indent=2), encoding="utf-8")
    await dump_forms_and_iframes(page, "after_ui_login")

    ok = looks_logged_in(page.url, html)
    return ok, page

# ——— ดึงแท็บ (จะทำภายหลังเมื่อ login ผ่าน) ———
async def extract_tables_from_dom(page, tab) -> pd.DataFrame:
    # ปรับ page length
    length_sel = page.locator('select[name$="_length"], select.dt-input')
    if await length_sel.count()>0:
        try:
            opts = await length_sel.first.evaluate("(el)=>Array.from(el.options).map(o=>o.value)")
            for v in ["-1","1000","500","250","100"]:
                if v in opts:
                    await length_sel.first.select_option(v); 
                    await page.wait_for_load_state("networkidle"); 
                    break
        except: pass
    # กด next ให้สุด
    next_btn = page.locator('a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
    for _ in range(200):
        if await next_btn.count()==0 or not await next_btn.first.is_enabled(): break
        try: 
            await next_btn.first.click(); 
            await page.wait_for_load_state("networkidle")
        except: break

    # อ่านตาราง
    tables = await page.locator("table").all()
    htmls = [await t.evaluate("(el)=>el.outerHTML") for t in tables]
    dfs=[]
    for h in htmls:
        try:
            for df in pd.read_html(StringIO(h)):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except: pass
    if dfs:
        out = pd.concat(dfs, ignore_index=True)
        out.to_csv(OUT/f"tab_{tab}.csv", index=False)
        return out
    (OUT/f"tab_{tab}_body.html").write_text(await page.content(), encoding="utf-8")
    return pd.DataFrame()

async def process_tab(context, page, tab):
    hits=[]
    def on_resp(resp):
        try:
            rq = resp.request
            if rq.resource_type in ("xhr","fetch"):
                u = resp.url
                if XHR_ALLOW_HOST in u and URL_INCLUDE_RE.search(u):
                    hits.append((resp,rq))
        except: pass

    page.on("response", on_resp)
    await page.goto(f"{INDEX}?tab={tab}", wait_until="networkidle")
    try: page.remove_listener("response", on_resp)
    except: pass

    if "login" in page.url.lower():
        (OUT/f"tab_{tab}_redirected_to_login.html").write_text(await page.content(), encoding="utf-8")
        return pd.DataFrame()

    # ถ้ามี XHR ที่เข้าข่าย ลองดึง
    frames=[]
    for i,(resp,rq) in enumerate(hits,1):
        try:
            m = rq.method.upper()
            h = {k:v for k,v in (await rq.all_headers()).items() if k.lower() not in {"host","content-length"}}
            body = await rq.post_data() if m!="GET" else None
            # ยิงซ้ำ
            if m=="GET":
                dup = await context.request.get(resp.url, headers=h)
            else:
                if (h.get("content-type","").lower().find("json")!=-1) and body:
                    try: dup = await context.request.post(resp.url, headers=h, json=json.loads(body))
                    except: dup = await context.request.post(resp.url, headers=h, data=body)
                else:
                    dup = await context.request.post(resp.url, headers=h, data=body)
            if not dup.ok: 
                (OUT/f"tab_{tab}_api_{i}_status.txt").write_text(str(dup.status), encoding="utf-8"); 
                continue
            # JSON?
            try:
                data = await dup.json()
                (OUT/f"tab_{tab}_api_{i}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                df = pd.json_normalize(data) if isinstance(data,(list,dict)) else pd.DataFrame([{"raw":data}])
                df.to_csv(OUT/f"tab_{tab}_api_{i}.csv", index=False)
                frames.append(df); 
                continue
            except: pass
            # HTML/text
            txt = await dup.text()
            (OUT/f"tab_{tab}_api_{i}.txt").write_text(txt, encoding="utf-8")
            try:
                for df in pd.read_html(StringIO(txt)):
                    frames.append(df)
            except: pass
        except Exception as e:
            (OUT/f"tab_{tab}_api_{i}_err.txt").write_text(str(e), encoding="utf-8")

    if frames:
        out = pd.concat(frames, ignore_index=True)
        out.to_csv(OUT/f"tab_{tab}_api_merged.csv", index=False)
        return out

    # fallback DOM
    return await extract_tables_from_dom(page, tab)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
                "--disable-web-security","--lang=th-TH"
            ]
        )
        context = await browser.new_context(
            viewport={"width":1400,"height":2000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            java_script_enabled=True,
            timezone_id="Asia/Bangkok",
            locale="th-TH",
        )

        # ลดการโหลดที่ไม่จำเป็น
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image","font"} else route.continue_())

        ok, page = await login_with_ui(context)
        await dump_page("after_login_check", page)

        if not ok:
            print("LOGIN_STATUS: FAIL")
            # ให้ job ล้มเพื่อบังคับดู Artifact
            await browser.close()
            sys.exit(1)

        print("LOGIN_STATUS: OK")

        # ถ้า login ผ่าน ลองดึงทุกแท็บ
        for t in TABS:
            df = await process_tab(context, page, t)
            print(f"TAB {t} -> rows={len(df)} cols={len(df.columns)}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
