import asyncio, json, re, os, pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright

USER, PASS = os.getenv("EDOCLITE_USER", "01000566"), os.getenv("EDOCLITE_PASS", "01000566")
BASE   = "https://jobm.edoclite.com/jobManagement"
LOGIN  = f"{BASE}/pages/login"
TABS   = [13, 14, 15, 8, 7, 11]

# ปรับ selector ให้ตรงกับหน้า login จริง ถ้าจำเป็น
USER_SEL   = 'input[name="username"], #username'
PASS_SEL   = 'input[name="password"], #password'
SUBMIT_SEL = 'button[type="submit"], input[type="submit"]'

# เงื่อนไขกรอง XHR/Fetch ที่ “น่าจะ” เป็นข้อมูล (ดัดแปลงได้)
XHR_ALLOW_HOST = "jobm.edoclite.com"
URL_INCLUDE_RE = re.compile(r"(counter|api|ajax|list|data|table|report|online)\.(php|aspx|json)|/(api|ajax|data)/", re.I)

OUT = Path("output"); OUT.mkdir(exist_ok=True)

async def login(page):
    await page.goto(LOGIN, wait_until="domcontentloaded")
    await page.locator(USER_SEL).first.fill(USER)
    await page.locator(PASS_SEL).first.fill(PASS)
    await page.locator(SUBMIT_SEL).first.click()
    await page.wait_for_load_state("networkidle")
    # ลองเข้า index เพื่อยืนยันว่าไม่เด้งกลับหน้า login
    await page.goto(f"{BASE}/pages/index", wait_until="networkidle")

async def extract_tables_from_dom(page, tab):
    # ดึงทุก table ใน DOM
    tables = await page.locator("table").all()
    htmls = [await t.evaluate("(el)=>el.outerHTML") for t in tables]
    dfs = []
    for h in htmls:
        try:
            for df in pd.read_html(h):
                df.columns = [str(c).strip() for c in df.columns]
                dfs.append(df)
        except ValueError:
            pass
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(OUT / f"tab_{tab}_dom.csv", index=False)
        return df
    # ไม่เจอ table → เซฟข้อความทั้งหน้าไว้ดีบัก
    body = await page.locator("body").inner_text()
    (OUT / f"tab_{tab}_body.txt").write_text(body, encoding="utf-8")
    return pd.DataFrame()

async def hit_same_request(ctx, req):
    """
    ยิงซ้ำด้วย context.request (ใช้คุกกี้/เฮดเดอร์เดียวกับหน้าปัจจุบัน)
    รองรับทั้ง GET/POST พร้อม post_data
    """
    method = req.method.upper()
    url    = req.url
    headers = {k:v for k,v in (await req.all_headers()).items()
               if k.lower() not in {"host","content-length"}}  # กัน header ต้องห้าม
    # ใส่ Referer ให้เหมือนเดิม (ช่วยผ่านบางเว็บ)
    if "referer" not in {k.lower() for k in headers}:
        headers["Referer"] = req.headers.get("referer", f"{BASE}/pages/index")

    post_data = await req.post_data() if method != "GET" else None

    if method == "GET":
        return await ctx.request.get(url, headers=headers)
    else:
        # พยายามคง content-type
        ctype = req.headers.get("content-type","").lower()
        if "json" in ctype and post_data:
            try:
                return await ctx.request.post(url, headers=headers, data=None, json=json.loads(post_data))
            except Exception:
                pass
        return await ctx.request.post(url, headers=headers, data=post_data)

async def process_tab(ctx, page, tab):
    url = f"{BASE}/pages/index?tab={tab}"
    hits = []

    # event handler เก็บ XHR/Fetch
    def on_response(resp):
        try:
            rq = resp.request
            if rq.resource_type in ("xhr","fetch"):
                u = resp.url
                # กรอง host + ชื่อที่เข้าข่ายข้อมูล
                if XHR_ALLOW_HOST in u and URL_INCLUDE_RE.search(u):
                    hits.append((resp, rq))
        except:
            pass

        page.on("response", on_response)
        await page.goto(url, wait_until="networkidle")
        try:
            page.remove_listener("response", on_response)
        except Exception:
            # รองรับกรณีเวอร์ชันเก่า/พฤติกรรมไม่เหมือนกัน
            pass


    # ถ้ามี XHR เข้าข่าย → ลองยิงซ้ำ (เอาตัวสุดท้ายก่อน)
    if hits:
        all_frames = []
        for idx, (resp, rq) in enumerate(hits, 1):
            try:
                dup = await hit_same_request(ctx, rq)
                if not dup.ok:
                    continue
                # พยายาม parse JSON ก่อน
                try:
                    data = await dup.json()
                    df = pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame([{"raw": data}])
                    df.to_csv(OUT / f"tab_{tab}_api_{idx}.csv", index=False)
                    # เก็บ raw ไว้ด้วย
                    (OUT / f"tab_{tab}_api_{idx}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                    all_frames.append(df)
                except Exception:
                    txt = await dup.text()
                    (OUT / f"tab_{tab}_api_{idx}.txt").write_text(txt, encoding="utf-8")
                    # เผื่อเป็น HTML table
                    try:
                        for df in pd.read_html(txt):
                            all_frames.append(df)
                    except:
                        pass
            except Exception as e:
                (OUT / f"tab_{tab}_api_{idx}_err.txt").write_text(str(e), encoding="utf-8")

        if all_frames:
            df_all = pd.concat(all_frames, ignore_index=True)
            df_all.to_csv(OUT / f"tab_{tab}_api_merged.csv", index=False)
            return df_all

    # ไม่เจอ/ยิง API ไม่ผ่าน → fallback DOM
    return await extract_tables_from_dom(page, tab)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = await browser.new_context(viewport={"width": 1400, "height": 2000})
        page = await ctx.new_page()

        await login(page)

        for t in TABS:
            df = await process_tab(ctx, page, t)
            print(f"TAB {t} -> rows={len(df)} cols={len(df.columns)}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
