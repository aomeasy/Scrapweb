import asyncio, pandas as pd
from playwright.async_api import async_playwright

USER, PASS = "01000566", "01000566"
BASE = "https://jobm.edoclite.com/jobManagement"
LOGIN_URL = f"{BASE}/pages/login"

# ใส่ endpoint ที่เห็นจาก DevTools → Network (XHR/Fetch)
API_MAP = {
    13: "counterOnline.php?jobm=191",
    14: "counterOnline.php?jobm=192",
    15: "counterOnline.php?jobm=193",
     8: "counterOnline.php?jobm=180",
     7: "counterOnline.php?jobm=179",
    11: "counterOnline.php?jobm=188",
}

# ปรับ selector ให้ตรงกับหน้า login จริงของคุณ
USER_SEL  = 'input[name="username"], #username'
PASS_SEL  = 'input[name="password"], #password'
SUBMIT_SEL= 'button[type="submit"], input[type="submit"]'

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = await browser.new_context()
        page = await ctx.new_page()

        # 1) Login ผ่าน selector (ให้เบราว์เซอร์จัดการ token/JS/redirect)
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.locator(USER_SEL).first.fill(USER)
        await page.locator(PASS_SEL).first.fill(PASS)
        await page.locator(SUBMIT_SEL).first.click()
        await page.wait_for_load_state("networkidle")

        # 2) เรียก API โดยตรง (แชร์คุกกี้/เฮดเดอร์กับ session ที่ล็อกอินแล้ว)
        for tab, path in API_MAP.items():
            url = f"{BASE}/{path}"
            resp = await ctx.request.get(url)
            resp.ok or (_ for _ in ()).throw(RuntimeError(f"{url} -> {resp.status}"))
            # พยายาม parse JSON ถ้าไม่ใช่ JSON จะเก็บเป็น text
            try:
                data = await resp.json()
                df = pd.json_normalize(data) if isinstance(data, (list, dict)) else pd.DataFrame([{"raw": data}])
            except:
                txt = await resp.text()
                df = pd.DataFrame([{"raw": txt}])

            print(f"TAB {tab}: {df.shape} rows/cols")
            # TODO: อัปโหลด df ไป Google Sheets (gspread) หรือบันทึกไฟล์
            # df.to_csv(f"tab_{tab}.csv", index=False)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
