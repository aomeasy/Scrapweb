import os
import time
from io import StringIO
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ========= CONFIG =========
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"

# ดึงจาก Secrets/Env ถ้าไม่ตั้ง จะใช้ค่าด้านหลัง (อย่าลืมใส่ใน GitHub Secrets)
USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")

# แท็บที่ต้องดึง
TABS  = [13, 14, 15, 8, 7, 11]

# Google Sheets (ถ้าใส่ทั้งสองตัว จะอัปให้)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

OUT = Path("output")
OUT.mkdir(exist_ok=True)
# =========================


def make_driver():
    """สร้าง Chrome headless ที่เหมาะกับ GitHub Actions."""
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=th-TH")
    opts.add_argument("--window-size=1400,2000")
    # ปิดโหลดภาพ/ฟอนต์ให้เร็วขึ้น (ไม่บล็อค JS)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=opts)


def wait_visible(driver, locator, timeout=15):
    return WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))


def login(driver) -> bool:
    driver.get(LOGIN)
    # บันทึกหน้า login ไว้ดูย้อนหลัง
    (OUT / "login_page.html").write_text(driver.page_source, encoding="utf-8")

    wait_visible(driver, (By.NAME, "username"))
    driver.find_element(By.NAME, "username").clear()
    driver.find_element(By.NAME, "username").send_keys(USER)
    driver.find_element(By.NAME, "password").clear()
    driver.find_element(By.NAME, "password").send_keys(PASS)

    # ปุ่ม submit ที่ถูกตัว
    driver.find_element(By.NAME, "login__username").click()

    # รอเมนู/ลิงก์ที่มีเฉพาะหลังล็อกอิน (จากตัวอย่างของคุณ: “งานใหม่”)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(., 'งานใหม่')]"))
        )
    except TimeoutException:
        # เผื่อบางระบบเปลี่ยนข้อความ เมื่อล้มเหลวเก็บ HTML ไว้ดู
        (OUT / "after_login_fail.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail.png"))
        return False

    # เซฟหลักฐานหลังผ่าน
    (OUT / "after_login_ok.html").write_text(driver.page_source, encoding="utf-8")
    driver.save_screenshot(str(OUT / "after_login_ok.png"))
    return True


def datatables_expand_all_if_possible(driver):
    """
    พยายามสั่ง DataTables ให้แสดง All/จำนวนสูงสุด แล้วค่อยดึงตาราง
    - หยิบ select ที่ลงท้ายด้วย _length หรือ select.dt-input
    """
    try:
        sel = None
        # candidates
        for css in ['select[name$="_length"]', "select.dt-input"]:
            elems = driver.find_elements(By.CSS_SELECTOR, css)
            if elems:
                sel = Select(elems[0])
                break
        if not sel:
            return

        # ลองค่า -1 (All) หรือจำนวนสูงสุดที่หาเจอ
        choices = [o.get_attribute("value") for o in sel.options]
        for v in ["-1", "1000", "500", "250", "100"]:
            if v in choices:
                sel.select_by_value(v)
                # รอให้ DataTables รีเฟรช
                time.sleep(1.5)
                break
    except Exception:
        pass


def click_pagination_next_to_end(driver, max_clicks=200):
    try:
        for _ in range(max_clicks):
            # ปุ่ม next แบบที่พบได้บ่อย
            cand = driver.find_elements(By.CSS_SELECTOR, 'a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
            if not cand:
                return
            btn = cand[0]
            if "disabled" in (btn.get_attribute("class") or "").lower():
                return
            # ถ้าเป็น button element อาจใช้ is_enabled()
            if hasattr(btn, "is_enabled") and not btn.is_enabled():
                return
            btn.click()
            time.sleep(0.8)
    except Exception:
        pass


def html_tables_to_df(html: str) -> pd.DataFrame:
    dfs = []
    try:
        for df in pd.read_html(StringIO(html)):
            df.columns = [str(c).strip() for c in df.columns]
            dfs.append(df)
    except ValueError:
        pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_tab(driver, tab: int) -> pd.DataFrame:
    url = f"{INDEX}?tab={tab}"
    driver.get(url)

    # ถ้ายังเด้งกลับหน้า login
    if "เข้าสู่ระบบ" in driver.page_source:
        (OUT / f"tab_{tab}_redirected_to_login.html").write_text(driver.page_source, encoding="utf-8")
        return pd.DataFrame()

    # พยายาม All + คลิก next จนสุด แล้วดึงตาราง
    datatables_expand_all_if_possible(driver)
    click_pagination_next_to_end(driver)

    html = driver.page_source
    (OUT / f"tab_{tab}.html").write_text(html, encoding="utf-8")
    df = html_tables_to_df(html)
    if not df.empty:
        df.to_csv(OUT / f"tab_{tab}.csv", index=False)
    return df


# ------------- Google Sheets -------------
def get_gsheet():
    if not (GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON):
        return None
    import json, gspread
    from google.oauth2.service_account import Credentials
    data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)

def write_df_to_sheet(sh, title, df: pd.DataFrame):
    if sh is None or df.empty:
        return
    import gspread
    try:
        ws = sh.worksheet(title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(max(len(df)+10, 1000)), cols=str(max(len(df.columns)+5, 26)))
    header = [str(c) for c in df.columns]
    values = [header] + df.fillna("").astype(str).values.tolist()
    ws.update("A1", values)


def main():
    sh = get_gsheet()
    driver = make_driver()
    try:
        ok = login(driver)
        print("LOGIN_STATUS:", "OK" if ok else "FAIL")
        if not ok:
            # สร้างไฟล์ชี้ว่า run นี้ไม่มี data เพื่อให้ artifact step มีอะไรให้อัปโหลดเสมอ
            (OUT / "NO_DATA.txt").write_text("Login failed. See after_login_fail.html/png", encoding="utf-8")
            raise SystemExit(1)

        any_data = False
        for t in TABS:
            df = fetch_tab(driver, t)
            print(f"TAB {t}: rows={len(df)} cols={len(df.columns)}")
            if not df.empty:
                any_data = True
                write_df_to_sheet(sh, f"TAB_{t}", df)

        if not any_data:
            (OUT / "NO_DATA.txt").write_text("All tabs empty or tables not found. See tab_*.html", encoding="utf-8")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
