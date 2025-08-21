# scrape.py — Selenium ดึงแท็บ 7/8/11/13/14/15
import os, time
from io import StringIO
from pathlib import Path
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException

# ========= CONFIG =========
BASE  = "https://jobm.edoclite.com/jobManagement"
LOGIN = f"{BASE}/pages/login"
INDEX = f"{BASE}/pages/index"

USER  = os.getenv("EDOCLITE_USER", "01000566")
PASS  = os.getenv("EDOCLITE_PASS", "01000566")

TABS  = [13, 14, 15, 8, 7, 11]

OUT = Path("output")
OUT.mkdir(exist_ok=True)
(OUT / "RUN_STARTED.txt").write_text("runner started", encoding="utf-8")
# =========================

def make_driver():
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=th-TH")
    opts.add_argument("--window-size=1400,2000")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=opts)

def wait_visible(driver, locator, timeout=15):
    return WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))

def _force_fill(driver, by, selector, text, timeout=15):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, selector)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    try: el.click()
    except Exception: pass
    try: el.clear()
    except Exception: pass
    try:
        ActionChains(driver).move_to_element(el).click(el).pause(0.1).send_keys(text).perform()
    except Exception:
        el.send_keys(text)
    v = el.get_attribute("value") or ""
    if v.strip() != str(text):
        driver.execute_script("""
            const el = arguments[0], val = arguments[1];
            el.value = val;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
        """, el, str(text))

def login(driver) -> bool:
    print("🔐 เปิดหน้า login")
    driver.get(LOGIN)
    (OUT / "login_page.html").write_text(driver.page_source, encoding="utf-8")

    # เผื่อแท็บไม่ active
    try:
        tab_emp = driver.find_element(By.CSS_SELECTOR, '#custom-tabs-one-home-tab')
        if "active" not in (tab_emp.get_attribute("class") or ""):
            tab_emp.click()
            time.sleep(0.2)
    except Exception:
        pass

    print("📝 กำลังกรอก user/pass")
    _force_fill(driver, By.NAME, "username", USER, timeout=20)
    _force_fill(driver, By.NAME, "password", PASS,  timeout=20)

    # submit แบบ JS ป้องกัน required bubble
    try:
        form = driver.find_element(By.CSS_SELECTOR, '#custom-tabs-one-home form[action="./login_db"]')
        driver.execute_script("arguments[0].submit();", form)
    except Exception:
        driver.find_element(By.NAME, "login__username").click()

    print("⏳ รอผลล็อกอิน")
    try:
        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.url_contains("/pages/index"),
                EC.presence_of_element_located((By.XPATH, "//a[contains(., 'งานใหม่')]"))
            )
        )
    except TimeoutException:
        (OUT / "after_login_fail.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail.png"))
        return False

    ok = ("เข้าสู่ระบบ" not in driver.page_source) or ("/pages/index" in driver.current_url)
    if ok:
        (OUT / "after_login_ok.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_ok.png"))
    else:
        (OUT / "after_login_fail.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail.png"))
    return ok

def datatables_expand_all_if_possible(driver):
    try:
        sel = None
        for css in ['select[name$="_length"]', "select.dt-input"]:
            elems = driver.find_elements(By.CSS_SELECTOR, css)
            if elems:
                sel = Select(elems[0]); break
        if not sel: return
        choices = [o.get_attribute("value") for o in sel.options]
        for v in ["-1","1000","500","250","100"]:
            if v in choices:
                sel.select_by_value(v); time.sleep(1.2); break
    except Exception: pass

def click_pagination_next_to_end(driver, max_clicks=200):
    try:
        for _ in range(max_clicks):
            cand = driver.find_elements(By.CSS_SELECTOR, 'a.paginate_button.next, button[aria-label="Next"], .dt-paging .dt-paging-button.next')
            if not cand: return
            btn = cand[0]
            if "disabled" in (btn.get_attribute("class") or "").lower(): return
            if hasattr(btn, "is_enabled") and not btn.is_enabled(): return
            btn.click(); time.sleep(0.7)
    except Exception: pass

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
    print(f"➡️  เข้าแท็บ {tab} : {url}")
    driver.get(url)
    if "เข้าสู่ระบบ" in driver.page_source:
        (OUT / f"tab_{tab}_redirected_to_login.html").write_text(driver.page_source, encoding="utf-8")
        return pd.DataFrame()
    datatables_expand_all_if_possible(driver)
    click_pagination_next_to_end(driver)
    html = driver.page_source
    (OUT / f"tab_{tab}.html").write_text(html, encoding="utf-8")
    df = html_tables_to_df(html)
    if not df.empty:
        df.to_csv(OUT / f"tab_{tab}.csv", index=False)
    return df

def main():
    print("🚀 เริ่มทำงาน…")
    driver = make_driver()
    try:
        ok = login(driver)
        print("LOGIN_STATUS:", "OK" if ok else "FAIL")
        if not ok:
            (OUT / "NO_DATA.txt").write_text("Login failed. See after_login_fail.html/png", encoding="utf-8")
            return  # ไม่ raise เพื่อให้ job ผ่านและอัปไฟล์ได้

        any_data = False
        for t in TABS:
            df = fetch_tab(driver, t)
            print(f"TAB {t}: rows={len(df)} cols={len(df.columns)}")
            if not df.empty:
                any_data = True

        if not any_data:
            (OUT / "NO_DATA.txt").write_text("All tabs empty or tables not found. See tab_*.html", encoding="utf-8")
    finally:
        driver.quit()
    print("✅ เสร็จสิ้น")

if __name__ == "__main__":
    main()
