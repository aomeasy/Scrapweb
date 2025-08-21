from selenium.webdriver import ActionChains

def _force_fill(driver, by, selector, text, timeout=15):
    """กรอกค่าให้สำเร็จแน่ ๆ: click → clear → send_keys → ถ้ายังว่าง → ตั้งค่าโดย JS"""
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, selector))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    try:
        el.click()
    except Exception:
        pass
    try:
        el.clear()
    except Exception:
        pass

    # พิมพ์ด้วย action (เสถียรกว่าในบางธีม)
    try:
        ActionChains(driver).move_to_element(el).click(el).pause(0.1).send_keys(text).perform()
    except Exception:
        el.send_keys(text)

    # ตรวจว่าค่าลงจริงหรือยัง
    v = el.get_attribute("value") or ""
    if v.strip() != str(text):
        # ใส่ด้วย JS และยิง input/change เผื่อมี listener
        driver.execute_script("""
            const el = arguments[0], val = arguments[1];
            el.value = val;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
        """, el, str(text))

def login(driver) -> bool:
    driver.get(LOGIN)
    (OUT / "login_page.html").write_text(driver.page_source, encoding="utf-8")

    # เผื่อโดนสลับแท็บ ให้จิ้มแท็บ "พนักงาน" ให้ active อีกรอบ
    try:
        tab_emp = driver.find_element(By.CSS_SELECTOR, 'a#custom-tabs-one-home-tab')
        if "active" not in (tab_emp.get_attribute("class") or ""):
            tab_emp.click()
            time.sleep(0.2)
    except Exception:
        pass

    # กรอกแบบ force
    _force_fill(driver, By.NAME, "username", USER, timeout=20)
    _force_fill(driver, By.NAME, "password", PASS,  timeout=20)

    # ยืนยันว่าค่าลงแล้วจริงๆ ก่อนส่งฟอร์ม
    u_ok = (driver.find_element(By.NAME, "username").get_attribute("value") or "").strip() != ""
    p_ok = (driver.find_element(By.NAME, "password").get_attribute("value") or "").strip() != ""
    if not (u_ok and p_ok):
        # เก็บหลักฐานแล้วจบ
        (OUT / "after_login_fail_before_submit.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail_before_submit.png"))
        return False

    # ส่งฟอร์มด้วย JS ป้องกันปัญหา required bubble
    try:
        form = driver.find_element(By.CSS_SELECTOR, '#custom-tabs-one-home form[action="./login_db"]')
        driver.execute_script("arguments[0].submit();", form)
    except Exception:
        # fallback: คลิกปุ่มปกติ
        driver.find_element(By.NAME, "login__username").click()

    # รอผลลัพธ์: URL เปลี่ยนเป็น /pages/index หรือไม่มีคำว่า "เข้าสู่ระบบ"
    try:
        WebDriverWait(driver, 15).until(
            EC.any_of(
                EC.url_contains("/pages/index"),
                EC.presence_of_element_located((By.XPATH, "//a[contains(., 'งานใหม่')]")),
                EC.invisibility_of_element_located((By.XPATH, "//title[contains(.,'เข้าสู่ระบบ')]"))
            )
        )
    except TimeoutException:
        (OUT / "after_login_fail.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail.png"))
        return False

    # ตรวจซ้ำอีกชั้นเพื่อความชัวร์
    ok = ("เข้าสู่ระบบ" not in driver.page_source) or ("/pages/index" in driver.current_url)
    if ok:
        (OUT / "after_login_ok.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_ok.png"))
    else:
        (OUT / "after_login_fail.html").write_text(driver.page_source, encoding="utf-8")
        driver.save_screenshot(str(OUT / "after_login_fail.png"))
    return ok
