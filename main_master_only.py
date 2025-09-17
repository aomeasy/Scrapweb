# main_master_only.py

import os
import json
import base64
import pytz
import sys
import time
from io import StringIO
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

# ==============================================================================
# ⚙️ SECTION 1: CONFIGURATION
# ==============================================================================
# ตั้งค่า Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Config:
    """เก็บการตั้งค่าทั้งหมดของโปรแกรมไว้ในที่เดียว"""
    # Target Website
    BASE_URL = "https://jobm.edoclite.com/jobManagement"
    LOGIN_URL = f"{BASE_URL}/pages/login"
    INDEX_URL = f"{BASE_URL}/pages/index"
    TABS_TO_SCRAPE: List[int] = [13, 14, 15, 8, 7, 11]
    TAB_NAMES: Dict[int, str] = {
        13: "งานใหม่_แจ้งศูนย์อื่น",
        14: "อยู่ระหว่างดำเนินการ_แจ้งศูนย์อื่น", 
        15: "รอตรวจสอบ_แจ้งศูนย์อื่น",
        8: "งานใหม่_ภายในศูนย์",
        7: "อยู่ระหว่างดำเนินการ_ภายในศูนย์",
        11: "งานเสร็จ_ภายในศูนย์"
    }

    # Credentials (from Environment Variables)
    EDOCLITE_USER = os.getenv("EDOCLITE_USER", "").strip()
    EDOCLITE_PASS = os.getenv("EDOCLITE_PASS", "").strip()
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
    GOOGLE_SVC_JSON_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    GOOGLE_SVC_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
    LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "").strip()

    # Google Sheets
    GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    MASTER_SHEET_NAME = "Master_Data"
    LOG_SHEET_NAME = "Sync_Logs"

# ==============================================================================
# 📦 SECTION 2: HELPER SERVICES (CLASSES)
# ==============================================================================

class Notifier:
    """จัดการการส่งข้อความแจ้งเตือนผ่าน LINE Notify"""
    def __init__(self, token: str):
        self.token = token

    def send(self, message: str) -> bool:
        if not self.token:
            logger.warning("⚠️ LINE_NOTIFY_TOKEN is not set. Skipping notification.")
            return False
        try:
            url = "https://notify-api.line.me/api/notify"
            headers = {"Authorization": f"Bearer {self.token}"}
            data = {"message": message}
            response = requests.post(url, headers=headers, data=data, timeout=10)
            if response.status_code == 200:
                logger.info("📱 LINE Notify sent successfully.")
                return True
            else:
                logger.error(f"❌ LINE Notify failed with status code {response.status_code}: {response.text}")
                return False
        except requests.RequestException as e:
            logger.error(f"❌ Exception during LINE Notify request: {e}")
            return False

class GoogleSheetManager:
    """จัดการการเชื่อมต่อและการดำเนินการทั้งหมดกับ Google Sheets"""
    def __init__(self, sheet_id: str, svc_json_raw: str, svc_json_b64: str):
        self.sheet_id = sheet_id
        self.client = self._get_gspread_client(svc_json_raw, svc_json_b64)
        self.spreadsheet = self.client.open_by_key(self.sheet_id)
        logger.info(f"✅ Connected to Google Sheet: '{self.spreadsheet.title}'")

    def _get_gspread_client(self, svc_json_raw: str, svc_json_b64: str) -> gspread.Client:
        info = None
        try:
            if svc_json_b64:
                info = json.loads(base64.b64decode(svc_json_b64).decode("utf-8"))
            elif svc_json_raw:
                info = json.loads(svc_json_raw)
            elif os.path.exists("service_account.json"):
                with open("service_account.json", "r", encoding="utf-8") as f:
                    info = json.load(f)
            if not info:
                raise ValueError("Google Service Account JSON not found.")
            creds = Credentials.from_service_account_info(info, scopes=Config.GOOGLE_API_SCOPES)
            return gspread.authorize(creds)
        except Exception as e:
            logger.error(f"❌ Google Sheets connection error: {e}")
            raise

    def get_or_create_worksheet(self, title: str, headers: Optional[List[str]] = None) -> gspread.Worksheet:
        try:
            return self.spreadsheet.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"📄 Creating new sheet: '{title}'")
            ws = self.spreadsheet.add_worksheet(title=title, rows=1, cols=len(headers) if headers else 20)
            if headers:
                ws.update("A1", [headers])
                ws.freeze(rows=1)
            return ws

    def get_all_job_nos(self, worksheet_name: str) -> set:
        """ดึง Job_No ทั้งหมดเพื่อใช้ตรวจสอบข้อมูลซ้ำ"""
        try:
            ws = self.get_or_create_worksheet(worksheet_name)
            # ดึงเฉพาะคอลัมน์แรก (สมมติว่าเป็น Job_No) เพื่อลดปริมาณข้อมูล
            job_nos = ws.col_values(1)[1:] # [1:] to skip header
            logger.info(f"Found {len(job_nos)} existing Job_Nos in '{worksheet_name}'.")
            return set(job_nos)
        except Exception as e:
            logger.error(f"❌ Could not fetch existing Job_Nos from '{worksheet_name}': {e}")
            return set()
    
    def get_job_data_with_positions(self, worksheet_name: str) -> Dict[str, Dict]:
        """ดึงข้อมูล Job_No พร้อมตำแหน่งแถวและคอลัมน์ Status"""
        try:
            ws = self.get_or_create_worksheet(worksheet_name)
            all_data = ws.get_all_records()
            headers = ws.row_values(1)
            
            # หา index ของคอลัมน์ Job_No และ Source_Tab
            job_no_col_idx = None
            source_tab_col_idx = None
            
            for idx, header in enumerate(headers):
                if 'job' in str(header).lower() and 'no' in str(header).lower():
                    job_no_col_idx = idx + 1  # gspread uses 1-based indexing
                elif header == 'Source_Tab':
                    source_tab_col_idx = idx + 1
            
            job_positions = {}
            for row_idx, row_data in enumerate(all_data, start=2):  # start=2 เพราะแถว 1 คือ header
                job_no = str(row_data.get('Job_No', '')).strip() if 'Job_No' in row_data else ''
                if job_no:
                    job_positions[job_no] = {
                        'row': row_idx,
                        'source_tab_col': source_tab_col_idx,
                        'current_status': row_data.get('Source_Tab', '')
                    }
            
            logger.info(f"Found {len(job_positions)} existing jobs with positions in '{worksheet_name}'.")
            return job_positions
        except Exception as e:
            logger.error(f"❌ Could not fetch job data with positions from '{worksheet_name}': {e}")
            return {}
    
    def update_job_status(self, worksheet_name: str, job_no: str, new_status: str, row: int, col: int):
        """อัปเดตสถานะของงานที่มีอยู่แล้ว"""
        try:
            ws = self.get_or_create_worksheet(worksheet_name)
            ws.update_cell(row, col, new_status)
            logger.info(f"✅ Updated {job_no} status to '{new_status}' at row {row}")
        except Exception as e:
            logger.error(f"❌ Failed to update status for {job_no}: {e}")

    def append_rows(self, worksheet_name: str, data_rows: List[List[Any]]):
        """เพิ่มแถวข้อมูลใหม่ต่อท้ายชีต"""
        if not data_rows:
            return
        try:
            ws = self.get_or_create_worksheet(worksheet_name)
            ws.append_rows(data_rows, value_input_option='USER_ENTERED')
            logger.info(f"✅ Appended {len(data_rows)} new rows to '{worksheet_name}'.")
        except Exception as e:
            logger.error(f"❌ Failed to append rows to '{worksheet_name}': {e}")
    
    def log_activity(self, activity: str, details: str = "", status: str = "Success"):
        try:
            ws = self.get_or_create_worksheet(Config.LOG_SHEET_NAME, headers=['Timestamp', 'Activity', 'Details', 'Status'])
            
            # ใช้เวลาประเทศไทย
            thailand_tz = pytz.timezone('Asia/Bangkok')
            thailand_time = datetime.now(thailand_tz)
            ts = thailand_time.strftime('%d/%m/%Y %H:%M:%S')
            
            ws.insert_row([ts, activity, details, status], 2)
        except Exception as e:
            logger.error(f"❌ Failed to log activity: {e}")

# ==============================================================================
# 🌐 SECTION 3: WEB SCRAPER
# ==============================================================================

class WebScraper:
    """จัดการกระบวนการ Scrape ข้อมูลจากเว็บไซต์ด้วย Selenium"""
    def __init__(self, user: str, password: str):
        self.user = user
        self.password = password
        if not self.user or not self.password:
            raise ValueError("EDOCLITE_USER and EDOCLITE_PASS must be set.")

    def create_driver(self) -> webdriver.Chrome:
        """สร้าง Chrome WebDriver ด้วย options ที่เหมาะสม"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        
        return webdriver.Chrome(options=chrome_options)

    def login(self, driver: webdriver.Chrome) -> Tuple[bool, webdriver.Chrome]:
        """เข้าสู่ระบบ"""
        try:
            logger.info(f"Navigating to login page: {Config.LOGIN_URL}")
            driver.get(Config.LOGIN_URL)
            
            # รอให้หน้าโหลดและหา elements
            wait = WebDriverWait(driver, 10)
            
            # ใส่ username
            username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
            username_field.clear()
            username_field.send_keys(self.user)
            
            # ใส่ password
            password_field = driver.find_element(By.NAME, "password")
            password_field.clear()
            password_field.send_keys(self.password)
            
            # คลิก login button
            login_button = driver.find_element(By.NAME, "login__username")
            login_button.click()
            
            # รอให้เปลี่ยนหน้า
            time.sleep(3)
            
            # ตรวจสอบว่า login สำเร็จหรือไม่
            if "login" in driver.current_url.lower():
                logger.error("❌ Login failed: Redirected back to login page.")
                return False, driver
            
            logger.info("✅ Login successful.")
            return True, driver
            
        except Exception as e:
            logger.error(f"❌ Exception during login: {e}")
            driver.save_screenshot("login_error.png")
            return False, driver

    def extract_data_from_tab(self, driver: webdriver.Chrome, tab_num: int) -> pd.DataFrame:
        """ดึงข้อมูลจากแต่ละ tab"""
        url = f"{Config.INDEX_URL}?tab={tab_num}"
        logger.info(f"Scraping tab {tab_num} at {url}")
        
        try:
            driver.get(url)
            time.sleep(3)
            
            # พยายามเปลี่ยน page length เป็น show all
            try:
                wait = WebDriverWait(driver, 5)
                length_select = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name$="_length"]')))
                select = Select(length_select)
                select.select_by_value("-1")
                time.sleep(2)
                logger.info("Set table length to show all entries.")
            except (TimeoutException, NoSuchElementException):
                logger.warning("Could not find or change page length selector.")
                pass
            
            # ดึง HTML content
            html_content = driver.page_source
            
            # แปลง HTML เป็น DataFrame
            dfs = pd.read_html(StringIO(html_content))
            job_df = next((df for df in dfs if not df.empty and any('job' in str(col).lower() for col in df.columns)), pd.DataFrame())
            
            if job_df.empty:
                logger.warning(f"⚠️ No data table found on tab {tab_num}.")
                return pd.DataFrame()
            
            logger.info(f"📊 Found {len(job_df)} rows in tab {tab_num}.")
            return job_df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract data from tab {tab_num}: {e}")
            driver.save_screenshot(f"tab_{tab_num}_error.png")
            return pd.DataFrame()

# ==============================================================================
# 🚀 SECTION 4: MAIN APPLICATION LOGIC
# ==============================================================================

class JobSyncApplication:
    def __init__(self, config: Config):
        self.config = config
        self.notifier = Notifier(config.LINE_NOTIFY_TOKEN)
        self.sheet_manager = GoogleSheetManager(
            config.GOOGLE_SHEET_ID, 
            config.GOOGLE_SVC_JSON_RAW, 
            config.GOOGLE_SVC_JSON_B64
        )
        self.scraper = WebScraper(config.EDOCLITE_USER, config.EDOCLITE_PASS)
  
# ในไฟล์ main_master_only.py
# แก้ไขใน method _process_and_add_new_jobs

    def _process_and_add_new_jobs(self, all_tab_data: Dict[int, pd.DataFrame]) -> Tuple[int, int]:
        """กรองเฉพาะ Job ใหม่และเพิ่มลงใน Master Sheet หรือ อัปเดตสถานะของงานเดิม"""
        logger.info("Processing jobs: checking for new jobs and status updates...")
        
        # สร้าง Thailand timezone
        thailand_tz = pytz.timezone('Asia/Bangkok')
        
        # ดึงข้อมูล Job ที่มีอยู่แล้วพร้อมตำแหน่ง
        existing_jobs = self.sheet_manager.get_job_data_with_positions(self.config.MASTER_SHEET_NAME)
        
        new_records_to_add = []
        updated_jobs_count = 0
        
        # ✅ กำหนด headers ที่ต้องการ (ไม่ซ้ำกัน)
        all_headers = set(['Job_No', 'Source_Tab', 'First_Seen'])  # ใช้ Job_No เท่านั้น
    
        for tab_num, df in all_tab_data.items():
            if df.empty:
                continue
            
            # เก็บ Headers จากข้อมูลต้นฉบับ (ยกเว้น Job No. ที่ซ้ำ)
            for col in df.columns:
                col_str = str(col)
                # ✅ ไม่เพิ่มคอลัมน์ที่มี "job" และ "no" ซ้ำกัน
                if not (('job' in col_str.lower() and 'no' in col_str.lower()) or col_str.lower() == 'job no.'):
                    all_headers.add(col_str)
    
            tab_name = self.config.TAB_NAMES.get(tab_num, f"Tab_{tab_num}")
            
            # หาคอลัมน์ที่มี Job No (อาจชื่อต่างกัน)
            job_no_col = None
            for col in df.columns:
                col_str = str(col).lower()
                if 'job' in col_str and ('no' in col_str or 'number' in col_str):
                    job_no_col = col
                    break
            
            if not job_no_col:
                logger.warning(f"⚠️ No 'Job No.' column found in tab {tab_num}. Skipping.")
                continue
    
            for _, row in df.iterrows():
                job_no = str(row[job_no_col]).strip()
                if not job_no:
                    continue  # ข้ามถ้าไม่มี Job No
                
                # ตรวจสอบว่า Job No มีอยู่แล้วหรือไม่
                if job_no in existing_jobs:
                    # มีอยู่แล้ว - ตรวจสอบว่าสถานะเปลี่ยนหรือไม่
                    current_status = existing_jobs[job_no]['current_status']
                    if current_status != tab_name:
                        # สถานะเปลี่ยน - อัปเดต
                        self.sheet_manager.update_job_status(
                            self.config.MASTER_SHEET_NAME,
                            job_no,
                            tab_name,
                            existing_jobs[job_no]['row'],
                            existing_jobs[job_no]['source_tab_col']
                        )
                        updated_jobs_count += 1
                        self.notifier.send(f"🔄 อัปเดตสถานะงาน: {job_no}\n   จาก: {current_status}\n   เป็น: {tab_name}")
                else:
                    # ไม่มี - เพิ่มใหม่
                    new_record = {}
                    
                    # ✅ คัดลอกข้อมูลจากแถวต้นฉบับ (ยกเว้นคอลัมน์ Job ที่ซ้ำ)
                    for col, val in row.items():
                        col_str = str(col)
                        # ไม่เพิ่มคอลัมน์ Job No. ที่ซ้ำ
                        if not (('job' in col_str.lower() and 'no' in col_str.lower()) or col_str.lower() == 'job no.'):
                            new_record[col_str] = str(val)
                    
                    # เพิ่มข้อมูลพิเศษ
                    new_record['Job_No'] = job_no  # ✅ ใช้ชื่อเดียวเท่านั้น
                    new_record['Source_Tab'] = tab_name
                    
                    # สร้าง First_Seen ด้วยเวลาประเทศไทย
                    thailand_time = datetime.now(thailand_tz)
                    new_record['First_Seen'] = thailand_time.strftime('%d/%m/%Y %H:%M:%S')
                    
                    new_records_to_add.append(new_record)
                    existing_jobs[job_no] = {'current_status': tab_name}  
                    self.notifier.send(f"🆕 งานใหม่: {job_no} (จาก {tab_name})")
    
        # เพิ่มงานใหม่ลง Sheet
        if new_records_to_add:
            # ตรวจสอบและสร้าง Header ให้ Master Sheet หากยังไม่มี
            master_ws = self.sheet_manager.get_or_create_worksheet(self.config.MASTER_SHEET_NAME)
            
            # ✅ เรียงลำดับ headers ให้ Job_No อยู่คอลัมน์แรก
            final_headers = ['Job_No', 'First_Seen', 'Source_Tab'] + sorted([h for h in all_headers if h not in ['Job_No', 'First_Seen', 'Source_Tab']])
            
            if master_ws.row_count == 1 and master_ws.col_count == 1 and master_ws.cell(1,1).value is None:
                # Sheet is empty, write headers
                master_ws.update("A1", [final_headers])
            else:
                # ตรวจสอบ headers ที่มีอยู่แล้ว
                existing_headers = master_ws.row_values(1)
                
                # ถ้า headers ไม่ตรงกัน ให้อัปเดต
                if set(existing_headers) != set(final_headers):
                    logger.info("📋 Updating sheet headers to remove duplicates...")
                    master_ws.update("A1", [final_headers])
    
            # แปลง dicts เป็น list of lists ตามลำดับ header
            rows_to_append = []
            for record in new_records_to_add:
                row = [record.get(h, "") for h in final_headers]
                rows_to_append.append(row)
            
            self.sheet_manager.append_rows(self.config.MASTER_SHEET_NAME, rows_to_append)
    
        return len(new_records_to_add), updated_jobs_count

    def run(self):
        """ฟังก์ชันหลักสำหรับรันกระบวนการทั้งหมด"""
        start_time = datetime.now()
        self.sheet_manager.log_activity("Sync Start", "เริ่มต้นกระบวนการซิงค์งาน")
        
        all_tab_data = {}
        successful_tabs, failed_tabs = [], []
        
        # สร้าง WebDriver
        driver = self.scraper.create_driver()
        
        try:
            # Login
            logged_in, driver = self.scraper.login(driver)
            if not logged_in:
                self.notifier.send("❌ ข้อผิดพลาดร้ายแรง: เข้าสู่ระบบ edoclite ไม่ได้ กรุณาตรวจสอบ username/password")
                self.sheet_manager.log_activity("Login Failed", "ไม่สามารถเข้าสู่ระบบได้", "Failed")
                return
            
            # Scrape แต่ละ tab
            for tab in self.config.TABS_TO_SCRAPE:
                df = self.scraper.extract_data_from_tab(driver, tab)
                if not df.empty:
                    all_tab_data[tab] = df
                    successful_tabs.append(tab)
                else:
                    failed_tabs.append(tab)
                time.sleep(1)
        
        finally:
            # ปิด browser
            driver.quit()
        
        # ประมวลผลและเพิ่มข้อมูลใหม่ หรือ อัปเดตสถานะ
        new_jobs_count, updated_jobs_count = self._process_and_add_new_jobs(all_tab_data)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Log summary
        summary_details = f"เพิ่มงานใหม่ {new_jobs_count} งาน, อัปเดตสถานะ {updated_jobs_count} งาน. แท็บสำเร็จ: {len(successful_tabs)}. แท็บล้มเหลว: {len(failed_tabs)}."
        status = "Success" if not failed_tabs else "Partial Success"
        self.sheet_manager.log_activity("Sync Complete", summary_details, status)
        
        # Send final notification
        summary_msg = f"""
        ✅ ซิงค์งานเสร็จสิ้น!
        
        - 🆕 พบและเพิ่มงานใหม่: {new_jobs_count} งาน
        - 🔄 อัปเดตสถานะงาน: {updated_jobs_count} งาน
        - 🗂️ แท็บที่ดึงข้อมูลได้: {len(successful_tabs)}/{len(self.config.TABS_TO_SCRAPE)}
        - ⏱️ ใช้เวลา: {duration:.2f} วินาที
        
        🔗 Master Sheet: https://docs.google.com/spreadsheets/d/{self.config.GOOGLE_SHEET_ID}
        """.strip()
        self.notifier.send(summary_msg)
        logger.info(f"🎉 กระบวนการเสร็จสิ้นใน {duration:.2f} วินาที")


# ==============================================================================
# ▶️ SECTION 5: SCRIPT EXECUTION
# ==============================================================================

if __name__ == "__main__":
    try:
        app_config = Config()
        app = JobSyncApplication(app_config)
        app.run()
        sys.exit(0)
    except (ValueError, gspread.exceptions.GSpreadException) as e:
        logger.error(f"💥 ข้อผิดพลาดในการตั้งค่าหรือ Google Sheets: {e}")
        token = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
        if token:
            Notifier(token).send(f"💥 ซิงค์งานล้มเหลว (ข้อผิดพลาดการตั้งค่า): {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 เกิดข้อผิดพลาดร้ายแรงที่ไม่คาดคิด: {e}", exc_info=True)
        token = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
        if token:
            Notifier(token).send(f"💥 ระบบซิงค์งานขัดข้อง: เกิดข้อผิดพลาดที่ไม่คาดคิด กรุณาตรวจสอบ log {e}")
        sys.exit(1)
