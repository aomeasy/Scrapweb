# main_master_only.py

import os
import json
import base64
import sys
from io import StringIO
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from selenium_wrapper import sync_selenium
from selenium.webdriver.remote.webdriver import WebDriver as Page
from selenium.webdriver.remote.webdriver import WebDriver as BrowserContext
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
        13: "InProgress_Jobs", 14: "Pending_Jobs", 15: "Completed_Jobs",
        8: "Urgent_Jobs", 7: "Review_Jobs", 11: "Archive_Jobs"
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
            ts = datetime.now(timezone.utc).isoformat()
            ws.insert_row([ts, activity, details, status], 2)
        except Exception as e:
            logger.error(f"❌ Failed to log activity: {e}")

# ==============================================================================
# 🌐 SECTION 3: WEB SCRAPER
# ==============================================================================

class WebScraper:
    """จัดการกระบวนการ Scrape ข้อมูลจากเว็บไซต์ด้วย Playwright"""
    def __init__(self, user: str, password: str):
        self.user = user
        self.password = password
        if not self.user or not self.password:
            raise ValueError("EDOCLITE_USER and EDOCLITE_PASS must be set.")

    def login(self, context: BrowserContext) -> Tuple[bool, Page]:
        page = context
        try:
            logger.info(f"Navigating to login page: {Config.LOGIN_URL}")
            page.get(Config.LOGIN_URL)
            page.find_element("name", "username").send_keys(self.user)
            page.find_element("name", "password").send_keys(self.password)
            page.find_element("name", "login__username").click()
            # Wait and check URL
            import time
            time.sleep(3)
            if "login" in page.current_url.lower():
                logger.error("❌ Login failed: Redirected back to login page.")
                return False, page
            logger.info("✅ Login successful.")
            return True, page
        except Exception as e:
            logger.error(f"❌ Exception during login: {e}")
            page.save_screenshot("login_error.png")
            return False, page

    def extract_data_from_tab(self, page: Page, tab_num: int) -> pd.DataFrame:
        url = f"{Config.INDEX_URL}?tab={tab_num}"
        logger.info(f"Scraping tab {tab_num} at {url}")
        try:
            page.get(url)
            import time
            time.sleep(3)
            try:
                length_selector = page.find_element("css selector", 'select[name$="_length"], select.dt-input')
                length_selector.send_keys("-1")
                time.sleep(2)
                logger.info("Set table length to show all entries.")
            except:
                pass
            html_content = page.page_source
            dfs = pd.read_html(StringIO(html_content))
            job_df = next((df for df in dfs if not df.empty and any('job' in str(col).lower() for col in df.columns)), pd.DataFrame())
            if job_df.empty:
                logger.warning(f"⚠️ No data table found on tab {tab_num}.")
                return pd.DataFrame()
            logger.info(f"📊 Found {len(job_df)} rows in tab {tab_num}.")
            return job_df
        except Exception as e:
            logger.error(f"❌ Failed to extract data from tab {tab_num}: {e}")
            page.save_screenshot(f"tab_{tab_num}_error.png")
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
    
    def _process_and_add_new_jobs(self, all_tab_data: Dict[int, pd.DataFrame]) -> int:
        """กรองเฉพาะ Job ใหม่และเพิ่มลงใน Master Sheet"""
        logger.info("Filtering for new jobs to add to the Master sheet...")
        existing_job_nos = self.sheet_manager.get_all_job_nos(self.config.MASTER_SHEET_NAME)
        
        new_records_to_add = []
        all_headers = set(['Job_No', 'Source_Tab', 'First_Seen'])

        for tab_num, df in all_tab_data.items():
            if df.empty:
                continue
            
            # เก็บ Headers ทั้งหมดเพื่อสร้างชีตให้สมบูรณ์
            for col in df.columns:
                all_headers.add(str(col))

            tab_name = self.config.TAB_NAMES.get(tab_num, f"Tab_{tab_num}")
            job_no_col = next((col for col in df.columns if 'job' in str(col).lower()), None)
            if not job_no_col:
                logger.warning(f"⚠️ No 'Job No.' column found in tab {tab_num}. Skipping.")
                continue

            for _, row in df.iterrows():
                job_no = str(row[job_no_col]).strip()
                if not job_no or job_no in existing_job_nos:
                    continue  # ข้ามถ้าไม่มี Job No หรือมีอยู่แล้ว
                
                # ถ้าเป็น Job ใหม่ ให้เตรียมข้อมูล
                new_record = {str(col): str(val) for col, val in row.items()}
                new_record['Job_No'] = job_no
                new_record['Source_Tab'] = tab_name
                new_record['First_Seen'] = datetime.now(timezone.utc).isoformat()
                
                new_records_to_add.append(new_record)
                existing_job_nos.add(job_no) # เพิ่มเข้าไปใน Set เพื่อป้องกันการเพิ่มซ้ำจากแท็บอื่นในรอบเดียวกัน
                self.notifier.send(f"🆕 New Job Found: {job_no} (from {tab_name})")

        if new_records_to_add:
            # ตรวจสอบและสร้าง Header ให้ Master Sheet หากยังไม่มี
            master_ws = self.sheet_manager.get_or_create_worksheet(self.config.MASTER_SHEET_NAME)
            if master_ws.row_count == 1 and master_ws.col_count == 1 and master_ws.cell(1,1).value is None:
                # Sheet is empty, write headers
                final_headers = sorted(list(all_headers))
                master_ws.update("A1", [final_headers])
            else:
                final_headers = master_ws.row_values(1)

            # แปลง dicts เป็น list of lists ตามลำดับ header
            rows_to_append = []
            for record in new_records_to_add:
                row = [record.get(h, "") for h in final_headers]
                rows_to_append.append(row)
            
            self.sheet_manager.append_rows(self.config.MASTER_SHEET_NAME, rows_to_append)

        return len(new_records_to_add)

    def run(self):
        """ฟังก์ชันหลักสำหรับรันกระบวนการทั้งหมด"""
        start_time = datetime.now()
        self.sheet_manager.log_activity("Sync Start", "Starting job synchronization process.")
        
        all_tab_data = {}
        successful_tabs, failed_tabs = [], []
    
        with sync_selenium() as p:
            browser = p.chrome.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            logged_in, page = self.scraper.login(context)
            if not logged_in:
                self.notifier.send("❌ Critical Error: Login to edoclite failed. Please check credentials.")
                self.sheet_manager.log_activity("Login Failed", "Could not log in.", "Failed")
                browser.close()
                return
    
            for tab in self.config.TABS_TO_SCRAPE:
                df = self.scraper.extract_data_from_tab(page, tab)
                if not df.empty:
                    all_tab_data[tab] = df
                    successful_tabs.append(tab)
                else:
                    failed_tabs.append(tab)
                import time
                time.sleep(1)
            
            browser.close()
        
        # ส่วนที่เหลือเหมือนเดิม...

        # ประมวลผลและเพิ่มข้อมูลใหม่เท่านั้น
        new_jobs_count = self._process_and_add_new_jobs(all_tab_data)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Log summary
        summary_details = f"Added {new_jobs_count} new jobs. Success Tabs: {len(successful_tabs)}. Failed Tabs: {len(failed_tabs)}."
        status = "Success" if not failed_tabs else "Partial Success"
        self.sheet_manager.log_activity("Sync Complete", summary_details, status)
        
        # Send final notification
        summary_msg = f"""
        ✅ Job Sync Complete!
        
        - 🆕 Found and Added: {new_jobs_count} new jobs
        - 🗂️ Total tabs scraped: {len(successful_tabs)}/{len(self.config.TABS_TO_SCRAPE)}
        - ⏱️ Duration: {duration:.2f} seconds
        
        🔗 Master Sheet: https://docs.google.com/spreadsheets/d/{self.config.GOOGLE_SHEET_ID}
        """.strip()
        self.notifier.send(summary_msg)
        logger.info(f"🎉 Process finished in {duration:.2f} seconds.")


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
        logger.error(f"💥 Configuration or Google Sheets Error: {e}")
        token = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
        if token:
            Notifier(token).send(f"💥 Job Sync Failed (Setup Error): {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 An unexpected critical error occurred: {e}", exc_info=True)
        token = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
        if token:
            Notifier(token).send(f"💥 Job Sync CRASHED: An unexpected error occurred. Check logs. {e}")
        sys.exit(1)
