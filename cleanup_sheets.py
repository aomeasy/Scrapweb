# สคริปต์ทำความสะอาดข้อมูลใน Google Sheets (รันแยกต่างหาก)
# สร้างไฟล์ cleanup_sheets.py

import os
import gspread
from google.oauth2.service_account import Credentials
import json
import base64

def cleanup_duplicate_job_columns():
    """ลบคอลัมน์ Job No. ที่ซ้ำออกจาก Google Sheets"""
    
    # เชื่อมต่อ Google Sheets
    try:
        # ใช้ credentials จาก environment
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64"):
            info = json.loads(base64.b64decode(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")).decode("utf-8"))
        else:
            info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
        
        creds = Credentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
        client = gspread.authorize(creds)
        
        # เปิด spreadsheet
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet("Master_Data")
        
        print("📊 Analyzing current sheet structure...")
        
        # ดึง headers ปัจจุบัน
        headers = worksheet.row_values(1)
        print(f"Current headers: {headers}")
        
        # หาคอลัมน์ที่ซ้ำกัน
        job_columns = []
        for idx, header in enumerate(headers):
            if 'job' in str(header).lower() and 'no' in str(header).lower():
                job_columns.append((idx + 1, header))  # 1-based index
        
        print(f"Found Job columns: {job_columns}")
        
        if len(job_columns) > 1:
            print("🔄 Removing duplicate Job columns...")
            
            # เก็บเฉพาะคอลัมน์ Job_No (หรือคอลัมน์แรกถ้าไม่มี Job_No)
            keep_column = None
            remove_columns = []
            
            for col_idx, col_name in job_columns:
                if col_name == 'Job_No':
                    keep_column = (col_idx, col_name)
                else:
                    remove_columns.append((col_idx, col_name))
            
            # ถ้าไม่มี Job_No ให้เก็บคอลัมน์แรก
            if keep_column is None:
                keep_column = job_columns[0]
                remove_columns = job_columns[1:]
            
            print(f"Keeping column: {keep_column[1]} at position {keep_column[0]}")
            print(f"Removing columns: {[col[1] for col in remove_columns]}")
            
            # ลบคอลัมน์ที่ซ้ำ (เรียงจากขวาไปซ้ายเพื่อไม่ให้ index เปลี่ยน)
            for col_idx, col_name in sorted(remove_columns, reverse=True):
                print(f"Deleting column {col_name} at position {col_idx}")
                worksheet.delete_columns(col_idx)
            
            print("✅ Cleanup completed!")
            
            # แสดง headers ใหม่
            new_headers = worksheet.row_values(1)
            print(f"New headers: {new_headers}")
            
        else:
            print("✅ No duplicate columns found!")
            
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

if __name__ == "__main__":
    cleanup_duplicate_job_columns()
