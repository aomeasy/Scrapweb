# app.py

import os
import threading
import queue
from flask import Flask, render_template, jsonify, request

# สมมติว่าโค้ด scraper ของคุณอยู่ในไฟล์ scraper_app.py และมีคลาส JobSyncApplication
from scraper_app import JobSyncApplication, Config 

app = Flask(__name__)

# สร้างตัวแปร Global เพื่อเก็บสถานะและ Log
app_status = {
    "is_running": False,
    "logs": []
}
log_queue = queue.Queue()

def run_scraper_task():
    """ฟังก์ชันที่จะรันใน background thread"""
    app_status["is_running"] = True
    app_status["logs"] = ["🚀 [START] เริ่มกระบวนการกวาดข้อมูล..."]
    
    try:
        # ---- ส่วนนี้คือการเรียกใช้โค้d Scraper เดิมของคุณ ----
        app_config = Config()
        scraper = JobSyncApplication(app_config) 
        # คุณอาจจะต้องปรับคลาส scraper ให้รับ queue เข้าไปเพื่อส่ง log กลับมา
        asyncio.run(scraper.run()) 
        # ---------------------------------------------------
        
        app_status["logs"].append("✅ [SUCCESS] กระบวนการเสร็จสิ้น!")
    except Exception as e:
        app_status["logs"].append(f"❌ [ERROR] เกิดข้อผิดพลาด: {e}")
    finally:
        app_status["is_running"] = False

@app.route("/")
def index():
    """Render หน้าเว็บหลัก"""
    # ดึงค่า Sheet ID จาก Environment Variable มาแสดงผล
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "N/A")
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}" if sheet_id != "N/A" else "#"
    return render_template("index.html", sheet_url=sheet_url)

@app.route("/run-scraper", methods=["POST"])
def trigger_scraper():
    """Endpoint ที่รับคำสั่งให้เริ่มทำงาน"""
    if app_status["is_running"]:
        return jsonify({"status": "error", "message": "Scraper กำลังทำงานอยู่แล้ว"}), 400

    # เริ่มรัน scraper ใน Thread แยกเพื่อไม่ให้หน้าเว็บค้าง
    thread = threading.Thread(target=run_scraper_task)
    thread.start()
    
    return jsonify({"status": "success", "message": "Scraper เริ่มทำงานแล้ว"})

@app.route("/status")
def get_status():
    """Endpoint ให้หน้าเว็บมาถามสถานะล่าสุด"""
    return jsonify(app_status)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
