import os
import json
import asyncio
import threading
from datetime import datetime, timezone
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging

# Import our main scraper
from main_master_only import JobSyncApplication, Config, GoogleSheetManager, Notifier

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your-secret-key-change-this')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables to track scraping status
scraping_status = {
    'is_running': False,
    'last_run': None,
    'last_result': None,
    'progress': '',
    'logs': []
}

def add_log(message):
    """Add log message with timestamp"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    scraping_status['logs'].append(f"[{timestamp}] {message}")
    if len(scraping_status['logs']) > 50:  # Keep only last 50 logs
        scraping_status['logs'] = scraping_status['logs'][-50:]
    logger.info(message)

def run_scraping_sync():
    """Run scraping in sync context"""
    global scraping_status
    try:
        scraping_status['is_running'] = True
        scraping_status['progress'] = 'กำลังเริ่มต้น...'
        add_log('🚀 Starting job synchronization...')
        
        app_config = Config()
        app_instance = JobSyncApplication(app_config)
        
        scraping_status['progress'] = 'กำลังดำเนินการ...'
        app_instance.run()
        
        scraping_status['last_result'] = 'สำเร็จ'
        scraping_status['progress'] = 'เสร็จสิ้น'
        add_log('✅ Job synchronization completed successfully!')
        
    except Exception as e:
        scraping_status['last_result'] = f'ข้อผิดพลาด: {str(e)}'
        scraping_status['progress'] = 'เกิดข้อผิดพลาด'
        add_log(f'❌ Error during synchronization: {str(e)}')
    finally:
        scraping_status['is_running'] = False
        scraping_status['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def run_scraping_thread():
    """Run scraping in a separate thread"""
    run_scraping_sync()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html', status=scraping_status)

@app.route('/settings')
def settings():
    """Settings configuration page"""
    # Get current environment variables for display
    config = {
        'EDOCLITE_USER': os.environ.get('EDOCLITE_USER', ''),
        'GOOGLE_SHEET_ID': os.environ.get('GOOGLE_SHEET_ID', ''),
        'LINE_NOTIFY_TOKEN': os.environ.get('LINE_NOTIFY_TOKEN', ''),
        'has_google_creds': bool(os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON_B64') or 
                                os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON'))
    }
    return render_template('settings.html', config=config)

@app.route('/data')
def view_data():
    """View scraped data from Google Sheets"""
    try:
        config = Config()
        sheet_manager = GoogleSheetManager(
            config.GOOGLE_SHEET_ID,
            config.GOOGLE_SVC_JSON_RAW,
            config.GOOGLE_SVC_JSON_B64
        )
        
        # Get data from Master sheet
        ws = sheet_manager.get_or_create_worksheet(config.MASTER_SHEET_NAME)
        data = ws.get_all_records()
        
        # Get recent 100 records
        recent_data = data[-100:] if len(data) > 100 else data
        
        return render_template('data.html', 
                             data=recent_data, 
                             total_count=len(data),
                             sheet_url=f"https://docs.google.com/spreadsheets/d/{config.GOOGLE_SHEET_ID}")
    except Exception as e:
        add_log(f'Error fetching data: {str(e)}')
        return render_template('data.html', 
                             data=[], 
                             error=str(e),
                             total_count=0)

@app.route('/api/start-scraping', methods=['POST'])
def start_scraping():
    """API endpoint to start scraping"""
    global scraping_status
    
    if scraping_status['is_running']:
        return jsonify({'success': False, 'message': 'กำลังดำเนินการอยู่แล้ว'})
    
    try:
        # Start scraping in a separate thread
        scraping_thread = threading.Thread(target=run_scraping_thread)
        scraping_thread.daemon = True
        scraping_thread.start()
        
        add_log('🎯 Scraping process initiated by user')
        return jsonify({'success': True, 'message': 'เริ่มการกวาดข้อมูลแล้ว'})
    except Exception as e:
        add_log(f'Failed to start scraping: {str(e)}')
        return jsonify({'success': False, 'message': f'ไม่สามารถเริ่มได้: {str(e)}'})

@app.route('/api/status')
def get_status():
    """API endpoint to get current scraping status"""
    return jsonify(scraping_status)

@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Test Google Sheets and LINE Notify connections"""
    try:
        config = Config()
        results = {}
        
        # Test Google Sheets
        try:
            sheet_manager = GoogleSheetManager(
                config.GOOGLE_SHEET_ID,
                config.GOOGLE_SVC_JSON_RAW,
                config.GOOGLE_SVC_JSON_B64
            )
            results['google_sheets'] = {'status': 'success', 'message': 'เชื่อมต่อ Google Sheets สำเร็จ'}
        except Exception as e:
            results['google_sheets'] = {'status': 'error', 'message': f'Google Sheets Error: {str(e)}'}
        
        # Test LINE Notify
        try:
            notifier = Notifier(config.LINE_NOTIFY_TOKEN)
            success = notifier.send('🔔 ทดสอบการเชื่อมต่อ LINE Notify สำเร็จ')
            if success:
                results['line_notify'] = {'status': 'success', 'message': 'ส่ง LINE Notify สำเร็จ'}
            else:
                results['line_notify'] = {'status': 'error', 'message': 'LINE Notify ส่งไม่สำเร็จ'}
        except Exception as e:
            results['line_notify'] = {'status': 'error', 'message': f'LINE Notify Error: {str(e)}'}
        
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/logs')
def view_logs():
    """View application logs"""
    return render_template('logs.html', logs=scraping_status['logs'])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
