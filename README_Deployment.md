# Job Scraper Web UI - คู่มือการ Deploy บน Render.com

## 📋 ภาพรวมของโปรเจ็กต์

โปรเจ็กต์นี้แปลง Python script เดิมที่รันใน GitHub Actions ให้เป็น Web Application ที่มี UI ทันสมัยและสามารถควบคุมการกวาดข้อมูลผ่านเว็บได้

### ✨ คุณสมบัติหลัก

- **Dashboard ทันสมัย**: ควบคุมการกวาดข้อมูลด้วย UI ที่สวยงาม
- **Real-time Status**: ติดตามสถานะการทำงานแบบเรียลไทม์
- **Data Viewer**: ดูข้อมูลที่กวาดได้จาก Google Sheets
- **Settings Management**: จัดการการตั้งค่าต่างๆ
- **Live Logs**: ดู logs การทำงานแบบเรียลไทม์
- **Auto Refresh**: รีเฟรชข้อมูลอัตโนมัติ
- **Mobile Responsive**: ใช้งานได้ดีบนมือถือ

## 🏗️ โครงสร้างไฟล์

```
job-scraper-webapp/
├── app.py                     # Flask main application
├── main_master_only.py        # Original scraper code (เดิม)
├── requirements.txt           # Python dependencies
├── render.yaml               # Render.com config
├── Dockerfile                # Docker config (ทางเลือก)
├── templates/                # HTML templates
│   ├── dashboard.html        # หน้าหลัก Dashboard
│   ├── data.html            # หน้าแสดงข้อมูล
│   ├── settings.html        # หน้าตั้งค่า
│   └── logs.html            # หน้าดู Logs
└── README_Deployment.md      # คู่มือนี้
```

## 🚀 วิธีการ Deploy บน Render.com

### ขั้นตอนที่ 1: เตรียม Repository

1. สร้าง GitHub repository ใหม่
2. อัปโหลดไฟล์ทั้งหมดลง repository
3. ตรวจสอบให้แน่ใจว่าไฟล์ `render.yaml` อยู่ใน root directory

### ขั้นตอนที่ 2: สร้าง Web Service บน Render

1. เข้าไปที่ [Render.com](https://render.com)
2. สร้างบัญชีหรือเข้าสู่ระบบ
3. คลิก "New +" → "Web Service"
4. เชื่อมต่อ GitHub repository ของคุณ
5. เลือก repository ที่สร้างไว้
6. ตั้งค่าดังนี้:
   - **Name**: `job-scraper-webapp` (หรือชื่อที่ต้องการ)
   - **Environment**: `Python`
   - **Build Command**: (จะอ่านจาก render.yaml อัตโนมัติ)
   - **Start Command**: (จะอ่านจาก render.yaml อัตโนมัติ)

### ขั้นตอนที่ 3: ตั้งค่า Environment Variables

ไปที่ Settings → Environment ของ Web Service และเพิ่มตัวแปรเหล่านี้:

#### 🔐 ข้อมูลเข้าสู่ระบบ Edoclite
```
EDOCLITE_USER=your_username
EDOCLITE_PASS=your_password
```

#### 📊 Google Sheets Configuration
```
GOOGLE_SHEET_ID=your_google_sheet_id
GOOGLE_SERVICE_ACCOUNT_JSON_B64=your_base64_encoded_service_account_json
```

#### 📱 LINE Notify (ไม่บังคับ)
```
LINE_NOTIFY_TOKEN=your_line_notify_token
```

#### 🔒 Flask Security
```
FLASK_SECRET_KEY=your_random_secret_key
```

### ขั้นตอนที่ 4: Deploy

1. คลิก "Create Web Service"
2. รอการ build และ deploy (ประมาณ 5-10 นาที)
3. เมื่อเสร็จแล้วจะได้ URL สำหรับเข้าใช้งาน

## ⚙️ การตั้งค่า Environment Variables แบบละเอียด

### 1. GOOGLE_SERVICE_ACCOUNT_JSON_B64

วิธีการสร้าง:
1. ไปที่ [Google Cloud Console](https://console.cloud.google.com/)
2. สร้าง Service Account
3. ดาวน์โหลด JSON key file
4. แปลงเป็น Base64:
   ```bash
   # บน Linux/Mac
   base64 -i service_account.json
   
   # บน Windows (PowerShell)
   [Convert]::ToBase64String([IO.File]::ReadAllBytes("service_account.json"))
   ```
5. คัดลอกผลลัพธ์ไปใส่ในตัวแปร

### 2. GOOGLE_SHEET_ID

วิธีหา Sheet ID:
- จาก URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`
- คัดลอกส่วน `{SHEET_ID}` มาใช้

### 3. LINE_NOTIFY_TOKEN

วิธีการสร้าง:
1. ไปที่ [LINE Notify](https://notify-bot.line.me/)
2. เข้าสู่ระบบด้วย LINE
3. "Generate token"
4. เลือกกลุ่มที่ต้องการรับการแจ้งเตือน
5. คัดลอก token ที่ได้

## 🔧 การใช้งาน Web UI

### Dashboard (หน้าหลัก)
- **เริ่มกวาดข้อมูล**: คลิกปุ่มเพื่อเริ่มการทำงาน
- **ทดสอบการเชื่อมต่อ**: ตรวจสอบการเชื่อมต่อ Google Sheets และ LINE
- **ดูสถานะ**: ติดตามการทำงานแบบเรียลไทม์

### Data Viewer
- แสดงข้อมูลล่าสุดจาก Google Sheets
- กรองและค้นหาข้อมูล
- ลิงก์ไปยัง Google Sheets

### Settings
- ดูสถานะการตั้งค่า
- คำแนะนำการตั้งค่า Environment Variables
- ลิงก์ไปยังเครื่องมือต่างๆ

### Logs
- ดู logs การทำงานแบบเรียลไทม์
- Auto refresh logs
- กรอง logs ตามประเภท

## 🎯 ข้อดีของ Render.com

### ✅ ข้อดี
- **ฟรี**: แผน Free tier ที่ใจดี
- **Auto Deploy**: Deploy อัตโนมัติจาก Git
- **HTTPS**: SSL certificate ฟรี
- **Custom Domain**: รองรับ domain ของตัวเอง
- **Environment Variables**: จัดการง่าย
- **Logs**: ดู logs แบบเรียลไทม์
- **เสถียร**: uptime ดีกว่า Streamlit

### ⚠️ ข้อจำกัด
- **Sleep Mode**: แผนฟรีจะ sleep หลัง 15 นาทีไม่มีใครใช้
- **Startup Time**: ใช้เวลา 10-30 วินาทีในการ wake up
- **Resource Limit**: RAM และ CPU จำกัด

## 🔍 การแก้ไขปัญหา

### ปัญหาที่พบบ่อย

1. **Build Failed**
   - ตรวจสอบ `requirements.txt`
   - ดู build logs ใน Render dashboard

2. **Environment Variables ไม่ทำงาน**
   - ตรวจสอบชื่อตัวแปรให้ถูกต้อง
   - ตรวจสอบ Base64 encoding

3. **Google Sheets ไม่เชื่อมต่อได้**
   - ตรวจสอบ Service Account permissions
   - แชร์ Google Sheet ให้กับ Service Account

4. **แอป Sleep บ่อย**
   - ใช้ UptimeRobot หรือ service อื่นมา ping
   - อัปเกรดเป็น paid plan

### การ Monitor และ Debug

1. **Render Logs**: ดู logs ใน Render dashboard
2. **Web UI Logs**: ใช้หน้า Logs ใน web app
3. **Health Check**: ตรวจสอบ `/` endpoint

## 🚨 Security Best Practices

1. **ไม่เก็บ credentials ใน code**
2. **ใช้ Environment Variables เท่านั้น**
3. **ตั้ง FLASK_SECRET_KEY ที่แข็งแกร่ง**
4. **อัปเดต dependencies เป็นประจำ**
5. **จำกัด permissions ของ Service Account**

## 🔄 การอัปเดตแอป

1. Push code ใหม่ไปยัง GitHub
2. Render จะ auto-deploy อัตโนมัติ
3. ตรวจสอบใน Render dashboard

## 🎉 การใช้งานครั้งแรก

1. เข้าไปที่ URL ที่ Render ให้มา
2. ไปที่หน้า Settings ตรวจสอบการตั้งค่า
3. คลิก "ทดสอบการเชื่อมต่อ"
4. กลับไป Dashboard คลิก "เริ่มกวาดข้อมูล"
5. ติดตามผลใน Logs

## 📞 การสนับสนุน

หากมีปัญหาหรือต้องการความช่วยเหลือ:
1. ตรวจสอบ logs ใน Render dashboard
2. ดูเอกสารของ Render.com
3. ตรวจสอบ GitHub issues

---

**หมายเหตุ**: โปรเจ็กต์นี้แปลงจาก GitHub Actions script เดิมให้เป็น Web Application ที่ใช้งานง่ายขึ้นและมี UI ที่ทันสมัย พร้อมทั้งรองรับการใช้งานแบบ 24/7 บน Render.com
