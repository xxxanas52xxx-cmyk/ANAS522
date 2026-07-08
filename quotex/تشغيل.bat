@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo.
echo  ==========================================
echo     QUOTEX - LEADERS OF TRADING
echo  ==========================================
echo.
python --version > nul 2>&1 || (echo Python غير موجود && pause && exit /b 1)
echo [1/2] تثبيت المكتبات...
pip install httpx websockets certifi beautifulsoup4 fake-useragent rich pyfiglet flask flask-cors --quiet --disable-pip-version-check 2>nul
echo [2/2] تشغيل السيرفر...
echo.
echo  افتح المتصفح على: http://localhost:5000
echo.
python server.py
pause
