@echo off
:: ============================================================
:: Market Intelligence Daily Pipeline
:: Runs all scrapers, consolidates CSVs, uploads to Google Drive
:: Scheduled via Windows Task Scheduler at 6:00 AM IST daily
:: ============================================================

set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

:: Log file (overwrites daily)
set LOGFILE=%PROJECT_DIR%logs\pipeline_%date:~-4,4%%date:~-10,2%%date:~-7,2%.log

:: Create logs dir if missing
if not exist "%PROJECT_DIR%logs" mkdir "%PROJECT_DIR%logs"

echo ============================================================ >> "%LOGFILE%"
echo Pipeline started: %date% %time% >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"

:: Detect python
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: python not found in PATH >> "%LOGFILE%"
    exit /b 1
)

:: ── Blinkit ──────────────────────────────────────────────────
echo [1/6] Blinkit... >> "%LOGFILE%"
python blinkit\blinkit_sales_estimator.py ^
    --keywords "chips,protein powder,cold drinks,snacks,dairy" ^
    --all-locations >> "%LOGFILE%" 2>&1
echo Blinkit exit code: %errorlevel% >> "%LOGFILE%"

:: ── Myntra ───────────────────────────────────────────────────
echo [2/6] Myntra... >> "%LOGFILE%"
python myntra\myntra_sales_estimator.py ^
    --keywords "tshirts,sneakers,kurta,jeans,shoes" ^
    --pages 3 ^
    --out-dir data >> "%LOGFILE%" 2>&1
echo Myntra exit code: %errorlevel% >> "%LOGFILE%"

:: ── Amazon ───────────────────────────────────────────────────
echo [3/6] Amazon... >> "%LOGFILE%"
python amazon\amazon_scraper.py ^
    --keywords "protein powder,running shoes,electronics" ^
    --top 30 ^
    --out-dir data >> "%LOGFILE%" 2>&1
echo Amazon exit code: %errorlevel% >> "%LOGFILE%"

:: ── Flipkart ─────────────────────────────────────────────────
echo [4/6] Flipkart... >> "%LOGFILE%"
python flipkart\flipkart_scraper.py ^
    --keywords "protein powder,running shoes,electronics" ^
    --top 30 ^
    --out-dir data >> "%LOGFILE%" 2>&1
echo Flipkart exit code: %errorlevel% >> "%LOGFILE%"

:: ── Consolidate ──────────────────────────────────────────────
echo [5/6] Consolidating CSVs... >> "%LOGFILE%"
python pipeline\consolidate.py --data-dir data --out-dir data >> "%LOGFILE%" 2>&1
echo Consolidate exit code: %errorlevel% >> "%LOGFILE%"

:: ── Upload to Google Drive ────────────────────────────────────
echo [6/6] Uploading to Google Drive... >> "%LOGFILE%"
python pipeline\gdrive_sync.py --upload --data-dir data >> "%LOGFILE%" 2>&1
echo Drive upload exit code: %errorlevel% >> "%LOGFILE%"

echo ============================================================ >> "%LOGFILE%"
echo Pipeline finished: %date% %time% >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"
