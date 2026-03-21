@echo off
:: ============================================================
:: Registers run_pipeline.bat as a Windows Task Scheduler job
:: Runs daily at 6:00 AM. Run this script ONCE as Administrator.
:: ============================================================

set TASK_NAME=MarketIntelligencePipeline
set SCRIPT_PATH=%~dp0run_pipeline.bat
set SCHEDULE_TIME=06:00

echo Registering Task Scheduler job...
echo   Task : %TASK_NAME%
echo   File : %SCRIPT_PATH%
echo   Time : %SCHEDULE_TIME% daily

schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%SCRIPT_PATH%\"" ^
    /sc DAILY ^
    /st %SCHEDULE_TIME% ^
    /f ^
    /rl HIGHEST ^
    /ru "%USERNAME%"

if errorlevel 1 (
    echo.
    echo ERROR: Failed to register task. Try running this script as Administrator.
    pause
    exit /b 1
)

echo.
echo Task registered successfully!
echo.
echo To verify:   schtasks /query /tn "%TASK_NAME%"
echo To run now:  schtasks /run /tn "%TASK_NAME%"
echo To remove:   schtasks /delete /tn "%TASK_NAME%" /f
echo.
pause
