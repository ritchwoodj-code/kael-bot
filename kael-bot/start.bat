@echo off
cd /d "%~dp0"

:: Remove stale lock file if bot crashed previously
if exist bot.lock (
    echo Removing stale bot.lock...
    del bot.lock
)

:: Start the bot
echo Starting Kael bot...
echo Press Ctrl+C in this window to stop.
echo.
python run_bot.py
pause
