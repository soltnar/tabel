@echo off
setlocal
cd /d "%~dp0"
title Tabel - local setup and start

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0windows_setup.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Setup or startup failed. See the message above.
  pause
)

exit /b %EXIT_CODE%
