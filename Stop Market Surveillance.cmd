@echo off
setlocal
powershell -ExecutionPolicy Bypass -File "%~dp0shared\scripts\stop_stack_and_tunnel.ps1"
endlocal
