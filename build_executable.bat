@echo off
echo ========================================
echo Teams Chat Converter - Build Script
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

echo Installing dependencies...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install pyinstaller

echo.
echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist TeamsChartConverter.spec del TeamsChartConverter.spec

echo.
echo Building executable...
python -m PyInstaller --name=TeamsChartConverter --onefile --windowed --add-data="teams_chat_converter.py;." --hidden-import=bs4 --hidden-import=openpyxl --clean --noconfirm teams_chat_converter_gui.py

if errorlevel 1 (
    echo.
    echo ERROR: Build failed!
    pause
    exit /b 1
)

echo.
echo ========================================
echo BUILD COMPLETE!
echo ========================================
echo.
echo Executable location: dist\TeamsChartConverter.exe
echo.
echo You can now run: dist\TeamsChartConverter.exe
echo.
pause
