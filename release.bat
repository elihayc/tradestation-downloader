@echo off
setlocal enabledelayedexpansion

:: TradeStation Downloader Release Script
:: Usage: release.bat <version> <pypi_token>
:: Example: release.bat 1.0.7 pypi-xxxxxxxxxxxx

:: Check arguments
if "%~1"=="" (
    echo Usage: release.bat ^<version^> ^<pypi_token^>
    echo Example: release.bat 1.0.7 pypi-xxxxxxxxxxxx
    exit /b 1
)

if "%~2"=="" (
    echo Usage: release.bat ^<version^> ^<pypi_token^>
    echo Example: release.bat 1.0.7 pypi-xxxxxxxxxxxx
    exit /b 1
)

set VERSION=%~1
set PYPI_TOKEN=%~2

echo.
echo ========================================
echo  Releasing version %VERSION%
echo ========================================
echo.

:: Validate PyPI token format
echo %PYPI_TOKEN% | findstr /b "pypi-" >nul
if errorlevel 1 (
    echo ERROR: PyPI token must start with 'pypi-'
    exit /b 1
)

:: Check for uncommitted changes
git diff --quiet
if errorlevel 1 (
    echo ERROR: You have uncommitted changes. Please commit or stash them first.
    exit /b 1
)

:: Step 1: Update version in pyproject.toml
echo [1/8] Updating version in pyproject.toml...
powershell -Command "(Get-Content 'pyproject.toml') -replace 'version = \"[0-9]+\.[0-9]+\.[0-9]+\"', 'version = \"%VERSION%\"' | Set-Content 'pyproject.toml'"
if errorlevel 1 (
    echo ERROR: Failed to update pyproject.toml
    exit /b 1
)

:: Step 2: Update version in tradestation/__init__.py
echo [2/8] Updating version in tradestation/__init__.py...
powershell -Command "(Get-Content 'tradestation\__init__.py') -replace '__version__ = \"[0-9]+\.[0-9]+\.[0-9]+\"', '__version__ = \"%VERSION%\"' | Set-Content 'tradestation\__init__.py'"
if errorlevel 1 (
    echo ERROR: Failed to update tradestation/__init__.py
    exit /b 1
)

:: Step 3: Update lock file
echo [3/8] Updating lock file with uv sync...
uv sync --extra dev
if errorlevel 1 (
    echo ERROR: Failed to run uv sync
    exit /b 1
)

:: Step 4: Commit changes
echo [4/8] Committing version bump...
git add pyproject.toml tradestation/__init__.py uv.lock
git commit -m "Bump version to %VERSION%"
if errorlevel 1 (
    echo ERROR: Failed to commit changes
    exit /b 1
)

:: Step 5: Create git tag
echo [5/8] Creating git tag v%VERSION%...
git tag v%VERSION%
if errorlevel 1 (
    echo ERROR: Failed to create tag
    exit /b 1
)

:: Step 6: Push to remote
echo [6/8] Pushing to remote...
git push && git push --tags
if errorlevel 1 (
    echo ERROR: Failed to push to remote
    exit /b 1
)

:: Step 7: Clean and build
echo [7/8] Building package...
if exist dist\ (
    rmdir /s /q dist
)
.venv\Scripts\python.exe -m build
if errorlevel 1 (
    echo ERROR: Failed to build package
    exit /b 1
)

:: Step 8: Upload to PyPI
echo.
echo Ready to upload to PyPI.
set /p CONFIRM="Are you sure you want to upload version %VERSION% to PyPI? (y/N): "
if /i not "%CONFIRM%"=="y" (
    echo Upload cancelled by user.
    echo Note: Version has been committed and tagged but NOT uploaded to PyPI.
    echo To upload manually, run: .venv\Scripts\python.exe -m twine upload dist/* -u __token__ -p YOUR_TOKEN
    exit /b 0
)

echo [8/8] Uploading to PyPI...
.venv\Scripts\python.exe -m twine upload dist/* -u __token__ -p %PYPI_TOKEN%
if errorlevel 1 (
    echo ERROR: Failed to upload to PyPI
    exit /b 1
)

echo.
echo ========================================
echo  Successfully released version %VERSION%
echo ========================================
echo.
echo Verify at: https://pypi.org/project/tradestation-downloader/
echo.

endlocal
