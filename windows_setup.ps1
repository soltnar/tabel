$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$HostAddress = "127.0.0.1"
$Port = 8000
$AppUrl = "http://localhost:$Port"
$VenvDir = Join-Path $PSScriptRoot ".venv-windows"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

function Test-Python311 {
    param([string]$Executable)

    if (-not $Executable -or -not (Test-Path -LiteralPath $Executable)) {
        return $false
    }

    & $Executable -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)" 2>$null
    return ($LASTEXITCODE -eq 0)
}

function Find-Python311 {
    $LocalPython = Join-Path $env:LocalAppData "Programs\Python\Python311\python.exe"
    if (Test-Python311 $LocalPython) {
        return $LocalPython
    }

    $PyLauncher = Get-Command "py.exe" -ErrorAction SilentlyContinue
    if ($PyLauncher) {
        $Detected = & $PyLauncher.Source -3.11 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0 -and (Test-Python311 $Detected)) {
            return [string]$Detected
        }
    }

    $PythonCommand = Get-Command "python.exe" -ErrorAction SilentlyContinue
    if ($PythonCommand -and (Test-Python311 $PythonCommand.Source)) {
        return $PythonCommand.Source
    }

    return $null
}

function Install-Python311 {
    Write-Host "Python 3.11 was not found."
    Write-Host "Installing it automatically for the current Windows user..."

    $Winget = Get-Command "winget.exe" -ErrorAction SilentlyContinue
    if ($Winget) {
        Write-Host "Trying Windows Package Manager..."
        & $Winget.Source install --id Python.Python.3.11 -e --source winget --scope user --silent --accept-package-agreements --accept-source-agreements | Out-Host
        $Detected = Find-Python311
        if ($Detected) {
            return $Detected
        }
        Write-Host "Winget did not finish the installation. Using python.org..."
    }

    $PythonVersion = "3.11.9"
    switch ($env:PROCESSOR_ARCHITECTURE.ToUpperInvariant()) {
        "ARM64" { $InstallerName = "python-$PythonVersion-arm64.exe" }
        "X86" { $InstallerName = "python-$PythonVersion.exe" }
        default { $InstallerName = "python-$PythonVersion-amd64.exe" }
    }

    $InstallerPath = Join-Path $env:TEMP $InstallerName
    $InstallerUrl = "https://www.python.org/ftp/python/$PythonVersion/$InstallerName"

    Write-Host "Downloading the official signed Python installer..."
    Invoke-WebRequest -UseBasicParsing -Uri $InstallerUrl -OutFile $InstallerPath

    $Signature = Get-AuthenticodeSignature -FilePath $InstallerPath
    if ($Signature.Status -ne "Valid") {
        throw "The downloaded Python installer has no valid digital signature."
    }

    Write-Host "Installing Python 3.11..."
    $InstallProcess = Start-Process -FilePath $InstallerPath -Wait -PassThru -ArgumentList @(
        "/quiet",
        "InstallAllUsers=0",
        "PrependPath=1",
        "Include_launcher=1",
        "Include_test=0",
        "Shortcuts=0"
    )
    Remove-Item -LiteralPath $InstallerPath -Force -ErrorAction SilentlyContinue

    if ($InstallProcess.ExitCode -ne 0) {
        throw "Python installer returned code $($InstallProcess.ExitCode)."
    }

    $Detected = Find-Python311
    if (-not $Detected) {
        throw "Python 3.11 was installed but could not be found."
    }
    return $Detected
}

try {
    Write-Host ""
    Write-Host "Employee schedule generator"
    Write-Host "==========================="

    $Python = Find-Python311
    if (-not $Python) {
        $Python = Install-Python311
    }
    Write-Host "Python: $Python"

    if (-not (Test-Path -LiteralPath $VenvPython)) {
        Write-Host "Creating the private application environment..."
        & $Python -m venv $VenvDir
        if ($LASTEXITCODE -ne 0) {
            throw "Could not create the Python environment."
        }
    }

    & $VenvPython -c "import fastapi,uvicorn,pandas,openpyxl,multipart,holidays,reportlab,xlrd" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing application libraries. This is required only once..."
        & $VenvPython -m pip install --disable-pip-version-check --default-timeout=120 --retries 10 -r (Join-Path $PSScriptRoot "requirements.txt")
        if ($LASTEXITCODE -ne 0) {
            throw "Could not install the application libraries. Check the internet connection."
        }
    } else {
        Write-Host "Application libraries are already installed."
    }

    Write-Host ""
    Write-Host "Starting: $AppUrl"
    Write-Host "Close this window or press Ctrl+C to stop the application."

    $BrowserCommand = "Start-Sleep -Seconds 3; Start-Process '$AppUrl'"
    Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -ArgumentList @(
        "-NoLogo",
        "-NoProfile",
        "-Command",
        $BrowserCommand
    )

    & $VenvPython -m uvicorn app.main:app --host $HostAddress --port $Port
    exit $LASTEXITCODE
}
catch {
    Write-Host ""
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Internet is required during the first setup."
    Write-Host "If installation is blocked, ask the Windows administrator to allow START_WINDOWS.bat."
    exit 1
}
