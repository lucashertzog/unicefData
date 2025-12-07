# ============================================================================
# regenerate_metadata.ps1 - Regenerate all YAML metadata files
# ============================================================================
#
# This script regenerates metadata for all three languages:
# - Python: Uses schema_sync.py
# - R: Uses schema_sync.R
# - Stata: Uses unicefdata_sync command
#
# Usage:
#   .\tests\regenerate_metadata.ps1 [-Python] [-R] [-Stata] [-All] [-Verbose]
#
# Examples:
#   .\tests\regenerate_metadata.ps1 -All          # Regenerate all
#   .\tests\regenerate_metadata.ps1 -Python       # Python only
#   .\tests\regenerate_metadata.ps1 -Stata        # Stata only
#
# ============================================================================

param(
    [switch]$Python,
    [switch]$R,
    [switch]$Stata,
    [switch]$All,
    [switch]$Verbose
)

# Default to All if no specific language selected
if (-not $Python -and -not $R -and -not $Stata) {
    $All = $true
}

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $RepoRoot) {
    $RepoRoot = "D:\jazevedo\GitHub\unicefData"
}

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " unicefData Metadata Regeneration Script" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Repository: $RepoRoot"
Write-Host ""

# ----------------------------------------------------------------------------
# Python Metadata Regeneration
# ----------------------------------------------------------------------------
function Regenerate-PythonMetadata {
    Write-Host ""
    Write-Host "[Python] Regenerating metadata..." -ForegroundColor Yellow
    Write-Host "----------------------------------------"
    
    $pythonDir = Join-Path $RepoRoot "python"
    $syncScript = Join-Path $pythonDir "unicef_api\run_sync.py"
    
    if (-not (Test-Path $syncScript)) {
        Write-Host "[Python] ERROR: sync script not found at $syncScript" -ForegroundColor Red
        return $false
    }
    
    Push-Location $pythonDir
    try {
        Write-Host "[Python] Running schema_sync..."
        python -m unicef_api.run_sync
        $exitCode = $LASTEXITCODE
        
        if ($exitCode -eq 0) {
            $metadataDir = Join-Path $pythonDir "metadata\current"
            $fileCount = (Get-ChildItem -Path $metadataDir -Filter "*.yaml" -ErrorAction SilentlyContinue).Count
            Write-Host "[Python] SUCCESS: Generated $fileCount metadata files" -ForegroundColor Green
            return $true
        } else {
            Write-Host "[Python] ERROR: sync failed with exit code $exitCode" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "[Python] ERROR: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
    finally {
        Pop-Location
    }
}

# ----------------------------------------------------------------------------
# R Metadata Regeneration
# ----------------------------------------------------------------------------
function Regenerate-RMetadata {
    Write-Host ""
    Write-Host "[R] Regenerating metadata..." -ForegroundColor Yellow
    Write-Host "----------------------------------------"
    
    $rDir = Join-Path $RepoRoot "R"
    $syncScript = Join-Path $rDir "schema_sync.R"
    
    if (-not (Test-Path $syncScript)) {
        Write-Host "[R] ERROR: sync script not found at $syncScript" -ForegroundColor Red
        return $false
    }
    
    Push-Location $rDir
    try {
        Write-Host "[R] Running schema_sync.R..."
        # Run R script
        $rCode = @"
setwd('$($rDir -replace '\\', '/')')
source('schema_sync.R')
sync_dataflow_schemas()
"@
        $rCode | Rscript --vanilla -
        $exitCode = $LASTEXITCODE
        
        if ($exitCode -eq 0) {
            $metadataDir = Join-Path $rDir "metadata\current"
            $fileCount = (Get-ChildItem -Path $metadataDir -Filter "*.yaml" -ErrorAction SilentlyContinue).Count
            Write-Host "[R] SUCCESS: Generated $fileCount metadata files" -ForegroundColor Green
            return $true
        } else {
            Write-Host "[R] ERROR: sync failed with exit code $exitCode" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "[R] ERROR: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
    finally {
        Pop-Location
    }
}

# ----------------------------------------------------------------------------
# Stata Metadata Regeneration
# ----------------------------------------------------------------------------
function Regenerate-StataMetadata {
    Write-Host ""
    Write-Host "[Stata] Regenerating metadata..." -ForegroundColor Yellow
    Write-Host "----------------------------------------"
    
    $stataDir = Join-Path $RepoRoot "stata"
    $adoPath = Join-Path $stataDir "src\u\unicefdata_sync.ado"
    
    if (-not (Test-Path $adoPath)) {
        Write-Host "[Stata] ERROR: unicefdata_sync.ado not found at $adoPath" -ForegroundColor Red
        return $false
    }
    
    # Create a temporary do-file to run the sync
    $tempDo = Join-Path $env:TEMP "unicefdata_sync_test.do"
    
    $doContent = @"
// Temporary do-file for metadata regeneration
clear all
set more off

// Add the package to adopath
adopath ++ "$($stataDir -replace '\\', '/')/src/u"
adopath ++ "$($stataDir -replace '\\', '/')/src/_"

// Show adopath for debugging
adopath

// Run sync with verbose output
unicefdata_sync, verbose

// Exit
exit, clear STATA
"@
    
    Set-Content -Path $tempDo -Value $doContent
    
    try {
        Write-Host "[Stata] Running unicefdata_sync..."
        Write-Host "[Stata] (This may take a few minutes to fetch from SDMX API)"
        
        # Try to find Stata executable
        $stataExe = $null
        $stataPaths = @(
            "C:\Program Files\Stata18\StataMP-64.exe",
            "C:\Program Files\Stata17\StataMP-64.exe",
            "C:\Program Files\Stata18\StataSE-64.exe",
            "C:\Program Files\Stata17\StataSE-64.exe",
            "C:\Program Files\Stata18\Stata-64.exe",
            "C:\Program Files\Stata17\Stata-64.exe"
        )
        
        foreach ($path in $stataPaths) {
            if (Test-Path $path) {
                $stataExe = $path
                break
            }
        }
        
        if (-not $stataExe) {
            Write-Host "[Stata] WARNING: Stata executable not found in standard locations" -ForegroundColor Yellow
            Write-Host "[Stata] Please run manually in Stata:" -ForegroundColor Yellow
            Write-Host "        adopath ++ `"$stataDir\src\u`"" -ForegroundColor Gray
            Write-Host "        adopath ++ `"$stataDir\src\_`"" -ForegroundColor Gray
            Write-Host "        unicefdata_sync, verbose" -ForegroundColor Gray
            return $false
        }
        
        Write-Host "[Stata] Using: $stataExe"
        & $stataExe /e do "$tempDo"
        $exitCode = $LASTEXITCODE
        
        if ($exitCode -eq 0) {
            $metadataDir = Join-Path $stataDir "metadata\current"
            $fileCount = (Get-ChildItem -Path $metadataDir -Filter "*.yaml" -ErrorAction SilentlyContinue).Count
            Write-Host "[Stata] SUCCESS: Generated $fileCount metadata files" -ForegroundColor Green
            return $true
        } else {
            Write-Host "[Stata] ERROR: sync failed with exit code $exitCode" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "[Stata] ERROR: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
    finally {
        if (Test-Path $tempDo) {
            Remove-Item $tempDo -ErrorAction SilentlyContinue
        }
    }
}

# ----------------------------------------------------------------------------
# Main Execution
# ----------------------------------------------------------------------------

$results = @{
    Python = $null
    R = $null
    Stata = $null
}

if ($All -or $Python) {
    $results.Python = Regenerate-PythonMetadata
}

if ($All -or $R) {
    $results.R = Regenerate-RMetadata
}

if ($All -or $Stata) {
    $results.Stata = Regenerate-StataMetadata
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Summary" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($lang in @("Python", "R", "Stata")) {
    $status = $results[$lang]
    if ($status -eq $null) {
        Write-Host "  $lang : SKIPPED" -ForegroundColor Gray
    } elseif ($status) {
        Write-Host "  $lang : PASSED" -ForegroundColor Green
    } else {
        Write-Host "  $lang : FAILED" -ForegroundColor Red
    }
}

Write-Host ""

# Return success if all executed languages passed
$allPassed = $true
foreach ($result in $results.Values) {
    if ($result -eq $false) {
        $allPassed = $false
        break
    }
}

if ($allPassed) {
    Write-Host "All metadata regeneration completed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Some regeneration tasks failed. Check output above." -ForegroundColor Red
    exit 1
}
