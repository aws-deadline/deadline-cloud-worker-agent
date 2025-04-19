param(
    [string]$ScriptToRun,
    [string]$LogFile,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$RemainingArgs
)

$OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Set environment variables from remaining arguments
foreach ($envVar in $RemainingArgs) {
    if ($envVar -match '^(.+?)=(.*)$') {
        $key = $matches[1]
        $value = $matches[2]
        if ($value -eq "NULL") {
            Set-Item -Path "env:$key" -Value $null
        } else {
            Set-Item -Path "env:$key" -Value $value
        }
    }
}

# The actual script execution and logging
. $ScriptToRun *>&1 | Out-File -FilePath $LogFile -Encoding utf8

$exitCode = $LASTEXITCODE
if ($null -eq $exitCode) { $exitCode = if ($?) { 0 } else { 1 } }
exit $exitCode