# Run this script once as Administrator to register the Options Alert System scheduled task.
# Right-click this file -> "Run with PowerShell" (or open PowerShell as Admin and run it)

$taskName = "Options Alert System"
$batPath  = "C:\Users\Andrew Arnold\options_alert_system\start.bat"
$workDir  = "C:\Users\Andrew Arnold\options_alert_system"
$user     = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$action = New-ScheduledTaskAction `
    -Execute $batPath `
    -WorkingDirectory $workDir

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal `
    -UserId $user `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Options flow alert system - polls Polygon every 20 min during market hours, sends alerts, updates GitHub Pages dashboard." `
    -Force

Write-Host ""
Write-Host "Task '$taskName' registered successfully." -ForegroundColor Green
Write-Host "It will auto-start at next logon, or you can run it now:"
Write-Host "  schtasks /Run /TN '$taskName'"
Write-Host ""
Read-Host "Press Enter to close"
