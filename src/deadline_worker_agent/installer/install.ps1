# Amazon Deadline Cloud Worker Agent Installer

# Parse command-line arguments 
param(
    [string]$FarmId,
    [string]$FleetId,
    [string]$Region = "us-west-2",
    [string]$User = "deadline-worker",
    [string]$Group = "deadline-job-users",
    [string]$WorkerAgentProgram,
    [switch]$NoInstallService = $false,
    [switch]$Start = $false,
    [switch]$Confirm = $false
)

# Defaults
$default_wa_user = "deadline-worker"
$default_job_group = "deadline-job-users"

function Usage {
    Write-Host "Usage: install.ps1 -FarmId <FarmId> -FleetId <FLEET_ID> [-Region <REGION>] [-User <USER>] [-Group <GROUP>] [-WorkerAgentProgram <WORKER_AGENT_PROGRAM>] [-NoInstallService] [-Start] [-Confirm]"
    Write-Host ""
    Write-Host "Arguments"
    Write-Host "---------"
    Write-Host "    -FarmId <FarmId>"
    Write-Host "        The Amazon Deadline Cloud Farm ID that the Worker belongs to."
    Write-Host "    -FleetId <FLEET_ID>"
    Write-Host "        The Amazon Deadline Cloud Fleet ID that the Worker belongs to."
    Write-Host "    -Region <REGION>"
    Write-Host "        The AWS region of the Amazon Deadline Cloud farm. Defaults to $Region."
    Write-Host "    -User <USER>"
    Write-Host "        A user name that the Amazon Deadline Cloud Worker Agent will run as. Defaults to $default_wa_user."
    Write-Host "    -Group <GROUP>"
    Write-Host "        A group name that the Worker Agent shares with the user(s) that Jobs will be running as."
    Write-Host "        Do not use the primary/effective group of the Worker Agent user specified in -User as"
    Write-Host "        this is not a secure configuration. Defaults to $default_job_group."
    Write-Host "    -WorkerAgentProgram <WORKER_AGENT_PROGRAM>"
    Write-Host "        An optional path to the Worker Agent program. This is used as the program path"
    Write-Host "        when creating the service. If not specified, the first program named"
    Write-Host "        deadline-worker-agent found in the PATH will be used."
    Write-Host "    -NoInstallService"
    Write-Host "        Skips the worker agent service installation."
    Write-Host "    -Start"
    Write-Host "        Starts the service as part of the installation. By default, the service"
    Write-Host "        is configured to start on system boot but not started immediately."
    Write-Host "        This option is ignored if -NoInstallService is used."
    Write-Host "    -Confirm"
    Write-Host "        Skips a confirmation prompt before performing the installation."
    exit 2
}

function Banner {
    Write-Host "==========================================================="
    Write-Host "|      Amazon Deadline Cloud Worker Agent Installer       |"
    Write-Host "==========================================================="
}

function UserExists {
    param([string]$User)
    $UserObject = Get-WmiObject -Class Win32_UserAccount -Filter "Name='$User'"
    return $UserObject -ne $null
}

function GroupExists {
    param([string]$group)
    $groupObject = Get-WmiObject -Class Win32_Group -Filter "Name='$group'"
    return $groupObject -ne $null
}

function ValidateDeadlineId {
    param([string]$prefix, [string]$text)
    $pattern = "^$prefix-[a-f0-9]{32}$"
    return $text -match $pattern
}

function Set-DirectoryPermissions {
    param (
        [string]$Path,
        [string]$User,
        [string]$Permission
    )

    # Get the current ACL of the directory
    $acl = Get-Acl -Path $Path

    # Remove existing inheritance (if any) this makes the permissions predictable
    $acl.SetAccessRuleProtection($true, $false)

    # Create a new rule for the specified user and permission level
    $userRule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        $User, $Permission, "ContainerInherit,ObjectInherit", "None", "Allow"
    )

    # Create a rule for administrators with Full Control
       $adminRule = New-Object System.Security.AccessControl.FileSystemAccessRule(
        "Administrators", $Permission, "ContainerInherit,ObjectInherit", "None", "Allow"
    )

    # Add the rules to the ACL
    $acl.AddAccessRule($adminRule)
    $acl.SetAccessRule($userRule)

    # Apply the modified ACL to the directory
    Set-Acl -Path $Path -AclObject $acl
}


# Validate required command-line arguments
if (-not $FarmId) {
    Write-Host "ERROR: -FarmId not specified"
    usage
}
elseif (-not (ValidateDeadlineId -prefix "farm" -text $FarmId)) {
    Write-Host "ERROR: Not a valid value for -FarmId: ${FarmId}"
    usage
}

if (-not $FleetId) {
    Write-Host "ERROR: -FleetId not specified"
    usage
}
elseif (-not (ValidateDeadlineId -prefix "fleet" -text $FleetId)) {
    Write-Host "ERROR: Not a valid value for -FleetId: ${FleetId}"
    usage
}

if (-not $WorkerAgentProgram) {
    $WorkerAgentProgram=(Get-Command deadline-worker-agent -ErrorAction SilentlyContinue).Path

    if (-not $WorkerAgentProgram) {
        Write-Host "ERROR: Could not find deadline-worker-agent in search path"
        exit 1
    }
}
elseif (-not (Test-Path -Path $WorkerAgentProgram -PathType Leaf)) {
    Write-Host "ERROR: The specified Worker Agent path is not found: ${worker_agent_program}"
    usage
}


# Output configuration
Banner
Write-Host ""
Write-Host "Farm ID: $FarmId"
Write-Host "Fleet ID: $FleetId"
Write-Host "Region: $Region"
Write-Host "Worker agent user: $User"
Write-Host "Worker job group: $Group"
Write-Host "Worker agent program path: $WorkerAgentProgram"
Write-Host "Start service: $Start"

# Confirmation prompt
if (!$Confirm) {
    while ($true) {
        $choice = Read-Host "Confirm install with the above settings (y/n):"
        if ($choice -eq "y") {
            $Confirm = $true
            break
        }
        elseif ($choice -eq "n") {
            Write-Host "Installation aborted"
            exit 1
        }
        else {
            Write-Host "Not a valid choice ($choice). Please try again."
        }
    }
}

Write-Host ""

# Check if the worker agent user exists, and create it if not
if (!(UserExists $User)) {
    Write-Host "Creating worker agent user ($User)"
    $null = New-LocalUser -Name $User -NoPassword # -UserMayNotChangePassword -PasswordNeverExpires -AccountNeverExpires
    Write-Host "Done creating worker agent user ($User)"
}
else {
    Write-Host "Worker agent user $User already exists"
}

# Check if the job group exists, and create it if not
if (!(GroupExists $Group)) {
    Write-Host "Creating job group ($Group)"
    $null = New-LocalGroup -Name $Group
    Write-Host "Done creating job group ($Group)"
}
else {
    Write-Host "Job group $Group already exists"
}

# Add the worker agent user to the job group
$groupMembers = Get-LocalGroupMember -Group $Group -ErrorAction SilentlyContinue
if ($groupMembers -eq $null -or ($groupMembers | Where-Object { $_.Name -eq $User }) -eq $null) {
    # User is not a member of the group, so add them
    Add-LocalGroupMember -Group $Group -Member $User -ErrorAction SilentlyContinue
    Write-Host "User $User added to group $Group."
} else {
    Write-Host "User $User is already a member of group $Group."
}


$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Provision directories
$deadLineDirectory = "C:\ProgramData\Amazon\Deadline"
Write-Host "Provisioning root directory ($deadlineDirectory)"
$null = New-Item -Path $deadLineDirectory -ItemType Directory -Force
# Setting the permissions correctly on this directory will set them correctly on everything 
# within it
Set-DirectoryPermissions -Path $deadLineDirectory -User $User -Permission FullControl
Write-Host "Done provisioning root directory ($deadlineDirectory)"

$deadlineLogSubDirectory = Join-Path $deadLineDirectory "Logs"
Write-Host "Provisioning log directory ($deadlineLogSubDirectory)"
$null = New-Item -Path $deadlineLogSubDirectory -ItemType Directory -Force
Write-Host "Done provisioning log directory ($deadlineLogSubDirectory)"

$deadlinePersistenceSubDirectory = Join-Path $deadLineDirectory "Cache"
Write-Host "Provisioning persistence directory ($deadlinePersistenceSubDirectory)"
$null = New-Item -Path $deadlinePersistenceSubDirectory -ItemType Directory -Force
Write-Host "Done provisioning persistence directory ($deadlinePersistenceSubDirectory)"

$deadlineConfigSubDirectory = Join-Path $deadLineDirectory "Config"
Write-Host "Provisioning config directory ($deadlineConfigSubDirectory)"
$null = New-Item -Path $deadlineConfigSubDirectory -ItemType Directory -Force
Write-Host "Done provisioning config directory ($deadlineConfigSubDirectory)"


Write-Host "Configuring farm and fleet"
$workerConfigFile = Join-Path $deadlineConfigSubDirectory "worker.toml"
if (-not (Test-Path -Path $$workerConfigFile -PathType Leaf)) {
    Copy-Item -Path "$ScriptDir/worker.toml.windows.example" -Destination $workerConfigFile
}
$backupWorkerConfig = "$workerConfigFile.bak"
Copy-Item -Path $workerConfigFile -Destination $backupWorkerConfig
$content = Get-Content -Path $workerConfigFile
$content = $content -replace '^# farm_id\s*=\s*("REPLACE-WTIH-WORKER-FARM-ID")$', "farm_id = `"$FarmId`""
$content = $content -replace '^# fleet_id\s*=\s*("REPLACE-WITH-WORKER-FLEET-ID")$', "fleet_id = `"$FleetId`""
$content | Set-Content -Path $workerConfigFile
Write-Host "Done configuring farm and fleet"


if (!$no_install_service) {
    # Set up the service
    Write-Host "Installing Windows service"
    $deadlineServiceName = "DeadlineCloudWorkerAgent"
    $deadlineServiceDisplayName = "Amazon Deadline Cloud Worker Agent"
    $deadlineServiceDescription = "Amazon Deadline Cloud Worker Agent"
    $deadlineServiceExecutable = $WorkerAgentProgram
    $credentials = New-Object System.Management.Automation.PSCredential ($User, (New-Object System.Security.SecureString))
                   
    # Check if the service exists
    if (-not (Get-Service -Name $deadlineServiceName -ErrorAction SilentlyContinue)) {
        # Service does not exist, so create it
        New-Service -Name $deadlineServiceName -Credential $credentials -DisplayName $deadlineServiceDisplayName -Description $deadlineServiceDescription -BinaryPathName $deadlineServiceExecutable -StartupType "Automatic"
        Write-Host "Service created."
    } else {
        Write-Host "Service already exists."
    }
                       
    Write-Host "Done installing Windows service"

    # Start the service
    if ($Start) {
        Write-Host "Starting the service"
        Start-Service -Name $deadlineServiceName
        Write-Host "Done starting the service"
    }
}

Write-Host "Done"