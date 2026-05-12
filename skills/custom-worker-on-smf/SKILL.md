---
name: custom-worker-on-smf
description: >
  Deploy a custom Worker Agent build to a Service Managed Fleet (SMF) using
  Host Configuration Scripts. Covers Linux (bash) and Windows (PowerShell)
  installation from Git. Use when asked to "test worker agent on SMF",
  "deploy custom worker agent", "install worker agent from git on fleet",
  "host config script for worker agent", or when testing unreleased worker
  agent changes on a real fleet.
tags: [deadline-cloud, worker-agent, smf, fleet, host-config, deploy, testing, linux, windows]
---

# Deploy Custom Worker Agent on SMF

> **⚠️ WARNING:** Replacing the Worker Agent on SMF is strictly for **testing purposes**
> and is **not supported**. Do not use this in production. AWS support cannot assist
> with issues arising from custom agent installations on SMF fleets.

## Overview

Service Managed Fleets (SMF) ship with a released Worker Agent version. To test
unreleased changes, use **Host Configuration Scripts** to replace the agent at
worker boot time. The script installs a custom build, optionally enables feature
flags, marks the host as configured, and reboots into a clean worker state.

**Background**: SMF fleets are secure and don't allow easy customization. CMF fleet
setup has known challenges and doesn't mimic the SMF environment 1:1. Host
Configuration Scripts provide administrative access to SMF workers, letting you
install a custom agent, enable feature flags, and reboot into a clean state.

## How It Works

1. On the Fleet page → **Configurations** → create a **Worker configuration script**
2. Paste the appropriate script (Linux or Windows) below
3. Host Config runs only on worker start
4. After first boot: installs custom agent → (optionally sets feature flags) → reboots
5. After reboot: detects marker file → skips install → starts processing jobs

### Verification

Worker logs should show:
- `Running Host Configuration Script`
- `Host already Rebooted, ready to start`
- `Worker Agent host configuration succeeded. Starting worker session loop.`

**Where to find logs**: CloudWatch Logs in the fleet's account:
- Log group: `/aws/deadline/<farmId>/<fleetId>`
- Log stream: `<workerId>`

**Check logs via CLI:**

```bash
# List log streams (wait ~2-3 min after setting minWorkerCount for worker to appear)
aws logs describe-log-streams \
  --log-group-name "/aws/deadline/<farm-id>/<fleet-id>" \
  --order-by LastEventTime --descending \
  --region <region> \
  --query 'logStreams[*].logStreamName' --output json

# Read worker logs
aws logs get-log-events \
  --log-group-name "/aws/deadline/<farm-id>/<fleet-id>" \
  --log-stream-name "<worker-id>" \
  --region <region> \
  --limit 100 \
  --query 'events[*].message' --output text
```

**What to look for in the logs:**
1. First boot: pip install output, `Marker file created, Rebooting worker host.`
2. After reboot: `Host already Rebooted, ready to start.`, exit code 0
3. Agent info: verify `agent.version` and `dependencies.openjd.sessions` match expected versions
4. Worker polling: `UpdateWorkerSchedule` returning 200 means the worker is ready for jobs

---

## Fleet Configuration Steps

> **Note:** All `aws deadline` commands require `--endpoint-url https://deadline.<region>.amazonaws.com`.
> The default endpoint resolution may not work in all environments.

### Step 1: Fleet — Create or Update

Ask the user whether to:
- **Update an existing fleet** — provide the fleet ID
- **Create a new fleet** — provide fleet name, instance type, OS, etc.

Also ask the user for the **installation source** (Git URL):
- Default: `git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git`
- With branch: `git+https://github.com/<user>/deadline-cloud-worker-agent.git@<branch>`
- With commit: `git+https://github.com/<user>/deadline-cloud-worker-agent.git@<sha>`

Optionally ask if they also need to install custom versions of dependencies:
- `git+https://github.com/aws-deadline/deadline-cloud-job-attachments.git@<branch>`
- `git+https://github.com/OpenJobDescription/openjd-sessions-for-python.git@<branch>`

Use the answers to fill in the `pip install` lines in the host config script template
(Linux or Windows section below) before setting it on the fleet.

#### Creating a new fleet

```bash
# Write the host config script to a temp file first, then:
SCRIPT_BODY=$(cat host-config.sh)

aws deadline create-fleet \
  --farm-id <farm-id> \
  --display-name "<fleet-name>" \
  --role-arn "<fleet-role-arn>" \
  --min-worker-count 0 \
  --max-worker-count 1 \
  --configuration '{
    "serviceManagedEc2": {
      "instanceCapabilities": {
        "vCpuCount": {"min": 2, "max": 2},
        "memoryMiB": {"min": 8192, "max": 8192},
        "osFamily": "LINUX",
        "cpuArchitectureType": "x86_64",
        "allowedInstanceTypes": ["m5.large"]
      },
      "instanceMarketOptions": {"type": "on-demand"}
    }
  }' \
  --host-configuration "{\"scriptBody\": $(echo "$SCRIPT_BODY" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), \"scriptTimeoutSeconds\": 600}" \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

> **Important:** Fleet creation is async. The fleet starts in `CREATE_IN_PROGRESS`
> and you cannot update it until it reaches `ACTIVE`. Poll with:
> ```bash
> aws deadline get-fleet --farm-id <farm-id> --fleet-id <fleet-id> \
>   --query 'status' --output text \
>   --region <region> --endpoint-url https://deadline.<region>.amazonaws.com
> ```
> This typically takes 1–2 minutes.

Required inputs:
- `farm-id` — the farm to create the fleet in
- `display-name` — human-readable fleet name
- `role-arn` — IAM role workers assume (e.g. `AWSDeadlineCloudFleetRole-*`)
- `osFamily` — `LINUX` or `WINDOWS`
- `allowedInstanceTypes` — e.g. `["m5.large"]`
- `instanceMarketOptions.type` — `on-demand` or `spot`
- Host config script body (from the Linux or Windows templates below)

#### Updating an existing fleet's host configuration

```bash
SCRIPT_BODY=$(cat host-config.sh)

aws deadline update-fleet \
  --farm-id <farm-id> \
  --fleet-id <fleet-id> \
  --host-configuration "{\"scriptBody\": $(echo "$SCRIPT_BODY" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), \"scriptTimeoutSeconds\": 600}" \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

To **remove** a host configuration script:

```bash
aws deadline update-fleet \
  --farm-id <farm-id> \
  --fleet-id <fleet-id> \
  --host-configuration '{"scriptBody": ""}' \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

**Host configuration parameters:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `scriptBody` | Full text of the script. Runs as `sudo root` (Linux) or Administrator (Windows). | — |
| `scriptTimeoutSeconds` | Max runtime before worker enters NOT_RESPONDING and shuts down. You are charged for this time. | 300 (5 min) |

> **Tip:** Set `--max-worker-count 1` on the fleet while testing your host config script to avoid spinning up multiple workers with a broken script.

### Step 2: Queue — Associate or Create

After the fleet is created/updated, it must be associated with a queue to receive jobs.

Ask the user whether to:
- **Use an existing queue** — provide the queue ID
- **Create a new queue** — provide queue name and optionally a job run-as user

#### Check existing associations

```bash
# List all fleet associations for a queue
aws deadline list-queue-fleet-associations \
  --farm-id <farm-id> \
  --queue-id <queue-id> \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

If the fleet is already associated and status is `ACTIVE`, no action needed.

#### Create a new queue (if needed)

```bash
aws deadline create-queue \
  --farm-id <farm-id> \
  --display-name "<queue-name>" \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

#### Associate fleet with queue

```bash
aws deadline create-queue-fleet-association \
  --farm-id <farm-id> \
  --queue-id <queue-id> \
  --fleet-id <fleet-id> \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

#### Stop scheduling on unwanted fleets

If multiple fleets are associated with the queue and you want jobs to only go to
your test fleet:

```bash
aws deadline update-queue-fleet-association \
  --farm-id <farm-id> \
  --queue-id <queue-id> \
  --fleet-id <unwanted-fleet-id> \
  --status STOP_SCHEDULING_AND_COMPLETE_TASKS \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

### Step 3: Trigger a worker and verify

Either set `minWorkerCount` to 1 to force a worker to spin up, or submit a job to
the associated queue (the fleet will auto-scale a worker to pick it up).

**Option A: Set min workers (fleet must be ACTIVE first):**

```bash
aws deadline update-fleet \
  --farm-id <farm-id> \
  --fleet-id <fleet-id> \
  --min-worker-count 1 \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

**Option B: Submit a job to the queue:**

```bash
aws deadline create-job \
  --farm-id <farm-id> \
  --queue-id <queue-id> \
  --template '{"specificationVersion": "jobtemplate-2023-09", "name": "HostConfigTest", "steps": [{"name": "Echo", "script": {"actions": {"onRun": {"command": "echo", "args": ["hello"]}}}}]}' \
  --template-type JSON \
  --priority 50 \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

Then wait ~2-3 minutes and check CloudWatch logs (see Verification section above).

After confirming the host config works, set `minWorkerCount` back to 0 if you don't
want to keep a worker running (you'll be charged for idle time):

```bash
aws deadline update-fleet \
  --farm-id <farm-id> \
  --fleet-id <fleet-id> \
  --min-worker-count 0 \
  --region <region> \
  --endpoint-url https://deadline.<region>.amazonaws.com
```

---

## Linux: Installing from Git

Paste this as the Worker configuration script for Linux fleets:

```bash
# This makes the script echo every command, to help track down issues
set -xeuo pipefail

echo "Running Host Configuration script to update worker agent and reboot this worker." \
     "If already rebooted, start processing steps." \
     "Otherwise, reboot"
ls /var/lib/deadline
if [ -f /var/lib/deadline/rebooted ]; then
    echo "Host already Rebooted, ready to start."
exit 0
fi
echo "Host has not been rebooted."

###
# UNCOMMENT BELOW IF NEEDED FOR DEBUGGING
###
# find /opt/deadline/worker
# ls -al /opt/deadline
# ls -al /opt/deadline/worker
# ls -al /opt/deadline/worker/lib
# ls -al /opt/deadline/worker/lib/python3.11
# whoami

# Remove the previous worker agent installation
mv /opt/deadline/worker /opt/deadline/worker-old

# Create a new virtual environment in its place
python -m venv /opt/deadline/worker
source /opt/deadline/worker/bin/activate

###
# INSTALLATION — change the Git URL/branch as needed:
###
pip install git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git
# To pin a branch:  git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git@my-branch
# To pin a commit:  git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git@abc1234

###
# OPTIONAL: Install custom versions of dependencies if testing dependency changes
###
# pip install git+https://github.com/aws-deadline/deadline-cloud.git@my-branch
# pip install git+https://github.com/aws-deadline/deadline-cloud-job-attachments.git@my-branch
# pip install git+https://github.com/OpenJobDescription/openjd-sessions-for-python.git@my-branch

# Ensure the deadline-worker user can run the agent
chmod -R go+rx /opt/deadline/worker

# List the package and version info to help with diagnosing issues
echo "The Deadline worker packages:"
pip list

###
# OPTIONAL: Enable feature flags via systemd environment
# Uncomment and edit the lines below if you need to set environment variables
# for the deadline-worker service (e.g. feature flags).
###
# echo "Configuring deadline-worker service..."
# echo "[Service]" >> /etc/systemd/system/deadline-worker.service.d/config.conf
# echo 'Environment="MY_FEATURE_FLAG=True"' >> /etc/systemd/system/deadline-worker.service.d/config.conf

# Caution When changing this file or path.
# If the file cannot be created properly a worker can be stuck in a rebooting loop.
touch /var/lib/deadline/rebooted

# Optional chmod, use 660 if jobs need to check if this host is configured.
chmod 660 /var/lib/deadline/rebooted
echo "Marker file created, Rebooting worker host."

sudo reboot now
sleep 60
exit 0
```

### Customization Points (Linux)

| Variable / Line | What to change |
|-----------------|----------------|
| `pip install git+...worker-agent...` | Change URL to your fork/branch/commit |
| `pip install git+...deadline-cloud.git` | Uncomment to install a custom `deadline-cloud` library |
| `pip install git+...job-attachments.git` | Uncomment to install a custom `deadline-cloud-job-attachments` |
| `pip install git+...openjd-sessions...` | Uncomment to install a custom `openjd-sessions-for-python` |
| Feature flags block | Uncomment and add `Environment="FLAG=Value"` lines as needed |

---

## Windows: Installing from Git

Paste this as the Worker configuration script for Windows fleets:

```powershell
Write-Host "Checking if rebooted file exists"
if (Test-Path "C:/rebooted") { exit 0 }

Write-Host "Pip path: $((Get-Command pip).Source)"

###
# INSTALLATION — change the Git URL/branch as needed:
###
pip install git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git
# To pin a branch:  git+https://github.com/aws-deadline/deadline-cloud-worker-agent.git@my-branch

###
# OPTIONAL: Install custom versions of dependencies if testing dependency changes
###
# pip install git+https://github.com/aws-deadline/deadline-cloud.git@my-branch
# pip install git+https://github.com/aws-deadline/deadline-cloud-job-attachments.git@my-branch
# pip install git+https://github.com/OpenJobDescription/openjd-sessions-for-python.git@my-branch

###
# OPTIONAL: Set environment variables (feature flags) via Windows Registry
# Uncomment and edit the Python block below if you need to configure
# environment variables for the DeadlineWorker service.
###
# # Verify current registry values
# Get-ItemProperty -Path 'HKLM:\\SYSTEM\\CurrentControlSet\\Services\\DeadlineWorker' -Name 'Environment'
#
# $pythonScript = @"
# import winreg
#
# def set_registry_values():
#     values = [
#         'MY_FEATURE_FLAG=True',
#         'AWS_DEFAULT_REGION=us-west-2',
#     ]
#
#     try:
#         key_path = r'SYSTEM\CurrentControlSet\Services\DeadlineWorker'
#         key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path, 0, winreg.KEY_ALL_ACCESS)
#         winreg.SetValueEx(key, 'Environment', 0, winreg.REG_MULTI_SZ, values)
#         winreg.CloseKey(key)
#         print('Registry values set successfully by Python.')
#     except WindowsError as e:
#         print(f'Error setting registry values in Python: {e}')
#
# set_registry_values()
# "@
#
# python -c $pythonScript
#
# # Verify registry values after update
# Get-ItemProperty -Path 'HKLM:\\SYSTEM\\CurrentControlSet\\Services\\DeadlineWorker' -Name 'Environment'

Write-Host "Creating rebooted file"
New-Item "C:/rebooted" -ItemType File
Write-Host "Restarting computer"
Restart-Computer -Force
Write-Host "Sleeping 60 seconds"
Start-Sleep 60
```

### Customization Points (Windows)

| Variable / Line | What to change |
|-----------------|----------------|
| `pip install git+...worker-agent...` | Change URL to your fork/branch |
| `pip install git+...deadline-cloud.git` | Uncomment to install a custom `deadline-cloud` library |
| `pip install git+...job-attachments.git` | Uncomment to install a custom `deadline-cloud-job-attachments` |
| `pip install git+...openjd-sessions...` | Uncomment to install a custom `openjd-sessions-for-python` |
| Registry `values` list | Uncomment the Python block and add `'FLAG=Value'` entries as needed |
| `'AWS_DEFAULT_REGION=us-west-2'` | Change region if your fleet is elsewhere |

---

## Key Differences: Linux vs Windows

| Aspect | Linux | Windows |
|--------|-------|---------|
| Marker file | `/var/lib/deadline/rebooted` | `C:/rebooted` |
| Venv recreation | Yes — `mv` old, `python -m venv` new | No — installs into existing pip environment |
| Feature flags (optional) | systemd env file (`config.conf`) | Windows Registry (`HKLM:\SYSTEM\CurrentControlSet\Services\DeadlineWorker`) |
| Reboot command | `sudo reboot now` | `Restart-Computer -Force` |
| Permissions fix | `chmod -R go+rx /opt/deadline/worker` | Not needed |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Worker stuck in reboot loop | Marker file not created (script error before `touch`) | Check script for syntax errors; ensure `/var/lib/deadline/` exists |
| Agent starts but uses old version | Venv not recreated properly | Verify `mv` succeeded and new venv was activated |
| Feature flag not active | Env var not in systemd config (Linux) or registry (Windows) | Check `/etc/systemd/system/deadline-worker.service.d/config.conf` or registry path |
| pip install fails | Network/DNS issue on worker | Ensure fleet has internet access to reach GitHub |
| Permission denied on Linux | `chmod` step skipped | Re-run `chmod -R go+rx /opt/deadline/worker` |

---

## References

| Item | Link |
|------|------|
| Feature flags source | [feature_flag.py](https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/mainline/src/deadline_worker_agent/feature_flag.py) |
| Worker Agent releases | [GitHub releases](https://github.com/aws-deadline/deadline-cloud-worker-agent/releases) |
