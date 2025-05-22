# Development Environment Setup

This guide provides instructions for setting up development environments for the AWS Deadline Cloud Worker Agent on both Amazon Linux 2023 (AL2023) and Windows Server 2022 EC2 instances using AWS Systems Manager (SSM) Session Manager for secure access.

![Development Environment Setup](images/dev_environment_diagram.drawio.svg)  
<small>*Diagram can be edited using draw.io*</small>

## Prerequisites

Before setting up your development environment, ensure you have:

*   AWS account access with permissions to:
    *   create and manage EC2 instances, a security group, and a keypair
    *   use SSM session manager
    *   create and manage an S3 bucket 
    *   create IAM roles and policies
    *   create and manage Deadline Cloud resources
*   A GitHub account
*   A fork of the `deadline-cloud-worker-agent` repository.
*   Basic familiarity with Git, Python, bash, and powershell
*   AWS CLI v2 installed and configured on your local machine
*   Session Manager plugin for AWS CLI installed (see [installation instructions](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html))

## Creating an EC2 Key Pair

The development environment scripts use a conventional key pair name: `deadline-worker-agent-dev-keypair`. You must create this key pair in your AWS account before using the scripts:

1.  Navigate to the EC2 console in the AWS Management Console
2.  In the left navigation pane, under "Network & Security", click "Key Pairs"
3.  Click "Create key pair"
4.  Enter the name `deadline-worker-agent-dev-keypair` (this exact name is required)
5.  Select the key pair type (RSA is recommended)
6.  Select the private key file format (.pem for OpenSSH, .ppk for PuTTY)
7.  Click "Create key pair"
8.  The private key file will be automatically downloaded to your computer
9.  Move the private key file to your SSH directory:
    ```bash
    # For Linux/macOS
    mv ~/Downloads/deadline-worker-agent-dev-keypair.pem ~/.ssh/
    chmod 400 ~/.ssh/deadline-worker-agent-dev-keypair.pem
    
    # For Windows (PowerShell)
    Move-Item -Path "$env:USERPROFILE\Downloads\deadline-worker-agent-dev-keypair.pem" -Destination "$env:USERPROFILE\.ssh\"
    ```

>   **Important**: The private key file is downloaded only once when you create the key pair. Store it securely and don't lose it, as you won't be able to retrieve the Windows administrator password without it.

## Setting Up an AL2023 Development Environment

### 1. Launch an AL2023 EC2 Instance with SSM

1.  Navigate to the EC2 console in the AWS Management Console
2.  Click "Launch Instance"
3.  Configure your instance with the following settings:
    -   Name: `deadline-worker-dev-linux`
    -   AMI: Amazon Linux 2023 (AL2023)
    -   Instance type: Recommend `t3.medium` or larger
    -   Key pair: Select `deadline-worker-agent-dev-keypair` (required)
    -   Network settings:
        -   VPC: Select your VPC. You can use the default VPC that comes with your AWS account
        -   Auto-assign public IP: Enabled
        -   Security group: No inbound traffic required. Allow only necessary outbound traffic
    -   Storage:
        -   Size: At least 20 GB
        -   Volume type: gp3
        -   Encyption recommended. You can use the "(default) aws/ebs" key or use an alternate KMS key.
    -   Advanced details:
        -   IAM instance profile: Select or create a role with
            -   `AmazonSSMManagedInstanceCore` AWS managed policy
            -   `AWSDeadlineCloud-WorkerHost` AWS managed policy
4.  Launch the instance

### 2. Connect to Your Instance Using SSH Over SSM Session Manager

You can connect to your Linux EC2 instance using a SSH through an SSM port forward tunnel.
To do so, create an SSH config entry in your `~/.ssh/config` file:

```
Host deadline-agent-linux-dev
    HostName i-instanceid
    User ec2-user
    Port 8022
    IdentitiesOnly yes
    IdentityFile ~/.ssh/deadline-worker-agent-dev-keypair.pem
    ProxyCommand aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p
```

To connect with SSH, run:

```bash
ssh deadline-agent-linux-dev
```

You can also transfer files **from your local machine** using the format:

```bash
# Copy from local machine to remote EC2 instance
scp local_file_path deadline-agent-linux-dev:remote_file_path

# Copy from remote EC2 instance to local machine
scp deadline-agent-linux-dev:remote_file_path local_file_path
```

### 3. Install Dependencies

**When connected remotely to the instance**, update the system and install required dependencies:

```bash
# Update the system
sudo dnf update -y

# Install development tools and dependencies
sudo dnf install -y git python3-pip python3-devel gcc make

# Install Hatch
sudo pip install pipx
pipx install hatch
```

### 4. Clone the Repository and Install Hatch

**When connected remotely to the instance**, clone the AWS Deadline Cloud Worker Agent repository:

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/aws-deadline/deadline-cloud-worker-agent.git
cd deadline-cloud-worker-agent

# Add your fork as a remote. Replace <USERNAME> with your GitHub alias
git remote add personal git@github.com:<USERNAME>/deadline-cloud-worker-agent.git
git fetch personal
```

### 5. Setup a Git Remote on Local Workstation

**From your local workstation**, setup a remote that points to the Linux instance's git repository.
From the local checkout of the worker agent, run:

```bash
git remote add linux-dev-instance deadline-agent-linux-dev:projects/deadline-cloud-worker-agent
```

Now fetch from the remote to ensure the remote and SSH setup are correct:

```bash
git fetch linux-dev-instance
```

If everything is setup correct, you should see the output:

```
From deadline-agent-linux-dev:projects/deadline-cloud-worker-agent
 * [new branch]      mainline   -> deadline-agent-linux-dev/mainline
```

We now have a setup for easily synchronizing git branches between your local workstation and the remote Linux EC2 instance.

## Setting Up a Windows Server 2022 Development Environment

### 1. Launch a Windows Server 2022 EC2 Instance with SSM

1.  Navigate to the EC2 console in the AWS Management Console
2.  Click "Launch Instance"
3.  Configure your instance with the following settings:
    -   Name: `deadline-worker-dev-windows`
    -   AMI: Windows Server 2022 Base
    -   Instance type: Recommend `t3.large` or larger
    -   Key pair: Select `deadline-worker-agent-dev-keypair` (required)
    -   Network settings:
        -   VPC: Select your VPC
        -   Auto-assign public IP: Enabled
        -   Security group: No inbound traffic required. Allow only necessary outbound traffic
    - Storage:
        -   Size: At least 50 GB
        -   Volume type: gp3
        -   Encyption recommended. You can use the "(default) aws/ebs" key or use an alternate KMS key.
    -   Advanced details:
        -   IAM instance profile: Select or create a role with
            -   `AmazonSSMManagedInstanceCore` AWS managed policy
            -   `AWSDeadlineCloud-WorkerHost` AWS managed policy
4. Launch the instance

### 2. Connect to Your Instance Using SSM Session Manager

#### For Command Line Access

Connect to your Windows instance using AWS SSM Session Manager:

```bash
# Start a PowerShell session
aws ssm start-session --target i-instanceid
```

#### For GUI Access (RDP)

For GUI access, you can use port forwarding with SSM and then connect via RDP:

1.  In one terminal, set up a port forwarding session:

    ```bash
    aws ssm start-session --target i-instanceid \
        --document-name AWS-StartPortForwardingSession \
        --parameters "localPortNumber=3389,portNumber=3389"
    ```

2.  In a second terminal, get the Windows password using your key pair:

    ```bash
    aws ec2 get-password-data --instance-id i-instanceid --priv-launch-key ~/.ssh/deadline-worker-agent-dev-keypair.pem --query PasswordData --output text
    ```

3.  Connect using an RDP client to `localhost:3389` using the administrator credentials

#### For SSH Access

You can connect to your Windows EC2 instance using a SSH through an SSM port forward tunnel.
To do so, create an SSH config entry in your `~/.ssh/config` file:

```
Host deadline-agent-windows-dev
    HostName i-instanceid
    User ec2-user
    Port 8022
    IdentitiesOnly yes
    IdentityFile ~/.ssh/deadline-worker-agent-dev-keypair.pem
    ProxyCommand aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p
```

To connect with SSH, run:

```bash
ssh deadline-agent-windows-dev
```

You can also transfer files from your local machine using the format:

```bash
# Copy from local machine to remote EC2 instance
scp local_file_path deadline-agent-windows-dev:remote_file_path

# Copy from remote EC2 instance to local machine
scp deadline-agent-windows-dev:remote_file_path local_file_path
```

### 3. Install Dependencies

**When connected remotely to the instance**, open PowerShell as Administrator (SSM sessions use Powershell as Administrator):

1.  Install Git ([docs](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git#_installing_on_windows))
2.  Install Python
3.  Install Hatch

    ```
    pip install pipx
    # pipx creates command-line entrypoints in %USERPROFILE%\.local\bin
    setx PATH "%PATH%;C:\Users\Administrator\.local\bin"
    pipx install hatch
    ```
4.  Install OpenSSH Server ([docs](https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_install_firstuse?tabs=powershell&pivots=windows-server-2022))
    *   The SSH public key from the EC2 keypair is not automatically setup. You can manually add it in an RDP session by creating `C:\ProgramData\ssh\administrators_authorized_keys` and adding the SSH key to it.
5.  [Optional] Install Visual Studio Code which can be helpful to edit code over RDP
6.  [Optional] Install Process Explorer which can be helpful for inspecting and terminating processes

You will need to reboot your instance for changes to take full effect. This can be done using:

```
ssh deadline-agent-windows-dev shutdown /r /t 0
```

### 4. Clone the Repository and Install Hatch

**When connected remotely to the instance**, clone the AWS Deadline Cloud Worker Agent repository and install Hatch:

```powershell
mkdir -p C:\Projects
cd C:\Projects
git clone https://github.com/aws-deadline/deadline-cloud-worker-agent.git
cd deadline-cloud-worker-agent

# Add your fork as a remote. Replace <USERNAME> with your GitHub alias
git remote add personal git@github.com:<USERNAME>/deadline-cloud-worker-agent.git
git fetch personal
```

### 5. Setup a Git Remote on Local Workstation

**From your local workstation**, setup a remote that points to the Windows instance's git repository.
From the local checkout of the worker agent, run:

```bash
git remote add windows-dev-instance deadline-agent-windows-dev:projects/deadline-cloud-worker-agent
```

Now fetch from the remote to ensure the remote and SSH setup are correct:

```bash
git fetch windows-dev-instance
```

If everything is setup correct, you should see the output:

```
From deadline-agent-linux-dev:projects/deadline-cloud-worker-agent
 * [new branch]      mainline   -> deadline-agent-linux-dev/mainline
```

We now have a setup for easily synchronizing git branches between your local workstation and the remote Windows EC2 instance.

## Development Workflow Tips

### Using Visual Studio Code with Remote SSH over SSM

For a better development experience, you can use Visual Studio Code with Remote SSH over SSM:

1.  Install Visual Studio Code on your local machine
2.  Install the Remote SSH extension
3.  Connect to your instance through VS Code using the configured host (`deadline-agent-linux-dev` or `deadline-agent-windows-dev`)
4.  Open the project folder and start developing

>   ⚠️ There is a known issue running the unit tests on Windows over SSH (and hence using VSCode remote SSH), since OpenSSH runs as a Windows service. This causes test failures because the functional code has branching for session 0 (Window services) but the test suite is written assuming that it is run outside of a Windows service. To work-around this issue, instead you can RDP into the Windows host and run the unit tests from a command prompt.

## Security Best Practices

By using SSM Session Manager, you're following these security best practices:

*   **No inbound ports**: EC2 instances don't require any inbound ports to be open
*   **Encryption in transit**: Traffic between your local workstation and the development instances going through SSM are encrypted in transit.
*   **IAM-based access control**: Access is controlled through IAM permissions
*   **Audit logging**: All session activity can be logged to CloudTrail and S3
*   **Key management**: The EC2 key pair is only used for password retrieval, not for direct SSH access

To further enhance security:

*   Enable session logging in AWS Systems Manager settings
*   Use IAM policies to restrict which users can access which instances
*   Consider using VPC endpoints for SSM to keep all traffic within the AWS network

## Troubleshooting

### Common SSM Connection Issues

*   **Instance not appearing in SSM**: Ensure the instance has the correct IAM role and internet access (via NAT Gateway, VPC endpoints, or public IP)
*   **Permission denied**: Verify your IAM user has the necessary SSM permissions
*   **Session timeout**: Check network connectivity and increase the session timeout in SSM preferences

### Common Key Pair Issues

*   **Key pair not found**: Ensure you've created a key pair named `deadline-worker-agent-dev-keypair` in your AWS account
*   **Key file not found**: Ensure your private key file is in the expected location (`~/.ssh/deadline-worker-agent-dev-keypair.pem`)
*   **Permission too open**: On Linux/macOS, ensure your key file has the correct permissions (`chmod 400 ~/.ssh/deadline-worker-agent-dev-keypair.pem`)
*   **Cannot retrieve Windows password**: Make sure you're using the correct private key file that corresponds to the key pair used when launching the instance. Some SSH implementatios generate OpenSSH style private key files beginning with:

    ```
    -----BEGIN OPENSSH PRIVATE KEY-----
    ```

    but EC2 can only retrieve the Administrator password using an RSA PEM format private key file
    which begins with:

    ```
    -----BEGIN RSA PRIVATE KEY-----
    ```

    Ensure your private key file is in the required RSA format.

    

### Common Issues on Linux

*   **Permission denied errors**: Ensure you have the correct permissions for the directories you're working with
*   **Missing dependencies**: If you encounter missing dependencies, install them using `sudo dnf install ...`

### Common Issues on Windows

*   **Path too long errors**: Windows has path length limitations. Consider cloning to a shorter path like `C:\dev`

## Next Steps

After setting up your development environment, we recommend:

1.  Reviewing the [Worker Agent Lifecycle](worker_lifecycle.md) documentation
2.  Exploring the codebase to understand its structure
3.  Running the worker agent in development mode to see how it works
4.  Making a small change and running tests to verify your setup

For more information on development workflows, see the [Developer Workflows](developer_workflows.md) guide.
