# Worker Agent Lifecycle

This document explains the lifecycle of the AWS Deadline Cloud Worker Agent, including its startup, running, and shutdown phases. Understanding this lifecycle is crucial for developers working on the worker agent codebase.

## Overview

The worker agent goes through three main phases during its lifecycle:

1. **Startup Phase**: Initialization, configuration loading, and worker registration
2. **Running Phase**: Main operational loop, handling sessions and reporting status
3. **Shutdown Phase**: Graceful termination, cleanup, status change, and host shutdown

Each phase involves specific components and operations that ensure the worker agent functions correctly within the AWS Deadline Cloud ecosystem.

```mermaid
---
title: The Worker Life-Cycle
---
flowchart LR
    %% High-level worker lifecycle
    start([start]) --> startup[Startup Phase]
    startup --> running[Running Phase]
    running --> shutdown[Shutdown Phase]
    shutdown --> terminate([end])
    
    %% Styling
    classDef startupPhase fill:#c9e6ff,stroke:#0066cc,color:#000
    classDef runningPhase fill:#d8f0d8,stroke:#2d862d,color:#000
    classDef shutdownPhase fill:#ffe6cc,stroke:#cc7a00,color:#000
    classDef startEnd fill:#EEEEEE,stroke:#444444,color:#000
    
    class startup startupPhase
    class running runningPhase
    class shutdown shutdownPhase
    class start,terminate startEnd
```

## Startup Phase

The startup phase begins when the worker agent process is launched and completes once the worker
is ready to accept sessions. The startup phase consists of:

*   creating a worker resource (if necessary) or determining the existing worker resource
*   ensuring the worker has valid AWS credentials for the worker
*   ensuring the worker is in the `STARTED` status

The diagram below illustrates the startup phase as a flow chart:

```mermaid
flowchart TD
    %% Startup Phase
    start([start]) --> loadConfig[Load configuration]
    
    loadConfig --> initLogging[Initialize local logging]
    initLogging --> bootstrap

    subgraph bootstrap[**Bootstrap worker**]
        
        checkState{Worker State Exists?} -->|Yes| loadState[Load Worker State]
        checkState -->|No| createWorker[Create New Worker]
        
        loadState --> checkCreds{Cached Worker AWS Credentials AND not Expired?}
        checkCreds -->|Yes| useExistingCreds[Use Existing AWS Credentials]
        checkCreds -->|No| getNewCreds[Get Worker AWS Credentials]
        
        createWorker --> persistState[Persist Worker State]
        useExistingCreds --> updateWorker[Update Worker Status]
        persistState --> getNewCreds

        getNewCreds --> persistCreds[Persist Worker AWS Credentials]
        persistCreds --> updateWorker
    end
    
    updateWorker --> initRemoteLogging[Initialize remote log forwarding]
    initRemoteLogging --> next([to **Running** phase])
    
    %% Styling
    classDef startupPhase fill:#c9e6ff,stroke:#0066cc,color:#000
    classDef startEnd fill:#EEEEEE,stroke:#444444,color:#000

    class loadConfig,initLogging,checkState,loadState,createWorker,checkCreds,useExistingCreds,getNewCreds,persistState,persistCreds,updateWorker,initWorker,initRemoteLogging startupPhase
    class start,next startEnd
```

The following diagram provides a more detailed look into the sequence of interactions between the various components:

```mermaid
sequenceDiagram
    participant Entrypoint
    participant Bootstrap

    activate Entrypoint
    Entrypoint->>Entrypoint: Resolve configuration
    Entrypoint->>Entrypoint: Configure local logging
    Entrypoint->>Bootstrap: Bootstrap worker
    activate Bootstrap

    create participant DeadlineClient as Deadline API Client
    Bootstrap->>DeadlineClient: Create API client

    participant AWS as Deadline Cloud API

    Bootstrap->>Bootstrap: Detect host capabilities
    
    alt Has cached worker state file
        Bootstrap->>Bootstrap: Load worker state from disk
        
        alt Has cached AWS credentials (not expired)
            Bootstrap->>Bootstrap: Load cached AWS credentials
        else No cached AWS credentials or expired
            Bootstrap->>DeadlineClient: assume_fleet_role_for_worker()
            DeadlineClient->>AWS: AssumeFleetRoleForWorker
            AWS->>DeadlineClient: 
            DeadlineClient-->>Bootstrap: Worker AWS credentials
            Bootstrap->>Bootstrap: Persist AWS credentials to disk
        end
    else No cached worker state
        Bootstrap->>DeadlineClient: create_worker()
        DeadlineClient->>AWS: CreateWorker
        AWS->>DeadlineClient: 
        DeadlineClient-->>Bootstrap: Worker ID
        Bootstrap->>Bootstrap: Persist worker ID to state file
        Bootstrap->>DeadlineClient: assume_fleet_role_for_worker()
        DeadlineClient->>AWS: AssumeFleetRoleForWorker
        AWS-->>DeadlineClient: 
        DeadlineClient->>Bootstrap: Worker AWS credentials
        Bootstrap->>Bootstrap: Persist AWS credentials to disk
    end

    Bootstrap->>DeadlineClient: update_workerstatus=STARTED)
    DeadlineClient->>AWS: UpdateWorker(status=STARTED)
    AWS->>DeadlineClient: 
    DeadlineClient->>Bootstrap: 
    Bootstrap->>Entrypoint: 
    
    deactivate Bootstrap

    create participant Worker
    Entrypoint->>Worker: Create worker
    Entrypoint->>Worker: Run worker
    deactivate Entrypoint
```

### Key Steps

1.  **Process Initialization**

    The worker agent process starts from:

    -   the `deadline-worker-agent` command-line entrypoint or
    -   a configured operating system service
        -   a systemd service unit on Linux
        -   a Windows Service on Windows

    The `deadline-worker-agent` command runs the `entrypoint` function located in the
    `startup/entrypoint.py` file.

2.  **Configuration Loading**

    Configuration is resolved using a combination of command-line arguments, environment variables,
    and a config file (see the [user config documentation](../configuration.md)).

    The `Configuration` class in `config/config.py` handles the loading and validation.

3.  **Worker Bootstrap**

    -   The `bootstrap_worker` function in `startup/bootstrap.py` handles worker creation
    -   Host capabilities are collected (OS, vCPU count, system memory, GPU count, etc&hellip;)
    -   The [worker state file](../state.md#worker-state-file) is checked for existing worker information

        **If a worker state file exists:**

        -   The worker ID is loaded from the state file
        -   Cached AWS credentials are checked for validity
        -   If valid credentials exist, they are used to initialize the worker
        -   If credentials are missing or expired, new credentials are obtained by making an
            `AssumeFleetRoleForWorker` API request and persisting them to disk

        **If no worker state file exists:**

        -   A new worker is registered with the AWS Deadline Cloud service by making a `CreateWorker`
            API request
        -   The worker ID is persisted to the state file
        -   Worker credentials are obtained via `AssumeFleetRoleForWorker` and persisted to disk

    -   After obtaining credentials, the Worker status is updated to `STARTED` by making an
        `UpdateWorker` API request

4.  **Worker Initialization**
    -   The `Worker` class is instantiated with valid worker IAM credentials and the worker ID in
        the `STARTED` status
    -   The `Worker.run()` method is called which is the transition into the **Running** phase

## Running Phase

Coming soon&hellip;

## Shutdown Phase

Coming soon&hellip;

## Next Steps

After understanding the worker agent lifecycle, we recommend exploring:

- [Session Lifecycle](session_lifecycle.md) - Comprehensive overview of how sessions are executed
- [Architecture](architecture.md) - Overview of the worker agent architecture and components
- [Worker API Protocol](worker_api_protocol.md) - Documentation of the API interactions with the Deadline Cloud service
