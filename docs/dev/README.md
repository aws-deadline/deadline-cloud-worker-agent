# AWS Deadline Cloud Worker Agent Developer Documentation

This page provides guided documentation for development of the AWS Deadline Cloud worker agent.
The following diagram illustrates the journey recommended for a new developer onboarding to worker
agent development.

```mermaid
---
title: Worker Agent Developer Journey
config:
  theme: base
---
flowchart LR
    Start([Start Developer Journey]) --> Documentation
    
    subgraph Documentation["📖 Learning"]
        direction TB
        Architecture[Learn about architecture] --> WorkerLife[Learn about Worker life-cycle]
        WorkerLife --> SessionLife[Learn about Session life-cycle]
        SessionLife --> Protocol[Learn about Worker Protocol]
        Protocol --> GitWorkflows[Learn about git workflows]
        GitWorkflows --> ReleaseProcess[Learn about release process]
    end
    
    Documentation --> Setup
    
    subgraph Setup["📋 Setup"]
        direction TB
        ForkRepo[Fork GitHub Repository] --> DevEnvSetup[Setup Development Environment]
        DevEnvSetup --> CloneRepo[Clone git Repository]
    end
    
    Setup --> DevelopmentCycle
    
    subgraph DevelopmentCycle["🔄 Development Cycle"]
        direction TB
        FetchLatest[Fetch latest code] --> CreateBranch[Create Branch]
        CreateBranch --> DevIterations[[Development iterations]]
        
        subgraph Iterations["Development Iteration Loop"]
            direction TB
            DevIterations --> WriteCode[Write/modify code]
            WriteCode --> CodeQuality[Run code quality tools]
            CodeQuality --> RunUnitTests[Run unit tests]
            RunUnitTests --> RunIntegTests[Run integration tests]
            RunIntegTests --> |If required| RunLocal[Run worker agent locally]
            RunLocal --> RunE2ETests[Run E2E tests]
            RunIntegTests --> RunE2ETests
            RunE2ETests --> CommitChanges[Commit changes]
            CommitChanges --> DevIterations
        end
        
        DevIterations --> |Ready for review| PushChanges[Push branch]
        PushChanges --> CreatePR[Create pull request]
        CreatePR --> ReviewProcess{Review process}
        ReviewProcess -->|changes requested| DevIterations
        ReviewProcess -->|Approved| MergePR[Merge pull request]
    end

    style Documentation fill: #DDCCBB
    style Setup fill: #AACCCC
    style DevelopmentCycle fill: #BBDDBB
```

This directory contains comprehensive documentation for developers working on the AWS Deadline Cloud Worker Agent. The documentation is organized to provide a clear understanding of the worker agent's architecture, development processes, and technical details.

## Learning

If you're new to the project, we recommend starting with these documents:

* [Architecture](architecture.md) - Overview of the worker agent architecture and components
* [Worker Lifecycle](worker_lifecycle.md) - Detailed explanation of the worker agent's lifecycle
* [Session Lifecycle](session_lifecycle.md) - Comprehensive overview of how sessions are executed
* [Worker API Protocol](worker_api_protocol.md) - Documentation of the API interactions with the Deadline Cloud service
* [GitHub Workflows](github_workflows.md) - CI/CD pipelines and GitHub Actions configuration
* [Release Process](release_process.md) - Information about the release workflow and versioning strategy

## Setup

See [Development Environment Setup](development_environment_setup.md) for guidance on setting up
your development environment

## Development Processes

These documents cover the development workflows and processes:

- [Developer Workflows](developer_workflows.md) - Common development tasks and procedures
