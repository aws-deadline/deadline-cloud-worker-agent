# AWS Deadline Cloud Worker API Protocol

This document outlines the various workflows that a Worker Agent for AWS Deadline Cloud
must perform, and the formal requirement for how those workflows are implemented in terms
of the service APIs.

## Worker Agent Startup Workflow

The Worker Agent must `CreateWorker` when it is starting for the first time on a specific worker host. 
We recommend that the Worker Agent locally persist its worker id for subsequent launches on the same host
so that it preserves the host's identity in the service. This allows the end-user to more easily
track the history of a particular worker host, and does not pollute API responses or
UI elements with offline workers that will never return to an online status.

The steps to take are:

1. Invoke the `CreateWorker` API with (farmId, fleetId)
    * Use the AWS Credentials that the Worker Agent was started with access to. i.e. The default credentials
    provider chain available in the AWS SDK.
    * This creates the Worker resource in the service that corresponds to this Worker Agent's host.
    * Response: Success(200) -> Persist the worker id, and continue.
    * Response: ThrottlingException(429), InternalServerErrorException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: AccessDeniedException(403), ValidationException(400), ResourceNotFoundException(404) -> Stop.
    Exit the application.
    * Response: ConflictException(409):
        * `reason` is `RESOURCE_ALREADY_EXISTS` -> Stop. Exit the application. This happens when a set
        of EC2 instance credentials has already created a Worker and that Worker has not been deleted; only 1 worker
        is allowed for each set of EC2 instance credentials as a security control to prevent privilege escalation.
        * `reason` is `STATUS_CONFLICT`, `resourceId` is the Worker's Fleet ID, and `context["status"]` is
        `CREATE_IN_PROGRESS` -> Perform exponential backoff, and then retry.
        * `reason` is `STATUS_CONFLICT`, `resourceId` is the Worker's Fleet ID, and `context["status"]` is
        not `CREATE_IN_PROGRESS` -> Stop. Exit the application. The Fleet for this Worker cannot be joined.
        * Otherwise -> Stop. Exit the application.
2. Invoke the `AssumeFleetRoleForWorker` API with (farmId, fleetId, workerId)
    * Use the AWS Credentials that the Worker Agent was started with access to. i.e. The default credentials
    provider chain available in the AWS SDK.
    * This obtains AWS credentials for the Worker Agent to use going forward; the Worker Agent should only
      be started with access to the bare minimum bootstrapping IAM permissions required to get to this point.
    * Response: Success(200) -> Record the credentials, and **use them for every AWS API call from this
    point forward**.
    * Response: ThrottlingException(429), InternalServerErrorException(500)  -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ResourceNotFoundException(404) -> Stop. The Worker has been deleted. Either
    exit the application, or purge the worker id and go back to step 1 of the startup workflow.
    * Response: AccessDenied(403), ValidationException(400) -> Stop. Exit the application.
3. Invoke the `UpdateWorker` API with (farmId, fleetId, workerId, status=STARTED)
    * This informs the service that the Worker is now STARTED and will be available for
      doing work.
    * Response: Success(200) -> Continue to the Idle Workflow
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ConflictException(409) ->
        * `reason` is `STATUS_CONFLICT`:
            * `context["status"]` is `ASSOCIATED` -> Perform exponential backoff, and then retry.
            * `context["status"]` is `STOPPING` or `NOT_COMPATIBLE` -> Worker cannot transition to `STARTED` from its
            current status. Invoke `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPED)
            successfully, and then retry this call.
        * `reason` is `CONCURRENT_MODIFICATION` -> Perform exponential backoff, and then retry.
        * Otherwise -> Stop. Exit the application.
    * Response: ResourceNotFoundException(404) -> Stop. The Worker has been deleted. Either
    exit the application, or purge the worker id and go back to step 1 of the startup workflow.
    * Response: AccessDeniedException(403) -> Stop. Exit the application. The IAM Role on the Fleet lacks the
    required permissions.
    * Response: ValidationException(400) -> Stop. Exit the application.
4. Continue to the [Main Control Loop](#main-control-loop).

## Main Control Loop

During the main control loop the Worker Agent is periodically querying the service for additional work,
starting or stopping work in response to direction from the service, and reporting the status of any work
that it is working on. All of these service interactions are through a single API that also acts as a
heartbeat informing the service that the Worker Agent is still active for the Worker. A Worker that fails to
heartbeat for an extended period of time will be marked as NOT_RESPONDING and have any work that it was working
on rescheduled to other Workers; if a Worker reconnects to the service after being marked NOT_RESPONDING, then it
must restart -- terminating all work that it started without reporting their status to the service.

While in this workflow, repeat the following steps in a loop:

1. If the Agent's AWS Credentials from the `AssumeFleetRoleForWorker` API response are near to
expiring, then do the [Refresh Worker Agent AWS Credentials](#refresh-worker-agent-aws-credentials-workflow)
Workflow before proceeding.
2. Determine whether the Worker Agent requires syncing with the service by checking if:
    * This is the first time into the loop; or
    * A number of seconds equal to `updateIntervalSeconds` from a previous `UpdateWorkerSchedule` response has
    passed since the last call of the `UpdateWorkerSchedule` API;
    * A Session Action has `FAILED`, `CANCELED`, or `INTERRUPTED` since the last successful `UpdateWorkerSchedule` API call
    (these are reported immediately); or
    * The Worker Agent has completed all of the work that has been assigned to it for a particular Session
    in the previous `UpdateWorkerSchedule` API response.
3. If syncing with the service is required, then call the `UpdateWorkerSchedule` API with (farmId, fleetId, workerId).
    * This is the API that queries the service to determine what the worker should be working
    on, and to update the service on the local status of work that it is being performed.
    * Request's `updatedSessionActions` values contains:
        1. Update to a `completedStatus` of `SUCCEEDED` for any Session Actions that have completed
        successfully since the last successful `UpdateWorkerSchedule` API call.
        2. Updates for all Session Actions that are currently actively running if the running
        status has not previously been reported in a successful `UpdateWorkerSchedule` API call or there
        is a change from the previously sent update (e.g. an update to the progress or a
        new message field value).
        3. A `completedStatus` of `FAILED` with no `processExitCode` defined and a `message` indicating
        that the Worker cannot start the Session Action if either:
            * The Session Action is a type that is not understood by the Worker Agent (e.g. a new type
            added in the future);
            * The Session Action uses an Open Job Description version that the Worker Agent doesn't understand; or
            * Otherwise unable to be run by the Worker Agent (e.g. compatibility reasons, or failures
            in a `AssumeQueueRoleForWorker` request).
        4. Any other updates as dictated by a Worker-Initiated Drain workflow.
    * Response: Success(200) cases:
        1. `assignedSessions` empty -> End any locally active Sessions that the worker is
        holding as active, and then loop.
        2. `assignedSessions` non-empty ->
            * If there are any Sessions in the response that are new to the Worker Agent, then start running
            the new Session locally; queue all Session Actions received for the Session to be run
            sequentially in the order provided. 
            * If any of the Sessions in the response have Session Actions that were not present
            in a previous response, then add those Session Actions to the local action pipeline
            for the corresponding Session, so that they are run in the correct order.
            * See [Running a Session](#running-a-session) and [Session AWS Credentials](#session-aws-credentials)
            for additional relevant information.
            * Note: The temporary AWS Credentials from `AssumeFleetRoleForWorker` that are in-use by the Worker Agent
            must never be made available to a running Session Action. The AWS Credentials from `AssumeQueueRoleForWorker`
            serve that purpose.
        3. `cancelSessionActions` non-empty ->
            * Ignore any Session Action cancels for Session Actions that have already been
            completed, successfully or otherwise.
            * Cancel any Session Actions that are in the list. See:
            [Termination of Session Actions](#termination-of-sessionactions).
                * Report any Session Actions in this list that are currently running as `CANCELED` once
                the action has stopped running on the Worker host.
                * If there are Session Actions that are: 1/ in the cancelation list; 2/ not yet running
                and 3/ are behind an actively running Session Action in the same Session's pipeline; then
                those Session Action's `NEVER_ATTEMPTED` completed status must not be reported before the final
                completed status of the running Session Action.
        4. `desiredWorkerStatus=STOPPED` -> Continue to the
        [Worker Agent Shutdown](#worker-agent-shutdown-workflow) Workflow.
        Note: The service will only send this if `assignedSessions` in the response is also
        empty.
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry.
    * Response: ConflictException(409)
        * `reason` is `STATUS_CONFLICT` and the `resourceId` is for the Worker -> Transition to the
        [Worker Agent Startup](#worker-agent-startup-workflow) Workflow. The Worker is no longer
        in the STARTED status; this is likely due to failing to successfully call this API for an
        extended period of time.
        * `reason` is `CONCURRENT_MODIFICATION` -> Perform exponential backoff, and then retry.
        * Any other -> Unrecoverable error. Stop. Exit the application.
    * Response: AccessDeniedException(403) -> Stop. Exit the application. The IAM Role on the
    Fleet lacks the required permissions.
    * Response: ResourceNotFoundException(404) -> Stop; the Worker has been deleted from the service.
    If a service would restart the application, then exit. Alternatively, go back to the
    [Worker Agent Startup](#worker-agent-startup-workflow) Workflow.
    * Response: ValidationException(400) -> Stop. Exit the application. The request was
    malformed, so the Worker Agent has a bug.

Note: We recommend that the Worker Agent watch for SIGTERM or EC2 Spot Interruption, and transition to a
[Worker Initiated Drains](#worker-initiated-drains) Workflow if one is detected. This will help ensure that there is minimal delay
between the host shutting down and the Jobs that it was working on being picked up by another Worker.

## Refresh Worker Agent AWS Credentials Workflow

During operation, the Worker Agent uses AWS Credentials for AWS API calls that it obtained from the
`AssumeFleetRoleForWorker` API. These are temporary credentials that expire after a short duration, so
the Worker Agent will need to periodically refresh the credentials to remain operating. We recommend refreshing
credentials at least 15 minutes before expiry as that provides some buffer for re-sending the same request
in the event that the request is throttled or otherwise unable to be completed.

Note: **Do not** request fresh AWS credentials via `AssumeFleetRoleForWorker` for each API call that the
Worker Agent needs to make. This will result in heavy throttling of the Worker Agent's API requests.

To request fresh credentials:

1. Invoke the `AssumeFleetRoleForWorker` API with (farmId, fleetId, workerId)
    * Use the AWS Credentials that the Worker Agent previously obtained via a call to the API to make
    this request. Fall-back to the default AWS Credentials provider chain only if the credentials from
    the previous successful call are expired.
    * Response: Success(200) -> Record the credentials, and **use them for every AWS API call from this point forward**.
    * Response: ThrottlingException(429), InternalServerErrorException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ConflictException(409)
        * `reason` is `STATUS_CONFLICT` and the `resourceId` is of the Fleet -> The Fleet is not ACTIVE. Stop,
        and exit the application.
        * `reason` is `STATUS_CONFLICT` and the `resourceId` is of the Worker -> The request is likely being made
        with AWS Credentials from an EC2 instance profile while the Worker is online. This is not allowed as it
        can lead to a privilege escalation by the running Job. Retry the request with AWS Credentials previously
        obtained from this API.
    * Response: AccessDeniedException(403), ValidationException(400), ResourceNotFoundException(404) -> Stop.
    Exit the application.

## Running a Session

A Session is a logical isolation of a pipeline of Session Actions running in a given sequence. Meaning it is not
actually isolated, in the sense of running in a separate virtual machine/container from the agent or
other Sessions, but we think of its data as being separate from other Sessions that the agent may be
running concurrently on the same host.

In practice, this means that:
1. We can cache information on Session Actions, but that cache must be isolated to the Session and not shared
between Sessions. The reasoning is that a Job may be mutated in the service at any time, and thus when we start
a new Session we need to ensure that the information used to run it matches what is in the service at the start
of the Session, rather than from a previous pre-mutation Session on the same host.
2. Logs for a Session are separate from the Worker log and from other Sessions logs.
3. Subprocesses run during a Session are not isolated on the host from subprocesses run by other Sessions.

The response from a call to the `UpdateWorkerSchedule` API may contain information on new Sessions and
Session Actions to run, but the response does not contain *all* of the information required to run
those Session Actions; it simply contains abbreviated metadata regarding what needs to be run, and in
what order. The Agent uses the `BatchGetJobEntity` API to fetch the data required to run Session Actions;
this data must be cached within a Session, rather than fetching it anew for each Session Action. See
[Obtaining Details for running a Session](#obtaining-details-for-running-a-session) below for additional details.

### Session AWS Credentials

A running Session must be provided AWS Credentials from an IAM Role that the customer has attached to
the Queue the Job was submitted to. These AWS Credentials are provided to the Worker Agent
by making a request to the `AssumeQueueRoleForWorker` API. Note that the service guarantees that a specific
Worker Agent will never have Sessions to run from multiple different Queues at the same time, so we recommend
sharing these AWS Credentials with all running Sessions until a new Session is started that belongs to
a different Queue; at which point the old credentials should be deleted and new ones obtained.

These are temporary credentials that expire after a short duration, so the Worker Agent will need to periodically
refresh the credentials and update the credentials available to the running Session's processes. We
recommend refreshing credentials at least 15 minutes before expiry as that provides some buffer for re-sending
the same request in the event that the request is throttled or otherwise unable to be completed.

Note that this IAM Role is optional, so credentials may not always be available. The API will return
an empty response in the case that there is no IAM Role available.

Note: **Do not** request fresh AWS credentials via `AssumeQueueRoleForWorker` for each Session or Session Action
to be run. This will likely result in heavy throttling of the Worker Agent's API requests in the case where
Session Actions are very fast to run.

To request fresh credentials:

1. Invoke the `AssumeQueueRoleForWorker` API with (farmId, fleetId, workerId, queueId)
    * Use the AWS Credentials that the Worker Agent previously obtained via `AssumeFleetRoleForWorker` to make
    this request.
    * Response: Success(200) cases:
        1. AWS Credentials are present in the response -> Record the credentials, and make them available
        to all actions taken by a Session Action. For subprocesses, these credentials must be available
        via the default profile; we recommend using a
        [credentials process](https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html)
        for this as that allows for credentials to be refreshed by the subprocess that is running.
        2. AWS Credentials are not present in the response -> There are no AWS Credentials to provide
        to Session Actions. Any previously cached AWS Credentials for the Queue must be cleared.
    * Response: ThrottlingException(429), InternalServerErrorException(500) -> Perform exponential backoff,
    and then retry.
    * Response: AccessDeniedException(403) -> The IAM Role that is attached to this Worker's Fleet does not have
    the required IAM permissions. Log the issue and then continue in section
    [When Failing to Obtain Session AWS Credentials](#when-failing-to-obtain-session-aws-credentials).
    * Response ResourceNotFoundException(404) -> The Worker or Queue no longer exist. Log the issue and
    fail all Session Actions for Sessions of the requested Queue.
    * Response ConflictException(409) ->
        * `reason` is `STATUS_CONFLICT` and the `resourceId` is for the Worker -> The Worker is no longer online
        in the service. Either exit the application, or purge all active work and run the
        [Worker Agent Startup](#worker-agent-startup-workflow) Workflow.
        * `reason` is `STATUS_CONFLICT` and the `resourceId` is for the Queue -> The Queue is not
        scheduling work, or the Worker is not assigned Sessions from the Queue. This is likely
        eventual consistency. Perform exponential backoff, and then retry. If still receiving this
        error after 10 seconds of retries, then continue in section
        [When Failing to Obtain Session AWS Credentials](#when-failing-to-obtain-session-aws-credentials).
        * Otherwise -> [When Failing to Obtain Session AWS Credentials](#when-failing-to-obtain-session-aws-credentials).
    * Response ValidationException(400) -> Malformed request. The Worker Agent has a bug.
    [When Failing to Obtain Session AWS Credentials](#when-failing-to-obtain-session-aws-credentials)

#### When Failing to Obtain Session AWS Credentials

Log the issue in the Worker Agent's Log and then either:

1. Fail all Session Actions for Sessions of the requested Queue; or
2. Proceed with running the Session Actions but be sure to include a message in the Session Log that indicates that
there are no AWS Credentials available.

### Obtaining Details for running a Session

The response from `UpdateWorkerSchedule` does not contain all of the information required to run the Actions
within a Session. Those details are available through the `BatchGetJobEntity` API. 

To start the Session, the Agent must request `jobDetails` from the `BatchGetJobEntity` API; the response to
this request will contain information such as what OS user to run the Session's subprocesses as, the job parameter
values, logging settings for the Session, and so on.

There are four kinds of Session Actions, and the details for what to run for each of those is also obtained
through a request to the `BatchGetJobEntity` API:

1. `syncInputJobAttachments` -- Request `jobAttachmentDetails`
2. `envEnter` -- Request `environmentDetails`
3. `taskRun` -- Request `stepDetails`
4. `envExit` -- Use the cached details from the corresponding `envEnter` Session Action.

To make the request, call the `BatchGetJobEntity` API with (farmId,fleetId,workerId) and a list of the desired
identifiers:
* Use the AWS Credentials that the Worker Agent previously obtained via `AssumeFleetRoleForWorker` to make
this request.
* Response: Success(200) -> Record the response data.
* Response: ValidationException(400), AccessDeniedException(403), ResourceNotFound(404) -> Fail the next
Session Action that is pipelined according to [Handling Unsuccessful Session Actions](#handling-unsuccessful-session-actions)
with a failure message that clearly indicates the exception as being the reason.
* Response: ThrottlingException(429) -> Perform exponential backoff, and then retry.
* Response: InternalServerErrorException(500) -> Perform exponential backoff, and then retry a limited number of times.
If still unsuccessful after retries, then fail the next Session Action that is pipelined according to 
[Handling Unsuccessful Session Actions](#handling-unsuccessful-session-actions) with a failure message that
clearly indicates the exception as being the reason.

Aside from the request to `BatchGetJobEntity` raising an exception, the request for a specific details entity
may error. In response to these error codes, the agent should:
* InternalServerException, ValidationException, ResourceNotFoundException -> Fail the corresponding Session Action with
a clear message that indicates the API response as the cause. If the error is for the `jobDetails` then fail the
next Session Action that is pipelined according to [Handling Unsuccessful Session Actions](#handling-unsuccessful-session-actions)
with a failure message that clearly indicates the exception as being the reason.
* MaxPayloadSizeExceeded -> Re-request the corresponding entity in a subsequent request to `BatchGetJobEntity`;
the response payload was too large form other response items to include the entity in the response.

### Handling Unsuccessful Session Actions

A Session runs a pipelined series of Session Actions sequentially. The service currently defines four kinds of
Session Actions to consider:

1. `syncInputJobAttachments`
2. `envEnter`
3. `taskRun`
4. `envExit`

A failure or cancelation of any Session Action will impact whether or not the Worker is allowed to run
subsequent Session Actions in the same Session, which Session Actions get scheduled to the Session, and the
closing of the Session. A CANCELED, INTERRUPTED, or FAILED status of any Session Action in a Session prevents
that Session from being allowed to run further `taskRun`, `envEnter`, and `syncInputJobAttachments`
Session Actions.

#### Service-Initiated Session Action Cancelation

If an `UpdateWorkerSchedule` API response cancels a Session Action, then the same response will inform the Worker to
cancel all subsequent Session Actions in the Session except for `envExit` Session Actions that correspond
to completed, failed, or canceled `envEnter` Session Actions. When the Worker receives this response, it may
have already completed some of the specific Session Actions; that is okay, simply update the already
completed Session Action(s) with their completion status as though no cancel was received for them.

The next UpdateWorkerSchedule API call *after* a running Session Action was successfully canceled and
stopped will contain an update that the stopped Session Action was `CANCELED` and
all subsequent canceled Session Actions were `NEVER_ATTEMPTED`. The service response to that may
queue-up `envExit` Session Actions for all `envEnter` Session Actions that ran in some way (completed
successfully, failed, canceled, or interrupted) if they were not already queued to run.

If an `envExit` is cancelled, then the service will inform the Worker to cancel only the specific
`envExit` that was cancelled; any `envExit` Session Actions after it in the Session must be allowed to
attempt to run.

#### Handling Unsuccessful `syncInputJobAttachments` or `taskRun` Session Actions

If the Worker Agent reports these Session Action as `FAILED`, `INTERRUPTED`, or `CANCELED` then the Worker Agent
must report all queued Session Actions after it in the Session as `NEVER_ATTEMPTED`; except for
`envExit` actions that correspond to already completed `envEnter` Session Actions. Session Actions that are 
reported as `NEVER_ATTEMPTED` must not report `startedAt` or `endedAt` times. The service
must not queue additional Session Actions to the Session except for `envExit` Session Actions that
correspond to already completed `envEnter` Session Actions for the same Session.

#### Handling Unsuccessful `envEnter` Session Actions

If the Worker Agent reports this Session Action as `FAILED`, `INTERRUPTED`, or `CANCELED`, then the Worker Agent must
also report all queued Session Actions after it in the Session as `NEVER_ATTEMPTED`; except for
`envExit` actions that correspond this `envEnter` or to already completed `envEnter` Session Actions.
The service must not queue additional Session Actions to the Session except for `envExit` Session Actions
that correspond to the failed `envEnter` and all already completed `envEnter` Session Actions for the
same Session.

#### Handling Unsuccessful `envExit` Session Actions

If the Worker Agent reports this Session Action as `FAILED`, `INTERRUPTED`, or `CANCELED` then the Worker Agent reports
only this action as such. Any subsequent `envExit` actions in the Session must still be run as normal.


## Draining

Draining is the act of getting the Worker to a status where it is not running any work; usually in
preparation for a shutdown. Drains can be service-initiated (e.g. for an autoscaling scale-in), or
worker-initiated (e.g. ec2 spot interruption, or a SIGTERM sent to the application).

### Service-Initiated Drains

From the Worker Agent's perspective, it does not know that it is being drained. The service will send
UpdateWorkerSchedule API responses in an order that ultimately accomplishes the goal of draining the Worker.

The service implements two styles of worker draining:

1. Immediate Drain -- All active Session Actions are canceled, and Sessions are allowed to run their
`envExit` Session Actions as normal to cleanup Sessions.
2. Eventual Drain -- The Worker completes all Session Actions that it has been assigned as normal, but
the service does not assign any new `taskRun` Session Actions to the Worker. The `desiredWorkerStatus`
property of the `UpdateWorkerSchedule` response is set once the Worker Agent is no longer running any Sessions, and the
Worker shuts down in response.

In both cases the `desiredWorkerStatus` property of the `UpdateWorkerSchedule` response is set once the Worker Agent
is no longer running any Sessions. In response, the Worker Agent should run the
[Worker Agent Shutdown](#worker-agent-shutdown-workflow) Workflow.

### Worker-Initiated Drains

A worker-initiated drain is initiated by some activity on the host machine that the service is not, or
cannot be, aware of.

Worker initiated drains differ in their urgency based on what event initiates the drain.

* EC2 Spot Termination - The host is being permanently terminated in 2 minutes.
* SIGTERM - When sent by `shutdown` on the local POSIX host, this will be followed by a SIGKILL in 5 seconds.
    * The Worker Agent needs to cancel everything, and likely cannot even run environment-end actions.
    * The Worker Agent's goal is to get as much work wrapped up, and/or updated, as it can as fast as possible.
* CTRL-C - User wants the application to end.

In all cases, the service does not need to be informed that the sequence of actions that the worker is doing
is a drain; it’ll figure it out when the Worker does not heartbeat (`NOT_RESPONDING`).

#### Expedited Drain

This is a rapid drain that cuts corners with the primary goal being to inform the service that the worker
is shutting down. The Worker should perform this style of drain when the initiating event for the drain
results in the Worker having less than 10, or so, seconds before the Worker Agent terminates.

For the service to be able to reschedule the Session Actions that are assigned to the Worker it either needs
to be informed that the Worker has shut down, or for the service to set the Worker to `NOT_RESPONDING` status due
to a lack of heartbeats. Waiting for the Worker to become `NOT_RESPONDING` imposes a delay on the end-user's Jobs
being completed, so we aim to have the Worker inform the service that it has shut down to avoid the delay.

To perform an expedited drain, the Worker Agent should:

1. Immediately call the `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPING).
    * The purpose of invoking this API is to inform the service that the Worker Agent has initiated a drain
    and that the service should not assign it new `taskRun` actions in a response to a `UpdateWorkerSchedule` request.
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry. Do not let retries prevent the Worker Agent from completing the drain; it is more important
    to call the API with status=STOPPED later in this workflow than it is to complete this request.
    * Response: ConflictException(409)
        * `reason` is `CONCURRENT_MODIFICATION` -> Treat as a ThrottlingException/InternalServerException.
        * Otherwise -> Ignore; just continue.
    * Response: ResourceNotFoundException(404) -> The Worker has been deleted. Just exit the application.
    * Response: ValidationException(400), AccessDeniedException(403)  -> Ignore; just continue.
2. Cancel all running Session Actions. Override cancelation grace time, if any, to speed up the cancel. e.g. To 3
seconds for a SIGTERM. Perform any cleanup of temporary files created by the Session as normal.
3. Concurrent with the cancelation of running Session Action, without waiting for them to exit gracefully,
immediately call the `UpdateWorkerSchedule` API:
    * In the request:
        * `updatedSession Actions` must update all actively running Session Actions with
        `completedStatus=INTERRUPTED`.
        * `updatedSession Actions` must update all other Session Actions that have not yet been run with
        `completedStatus=NEVER_ATTEMPTED`.
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry. Do not let retries prevent the Worker Agent from completing the drain; it is more important
    to call `UpdateWorker` API with `status=STOPPED` later in this workflow than it is to complete this request.
    * Response: ResourceNotFoundException(404) -> The Worker has been deleted. Just exit the application.
    * Response: ConflictException(409), AccessDeniedException(403), ValidationException(400) -> Ignore; just continue.
4. Without waiting for the running Session Action to exit gracefully, call the `UpdateWorker` API
with (farmId, fleetId, workerId, status=STOPPED).
    * This request is the most important to succeed at. Doing so will allow the service to reschedule
    all of the Session Actions that have been previously assigned to the Worker without delay.
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ConflictException(409)
        * `reason` is `CONCURRENT_MODIFICATION` -> Treat as a ThrottlingException/InternalServerException.
        * Otherwise -> Ignore; just continue.
    * Response: ResourceNotFoundException(404) -> The Worker has been deleted. Just exit the application.
    * Response: ValidationException(400), AccessDeniedException(403),
    InternalServerException(500) -> Ignore; just continue.
5. If the Worker Agent supports and has been configured to Delete the Worker on shutdown, and the conditions have
been met for that deletion then call the `DeleteWorker` API with (farmId, fleetId, workerId).
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429) -> Perform exponential backoff, and then retry.
    * Response: ConflictException(409):
        * `reason` is `STATUS_CONFLICT` and `resourceId` is of the Worker -> This only happens if the Worker does not
        have `CREATED` or `STOPPED` status. Update the Worker to `STOPPED` status, and try to delete again.
        * Otherwise -> Stop. Exit the application.
    * Response: ResourceNotFoundException(404), ValidationException(400), AccessDeniedException(403) -> Ignore; just continue.
6. Exit the application as desired.

#### Regular Drain

The regular drain is not expedited. The workflow is intended for situations where the Worker Agent has some time to
let any running Session Actions exit and complete gracefully. For example, a Worker Agent can perform this style of
drain in response to an EC2 Spot Interruption event.

To perform a regular drain, the Worker Agent should:

1. Immediately call the `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPING).
    * The purpose of invoking this API is to inform the service that the Worker Agent has initiated a drain
    and that the service should not assign it additional new `taskRun` actions in a response to a
    `UpdateWorkerSchedule` request; the service may queue additional `envExit` Session Actions.
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429), InternalServerException(500)  -> Perform exponential backoff,
    and then retry. Do not let retries prevent the Worker Agent from completing the drain; it is more important to
    call the API with status=STOPPED later in this workflow than it is to complete this request.
    * Response: ResourceNotFoundException(404) -> The Worker has been deleted. Terminate all locally
    running Sessions, and then exit the application.
    * Response: ConflictException(409) -> The Worker is not online in the service. Terminate all locally
    running Sessions, and then skip straight to Step 6 of this drain algorithm.
    * Response: ValidationException(400), AccessDeniedException(403) -> Ignore; just continue.
2. Cancel all running Session Actions. Override cancelation grace time to speed up the cancel, if needed,
to give the Worker Agent time to complete the remainder of the drain workflow.
3. Cancel all queued Session Actions except for `envExit` type Session Actions.
4. Invoke the `UpdateWorkerSchedule` API as normally dictated by the [Main Control Loop](#main-control-loop).
5. Invoke the `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPED).
    * This request is the most important to succeed at. Doing so will allow the service to reschedule
    all of the Session Actions that have been previously assigned to the Worker without delay.
    * Response: Success(200) -> Continue
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ResourceNotFoundException(404) -> The Worker has been deleted. Just exit the application.
    * Response: ValidationException(400), ConflictException(409), AccessDeniedException(403),
    InternalServerException(500) -> Ignore; just continue.
7. Exit the application as desired.

Note: If the available time remaining for a regular drain is ever less than 10 seconds, then switch
to an expedited drain workflow.


## Worker Agent Shutdown Workflow

The Worker Agent application may be shut down for a multitude of reasons. Some of those may be temporary, and the
Worker Agent will be restarted on the same host again in the future, but others may be more permanent with the underlying
host being permanently terminated.

The steps to perform a shutdown differ based on the event that led to the shutdown. In the case of a service-initiated
shutdown (i.e. when the Worker Agent receives an `UpdateWorkerSchedule` response that has `desiredWorkerStatus=STOPPED`)
then the Worker Agent must:

1. Invoke the `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPING).
    * This informs the service that the Worker is no longer available to perform work and
      is actively trying to shutdown the host.
    * Response: Success(200) -> Continue.
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ConflictException(409)
        * `reason` is `CONCURRENT_MODIFICATION` -> Perform exponential backoff, and then retry.
        * Otherwise -> Ignore and continue.
    * Response: ResourceNotFoundException(404) -> The worker has been deleted. Continue to repeatedly
      trying to shutdown the host until successful.
    * Response: AccessDeniedException(403), ValidationException(400) -> Some other problem. Continue to repeatedly
      trying to shutdown the host until successful.
2. If successfully transitioned to STOPPING, then repeat in a loop:
    1. Call to the operating system to shutdown the host.
    2. Invoke the `UpdateWorkerSchedule` API with (farmId, fleetId, workerId, updatedSessionActions=None)
        * Response: Success (200) -> Continue
        * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff, and
          then retry.
        * Response: ConflictException(409)
            * `reason` is `CONCURRENT_MODIFICATION` -> Perform exponential backoff, and then retry.
            * Any other -> Ignore & continue.
        * Response: ValidationException(400), AccessDeniedException(403), ResourceNotFoundException(404) -> Ignore & continue.
    3. Sleep for 30 seconds.

In the case of a Worker-initiated shutdown (e.g. SIGTERM, EC2 Spot Interruption, etc) then follow the
[Regular Drain](#regular-drain) process and then:

1. Invoke the `UpdateWorker` API with (farmId, fleetId, workerId, status=STOPPED).
    * This informs the service that the Worker is no longer available to perform work.
    * Response: Success(200) -> Continue.
    * Response: ThrottlingException(429), InternalServerException(500) -> Perform exponential backoff,
    and then retry indefinitely.
    * Response: ConflictException(409)
        * `reason` is `CONCURRENT_MODIFICATION` -> Perform exponential backoff, and then retry.
        * Otherwise -> Ignore and continue.
    * Response: ResourceNotFoundException(404) -> Stop. Exit the application, the worker has been deleted.
    * Response: AccessDeniedException(403), ValidationException(400) -> Stop. Exit the application.

