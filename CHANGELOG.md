## 0.29.1 (2026-05-07)

### Features
* Job attachment S3 bucket and root prefix are now exposed as `DEADLINE_JA_S3_BUCKET` and `DEADLINE_JA_ROOT_PREFIX` environment variables in the session environment when job attachment settings are configured. This allows plugins to access job attachment storage locations without additional API calls. (#917)
* Added progress timer logging during host configuration script execution. The agent now periodically logs elapsed and remaining time (every 30s, accelerating to every 10s when ≤60s remain) and emits a warning when the server-side timeout is reached, improving observability for host configuration timeouts. (`d30e925`)
* Added region configuration support to `worker.toml` and the installer, allowing you to explicitly configure the AWS region for the worker agent. (`0690a0f`)
* Added `[telemetry]` section to `worker.toml` with `opt_out` and `identifier` settings, and `--telemetry-opt-out` / `--no-telemetry-opt-out` CLI flags to `python -m deadline_worker_agent.config`. (#920)
* Removed dependency on the `deadline` package (client library/CLI/config) and use `deadline-job-attachments` directly. Telemetry settings previously stored in `~/.deadline/config` are read as a temporary fallback if not specified. This fallback will be removed in a future release. (#920)

## 0.29.0 (2026-03-16)

### BREAKING CHANGES
* Job attachment downloads and uploads now run as the job user instead of the worker agent user. Previously available as the opt-in `ASSET_SYNC_JOB_USER_FEATURE` feature flag, this behavior is now the default and the legacy code path has been removed. Attachment sync operations are now run as OpenJD session actions under the job user's OS identity (`jobRunAsUser`).
* The `ASSET_SYNC_JOB_USER_FEATURE` environment variable is no longer recognized. The feature flag module has been removed as this behavior is now always enabled.
* The following APIs have been removed in favour of the new OpenJD-based attachment actions:
  * `SyncInputJobAttachmentsAction` — replaced by `AttachmentDownloadAction`
  * `SyncInputJobAttachmentsActionType` — replaced by `AttachmentDownloadActionType`
  * `Session.sync_asset_inputs()` — attachment downloads are now handled by `AttachmentDownloadAction` running as an OpenJD session action
  * `Session._sync_asset_outputs()` — attachment uploads are now handled by `AttachmentUploadAction` running as an OpenJD session action

### Features
* remove deprecated sync input and output code path replaced by attachment download and upload (#877) ([`ddb37e1`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/ddb37e1ee34e6f8afed0aec70637310a02e6a8b2))

### Bug Fixes
* **installer, windows**: SIDs with no LSA rights could fail the install on non-english locales (#891) ([`3182322`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/318232239cb60cb29e012b312a75786971001115))


## 0.28.21 (2026-01-14)


### Features
* This is the first Worker Agent release which fully supports [OpenJD chunking](https://github.com/OpenJobDescription/openjd-specifications/blob/mainline/rfcs/0001-task-chunking.md).
* Report output job attachment manifest locations to the service.(#865) ([`d748589`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/d7485896b739c2d5017638a15d294cea5c8fae23))


### Bug Fixes
* Worker fails to detect and upload output files when job outputs have file permissions scoped to exclude the worker-agent user. (#865) ([`d748589`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/d7485896b739c2d5017638a15d294cea5c8fae23))



## 0.28.20 (2025-12-29)


### Features
* Added telemetry for filesystem type detection (#843) ([`ddee082`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/ddee0827a127c1b74599d934f653a2631558ebf8))

### Bug Fixes
* Fixed false positive "low transfer rate" failures when syncing job attachments with many small files (#805)
 ([`3af802b`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/3af802b0a2204864ee3093834e1916ff87100656))


## 0.28.19 (2025-12-09)

### Experimental
These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - Improve logging for attachment upload and download (#826) ([`557d6f5`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/557d6f5a07702ccef346e0807ef97adbfffd05a3))

* ASSET_SYNC_JOB_USER_FEATURE - Escape special characters in output directory patterns when generate diff for upload (#828) ([`7022f96`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/7022f962d05d6f079180d419d93794a18e8ba3cf))


## 0.28.18 (2025-11-13)

### Experimental
These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - job attachment upload failures when session environment changes python search path (#810) ([`8a7806d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/8a7806dd20cd49b2be202ae4052eeee4fd39f506))


## 0.28.17 (2025-10-31)

### Experimental
These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - Job attachments sync output processing information was not being logged (#800) ([`a1f103d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/a1f103dec6999aea2fe3a442cff5e2385ddc699b))


## 0.28.16 (2025-10-28)


### Features
* add DEADLINE_STEP_ID to task run env vars (#787) ([`4ee307d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4ee307d1227dc8629cc32ff4cd3c23e516d92ff3))

### Experimental
These changes are experimental and only available through the use of feature flags

* MANIFEST_REPORTING_FEATURE - manifests being reported without job attachments (#789) ([`49b175a`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/49b175a4b2b061b2f4c4eaf542aaa53306bdba51))


## 0.28.15 (2025-10-23)

### Features
* support optional task id in task run session actions (#758) ([`dece265`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/dece26572600d47c26db937bcdecb4ae894e72ac))

### Experimental
These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - job attachments does not fall back to COPIED when launching VFS fails (#753) ([`f0f2696`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/f0f269629cd5c8ca88b4fde8b0850a9d42525735))
* ASSET_SYNC_JOB_USER_FEATURE - job attachment output manifest upload uses current time to construct s3 partition (#767) ([`84bb52d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/84bb52d7c193a2e3fc72c712fb2fda25ae93e3f5))
* ASSET_SYNC_JOB_USER_FEATURE - add progress reporting and low transfer rate check to sync input attachments (#759) ([`5504ff3`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/5504ff38fba53664295a14617e781c64dd76323f))
* ASSET_SYNC_JOB_USER_FEATURE - enhance download process by using WorkerManifestProperties and download_files_from_manifests (#755) ([`0272a46`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/0272a468157b893165d5f855d52b5c50e7d43e96))
* ASSET_SYNC_JOB_USER_FEATURE - enhance upload process by using WorkerManifestProperties and upload_assets (#762) ([`73ea2d7`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/73ea2d7e270567e4a1f8ee104d9a1ad4442797d2))
* ASSET_SYNC_JOB_USER_FEATURE - attachment uploads all as output when no output relative directories defined for a root (#780) ([`3b3d8ec`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/3b3d8ec462ab8ac0fb44a35b24416432d36d5804))
* MANIFEST_REPORTING_FEATURE - not reporting manifests in progress updates (#779) ([`c0ffe77`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/c0ffe77ac884235c79bf68561f4f7880abf331a1))


## 0.28.14 (2025-09-15)


### Features
* macOS worker can stop session processes and shutdown host (#720) ([`4524bf4`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4524bf4e503f4d8c0610b954f81534d289244c2b))

### Experimental
These changes are experimental and only available through the use of feature flags
* MANIFEST_REPORTING_FEATURE - Add worker session data model for job attachment sync operations (#727) ([`1f3e29c`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/1f3e29c04ddf6f4251253fb0ab88fafae58efe88))

## 0.28.13 (2025-07-23)


### Features
* Enable `TASK_CHUNKING` and `REDACTED_ENV_VARS` OpenJobDescription extensions by default. (#691) ([`5564caa`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/5564caa73b25492c98154ea62dcb33ec121a1ef7))

### Experimental
These changes are experimental and only available through the use of feature flags
* MANIFEST_REPORTING_FEATURE - add output manifest reporting (#663) ([`6ac9789`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/6ac97892055ef6bdf7688f002abdffd68d0d85d1))
* ASSET_SYNC_JOB_USER_FEATURE - avoid embedded file name conflicts with download/upload script (#690) ([`2b54166`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/2b54166695ed3afc8f09de8c6b736a5ed28e1023))
  If a file named either `upload.py` or `download.py` was uploaded by a user, this would break the job attachments feature.




## 0.28.12 (2025-06-24)


### Dependencies
* Update openjd-sessions requirement from ==0.10.2 to ==0.10.3 (#668) ([`a19e03e`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/a19e03ebd522c6c0ad815522bd3c39b8ae553f97))


## 0.28.11 (2025-06-13)



### experimental
These changes are experimental and only available through the use of feature flags
* ASSET_SYNC_JOB_USER_FEATURE - VFS not launching for Job Attachment VIRTUAL (#669) ([`59e3d4a`](https://github.com/aws-deadline/deadline-cloud-worker-agent/

## 0.28.10 (2025-06-09)



### Features
* Add GPU Utilization Metrics in Worker Host Metric Logging (#657) ([`99b30b8`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/99b30b8c3ad67ab5ecd401089079833fec4a78f1))

### Experimental
These changes are experimental and only available through the use of feature flags
* ASSET_SYNC_JOB_USER_FEATURE - Error sync inputs due to no path mapping rule found for the source path (#661) ([`f7ee2a0`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/f7ee2a0f3202b097bc383224c5ea22392ff0d8fc))

## 0.28.9 (2025-06-05)


### Experimental
These changes are experimental and only available through the use of feature flags
* ASSET_SYNC_JOB_USER_FEATURE - Job output download takes old session action output when there is a rerun (#641) ([`d42efc5`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/d42efc52f30ef56a6b24e6d6c529deb74c7adc3b))

## 0.28.8 (2025-06-04)



### Bug Fixes
* Disk Read/Write Bytes per Second Metrics Would Always Increase (#653) ([`e56a58a`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/e56a58a7b04a772bf844904486714f3dad5619d4))

### Experimental
These changes are experimental and only available through the use of feature flags
* ASSET_SYNC_JOB_USER_FEATURE - Job Attachments would repeatedly upload output from previous tasks in the same session (#631) ([`a0c504a`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/a0c504a8e011a661a4f348ed83a6a4373d1669c0))

## 0.28.7 (2025-05-21)


### Features
* Host Configuration feature launch (#636) ([`dd2071d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/dd2071d1d01d6cd811531bc1d689c114a7e760e5))
* Add initial support for chunkInt parameter type (#606) ([`4c517b4`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4c517b4a37c35ee4cc8c1ba79bfc6bc1cb29f250))

### Bug Fixes
* Worker agent exits on startup if host config log pre-exists with non-UTF8 contents (#634) ([`6466c4f`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/6466c4fbd509867f877a17af1a4a95693590808b))
* Worker Agent crashes on corrupted cached credentials (#614) ([`7f1ec05`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/7f1ec05d86035febd68daffa603d8da404b8854a))

### Experimental

These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - outputs would fail to upload with `NonValidInputError` when merging multiple manifests (#612) ([`4c12664`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4c126648a657df83e065c77274c965b8d0ad283e))
* ASSET_SYNC_JOB_USER_FEATURE - input files that were modified during a task run were mistakenly uploaded as output (#612) ([`4c12664`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4c126648a657df83e065c77274c965b8d0ad283e))


## 0.28.6 (2025-05-01)


### Experimental

These changes are experimental and only available through the use of feature flags

* HOST_CONFIGURATION_FEATURE - Prevent Windows ACL inheritance for host config script and log and grant full control to Administrators ([`98dfbfc`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/98dfbfc9dbebdb02145c8413a76fe82f45635c58))
* HOST_CONFIGURATION_FEATURE - delimit host configuration script with banners in worker logs and shutdown host on failure ([`f08bca8`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/f08bca8656488d9af992b1dca4b4969f5bdf96b6))


## 0.28.5 (2025-04-11)


### Experimental

These changes are experimental and only available through the use of feature flags

* HOST_CONFIGURATION_FEATURE - run admin host configuration scripts once worker becomes STARTED (#601) ([`d925c65`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/d925c65a052579675443d004295c2b70f019fe9b))


## 0.28.4 (2025-04-03)




## 0.28.3 (2025-03-13)



### Bug Fixes
* unexpected GPU memory configurations crash worker agent (#574) ([`b423bfb`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/b423bfb68e258c2b86e90925669572691ccab2c3))

## 0.28.2 (2025-03-11)



### Bug Fixes
* worker agent unable to start without EC2 metadata access (#572) ([`5e73148`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/5e7314839a3cd9da7bfa50ab9f4198fcabb5edf4))

## 0.28.1 (2025-03-05)



### Bug Fixes
* install-deadline-worker on Windows creates session root directory without read and traversal permissions for Users (#563) ([`29e7aee`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/29e7aee77ab362094b219dd5ec20920a1b859078))

### Experimental

These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - **fix**: task with step-step dependency upload job inputs as outputs (#529) ([`abe0fb0`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/abe0fb0f2c4b04de1490943502f83061e160d938))


## 0.28.0 (2025-02-27)
**This Release has been pulled from PyPI. The use of this release contains a regression in default permissions for the session directory on Windows. Downgrade to 0.27.5 or upgrade to the next release if available.**

### BREAKING CHANGES
* A defect was fixed in the OpenJobDescription specification ([OpenJobDescription/openjd-specifications#70](https://github.com/OpenJobDescription/openjd-specifications/issues/70)) which causes a breaking change to Worker Agent behaviour. Environment exits previously had no default timeout and they now have a default timeout of 5 minutes. To have long-running environment exit actions, job templates can specify a large timeout value when defining environment exit actions in a job or environment template.

### Features
* configurable session root directory (#513) ([`dd541a1`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/dd541a12eac344e109ca283166f978a20af8439c))

### Bug Fixes
* more robust config file modifications in install-deadline-worker (#512) ([`1060b4b`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/1060b4bd29890c0b76ece8665b0415cb5a3303e2))
* worker state file saved on AMI causes multiple instances sharing same worker ID (#527) ([`ec001c9`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/ec001c9830b0fd7639248a2b2bc892787993c5b2))
* skip attachment upload when outputs not modified (#524) ([`972df4e`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/972df4ead8311967ad26844f0445859cae9ae483))
* update log kind to differentiate sync input and sync output (#523) ([`af28588`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/af285880ae66c697534aabae2f28fbc626873fa8))

## 0.27.5 (2024-12-14)


### Features
* directly send cancel OS signals on Linux (#479) ([`a0fc35c`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/a0fc35c419bba964ffa5146f8bb65c54064fc929))

### Bug Fixes
* increase e2e test instance size (#468) ([`2f7cc57`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/2f7cc57169d710f3da21412ae0f40f5b1e171f05))

### Experimental

These changes are experimental and only available through the use of feature flags

* ASSET_SYNC_JOB_USER_FEATURE - run job attachment output upload as job user (#495) ([`678f29a`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/678f29a6ae86265b6a0f912cdcc52a2ceccb7b62))
* ASSET_SYNC_JOB_USER_FEATURE - integrate with job attachment download cli as a openjd action run (#476) ([`07abc76`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/07abc76dc9bdd46a7281a6349b1bb7eaaa808810))

## 0.27.4 (2024-10-30)


### Features
* include specific session runtime logs in the worker log (#422) ([`be55928`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/be55928bb55ca361a2900b16f599ccc1f1b03b7c))

### Bug Fixes
* non-user-friendly error when trying to install the worker agent as domain user (#457) ([`15afe89`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/15afe89930f99f92346a4b96806c4c68d76bdbae))

## 0.27.3 (2024-10-17)



### Bug Fixes
* crash on startup when host has multiple NVIDIA GPUs (#435) ([`760118c`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/760118c0ab8157ecc5087fc919f6773b0cd6c376))
* vague error message when no AWS region specified (#413) ([`0d5ccad`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/0d5ccadebab5aca5b562abf6d8032aa79ca51678))
* WindowsPath is not JSON serializable during session cleanup (#412) ([`5d5055c`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/5d5055cf728a98769415a30a923eea057b8d9683))
* Ensure scheduler drain and status update on Windows service shutdown (#408) ([`f269b67`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/f269b67d415e0203a49b9f85bf8b811b345f96e6))
* Agent logs to `/var/log/messages` when running as a service on Linux (#396) ([`b2368ed`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/b2368edeaec80b6c31132195dbdf4579b5e7e225))
* `--run-jobs-as-agent-user` crashes on windows (#395) ([`6fef296`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/6fef2969ef5cd4eb12adc76eded4587f447c0726))

## 0.27.2 (2024-08-13)


### Features
* add job user override for windows (#372) ([`84c83bd`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/84c83bdffbce7b44f46668cce3ec36ac596fab5d))
* add success or fail metric on Job Attachment calls (#352) ([`ac1e5e0`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/ac1e5e0d4b7ba8c85ec868c84ee5cabf4980f091))
* add deadline client lib, OpenJD Sessions to boto3 header (#351) ([`51fc0f1`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/51fc0f10b02fde81904b4688e20c91adb83c3c8e))

### Bug Fixes
* install-deadline-worker doesn't create queues persistence dir on Windows (#377) ([`bd40074`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/bd40074f3117d78e2080b01bb439cc1f146091eb))
* posix installer succeeds if agent-user does not already exist (#378) ([`17435b1`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/17435b15941305baf09ac772eeb6723a66ec7902))
* jobRunAsUser always removed in BatchGetJobEntity JobDetails (#349) ([`b6a64de`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/b6a64ded6b725822d097ac9fad8f12c4e63e9388))
* fail on BatchGetJobEntity jobRunAsUser validation with a job user override  (#346) ([`c0dd3b3`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/c0dd3b35db2faef3296277cbf66e3dc42adeb80f))
* install-deadline-worker on Linux assumes agent os group matches username (#345) ([`4ed1136`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/4ed1136e1ed6e5d7cdfcbd9cb5d0848ff4bd316e))
* error due to out-of-range process exit code (#339) ([`7d4ec30`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/7d4ec30f15af952cdcf03d3c28d9ce9608d861ab))

## 0.27.1 (2024-05-01)

### Dependencies
* update deadline requirement from ==0.47.* to ==0.48.* (#306) ([`01326f7`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/01326f78a93f76386b4a0ef8253909127726c66f))


## 0.27.0 (2024-04-10)

### BREAKING CHANGES
* differentiate step actions from non-step actions in logs (#292) ([`a6d55e3`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/a6d55e3849cd4c374e91d184ed3ad40bd88d491f))


### Bug Fixes
* handle case where BatchGetJobEntity returns no jobRunAsUser (#293) ([`616e16c`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/616e16c293ae544fe948528b485a6892c36bf0a6))

## 0.26.1 (2024-04-03)



### Bug Fixes
* provide region in queue AWS configuration (#289) ([`efeecfe`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/efeecfe354cdfe321fe96718ed51704e67ab847a))
* stop the running Windows service before re-installation. (#288) ([`0587b60`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/0587b6026622424584e46717561d6ee61879bbba))

## 0.26.0 (2024-04-02)

### BREAKING CHANGES
* public release (#271) ([`ed1e14d`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/ed1e14df818e8161490a5c6b884320eeb7b6832e))
* remove deprecated features (#277) ([`d984094`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/d984094f74b9ad60dd0bc82ec3eb917cda4e138d))


### Bug Fixes
* example config file inaccurately documents allow_ec2_instance_profile (#278) ([`1d1ecc1`](https://github.com/aws-deadline/deadline-cloud-worker-agent/commit/1d1ecc165384c6f14ace4465236460ba12b176ee))

## 0.25.2 (2024-03-29)




## 0.25.1 (2024-03-28)


### Features
* adds data on action kind and queue length to logs (#266) ([`bb10c47`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/bb10c4758dab094738b23859a3e8aae64fac4850))

### Bug Fixes
* agent not logging events with emojis on Windows due to default encoding (#267) ([`1008083`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/10080837558bc369c35243b5c15af82d45e35467))

## 0.25.0 (2024-03-27)

### BREAKING CHANGES
* remove time field from structured logs (#263) ([`d246abf`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/d246abf1595d07e5cb99bb9a451d3fb7162baf2f))


### Bug Fixes
* unhandled exception unloading user profile (#264) ([`62a404b`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/62a404b0d9db8d95e02b4e977d4a343f2444cc00))

## 0.24.0 (2024-03-26)

### BREAKING CHANGES
* overhaul agent logging to introduce structured logs (#216) ([`abed8c9`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/abed8c95c932f0890eb03f5ed383ce8def3a37dc))
* **installer**: allow ec2 instance profile by default (#259) ([`7e4d947`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/7e4d9474bd96d1d0b7a7ba749acca80d1266b0a3))
* **installer**: detect default AWS region on EC2 (#250) ([`3db8685`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/3db86851bc9bcc9afec33c9fdd1b981897266b12))

### Features
* aws config directory managed by agent (#254) ([`2f4fd8a`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/2f4fd8a7a2431f8b4bbbfd92cbc435095be8278b))

### Bug Fixes
* handle OSError when detecting GPU capabilities (#255) ([`677fda6`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/677fda63e51a8c436c47faf9650b147a462b2a31))
* handle IMDS disruptions gracefully (#249) ([`ea6b701`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/ea6b70102add121362da7009724146c604ebfa45))

## 0.23.1 (2024-03-23)


### Features
* cleanup asset_sync session with os_user (#251) ([`b68922e`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/b68922e37d95c37b3eec88b990d88b7fba60875e))
* **windows-installer**: configure AWS region in Windows service (#242) ([`adf164f`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/adf164ff56b56bc48837012d99bc086b1120193a))

### Bug Fixes
* return sessionactions if Session is stopped (#252) ([`1230d89`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1230d89152ae72bae77747da32e551dbc92c9c98))
* insufficient Windows ACLs for job user AWS config files (#246) ([`f5e2f52`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/f5e2f52aa747976f15540a6f0da561a0f7faa57d))

## 0.23.0 (2024-03-21)

### BREAKING CHANGES
* Fail jobs configured to run as worker agent on Windows (#230) ([`7ce01a8`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/7ce01a8731005178a6da964c24870b3fc9d57f03))

### Features
* agent logs contain more verbose API request/response information (#215) ([`6e8e566`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/6e8e566dadfd39c77737ac5243276256f87d492c))
* session cleanup on windows (#212) ([`93f4305`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/93f43053ccd498fd913b31b8aa96c4d43fae7157))
* **windows-installer**: grant and validate agent user permissions (#206) ([`0d8e3de`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/0d8e3de5ee4ccdadffbbc365c2e822ff62efc0fd))
* Add telemetry event for uncaught exceptions (#203) ([`9a17a07`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/9a17a0786fe47b1e0ad0d69ee55c0746823a95a5))

### Bug Fixes
* NEVER_ATTEMPTED session actions should not report startedAt or endedAt (#237) ([`99fd7d3`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/99fd7d385dedc25c22aeaecee7247bef3e682fa0))
* improve logging when getting secret (#184) ([`d3e2e0c`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/d3e2e0c863efd2af8b1eeb0a6f3173b7c6b6a23d))
* increasing default password length for windows users (#236) ([`c411c85`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c411c851395fa6a46a174d9709b3b907dd9796f1))
* change windows logon type from NETWORK to INTERACTIVE (#234) ([`82a5c11`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/82a5c11195102e3334961b4420915feccfc849b0))

## 0.22.1 (2024-03-19)

### Bug Fixes
* change OpenJD&#39;s session root directory to /sessions (#222) ([`3b68342`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/3b68342bc12307e63f84214b310ed616437c1c8e))

## 0.22.0 (2024-03-18)

### BREAKING CHANGES
* retool scale-in behaviour (#193) ([`40390e9`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/40390e92d3e799f5299233b6d030a9e66582e18c))

### Features
* windows service (#207) ([`1d97970`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1d979709941e2c2b116cb7932d664a64584a95d4))
* **windows-installer**: add client telemetry opt out option (#210) ([`7551869`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/7551869124f8a7ef219888b52e472f632ec68b0d))
* windows support (#205) ([`80e8ec4`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/80e8ec423ced2130792d95af7690bb9b64b77565))
* change OpenJD&#39;s session directory path, and add `--retain-session-dir` command option (#196) ([`091608c`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/091608c65b50e6fecac94dfff4e2f088c1f49926))

### Bug Fixes
* improve error messaging for Windows logon (#219) ([`de23226`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/de232262d503da01f5f1aa974d985555fe51008a))
* **install.sh**: update ownership and permissions for session root directory (#201) ([`230b73c`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/230b73c5c6c472256db68ab43ddd83203889d245))

## 0.21.2 (2024-03-07)


### Features
* Add job and session metadata to the environment of a job (#189) ([`92b6d17`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/92b6d17e91cfd19981f0e19d66c47e614150eb44))

### Bug Fixes
* complete all actions following unsuccessful actions as NEVER_ATTEMPTED (#190) ([`d266c0f`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/d266c0f9740bd82d4ca85b7de2031e68edfd8b77))

## 0.21.1 (2024-02-28)


### Features
* Cancel Job Attachments session action when transfer rates drop below threshold (#143) ([`c49bbb4`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c49bbb498949e3ed2b469714717018669134d5c2))

### Bug Fixes
* handle non-existent queue jobRunAsUser on worker host (#176) ([`1049a48`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1049a48f0ff045160f5524eb25c3f97a42114fcb))

## 0.21.0 (2024-02-23)

### BREAKING CHANGES
* Terminating all VFS processes when cleaning up session (#149) ([`50178ed`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/50178ede860949b766f85ef7f3e0c586ce8bc8e9))

### Features
* report action timeout as failed with timeout message (#165) ([`ff36123`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/ff3612387f9b010359480367bea62f67235be3aa))
* provision ownership on /var/lib/deadline/credentials directory (#145) ([`3b3e7af`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/3b3e7af471ab4f7947163f8978d2d9de0baad091))

### Bug Fixes
* Set shutdown_on_stop value in config file. (#164) ([`858f621`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/858f621e4939b0b5bc32262bc90cc4fa80dd148e))
* permissions on /var/log/amazon directory (#162) ([`1acfffc`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1acfffc69040061b3e9fa02894e2a7a7cbab204d))
* no longer sigterm agent when running jobs as same user (#161) ([`fe12ad3`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/fe12ad32076a3ce2a87ae84f1dbe46a4fc8c0121))
* don&#39;t invert shutdown_on_stop config file setting&#39;s meaning (#155) ([`1a7329f`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1a7329f039ad9c164c0ee2c5c05e333462fdf892))

