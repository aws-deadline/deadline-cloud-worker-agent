## 0.21.0 (2024-02-22)

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

## 0.20.1 (2024-01-30)




## 0.20.0 (2024-01-24)

### BREAKING CHANGES
* adds compatibility with upcoming BatchGetJobEntity API change (#139) ([`5197e57`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/5197e5767a5134e192602ecc85d5d98f0f32f24e))



## 0.19.0 (2024-01-16)

### BREAKING CHANGES
* **api**: remove old jobsRunAs (#44) ([`fab7d53`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/fab7d538d598285d8385779e1b82114565ea0e8f))

### Features
* include Queue ID in sync_inputs, sync_outputs telemetry (#135) ([`c3579af`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c3579af41735715e74acfec56b8be87a6995ecac))
* compatibility with BatchGetJobEntity API changes for jobRunAsUser (#133) ([`08d7ba6`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/08d7ba6ebd219e202f545885937d0ed02aee9811))
* rename &#34;updateTime&#34; to &#34;updatedAt&#34; in UpdateWorkerSchedule API request (#131) ([`f607954`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/f607954112b7f1298b95afc3b1dc16322ce6bfbe))

### Bug Fixes
* crash if queue has no user defined (#127) ([`3b62715`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/3b6271590517820e2d187a853fbfa023cf306a09))
* job attachment output upload blocking UpdateWorkerSchedule API requests (#122) ([`f2b3893`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/f2b38939b2b5936979bdbd8d562fd5454de3dfb6))
* OpenJD architecture capability reporting on ARM (#118) ([`73a2877`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/73a287792381542f68138d137086bf7afd8747c0))

## 0.18.0 (2023-12-13)

### BREAKING CHANGES
* &#34;jobs_run...&#34; -&gt; &#34;run_jobs...&#34; (#93) ([`1cc9f34`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1cc9f3412fc3c2118133560e03705f330d68e168))
* rename impersonation options to use run-as nomenclature (#92) ([`5165a4d`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/5165a4d2ade202b4a85a78473f6d9d160bba1f06))

### Features
* emit job attachment transfer statistics as telemetry events (#104) ([`ccaa097`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/ccaa09759ed9533e5388fa4ecef60fa17cbe098d))
* print versions of 1st party dependencies to worker log (#82) ([`b98be06`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/b98be067266dfdf1f1f52bc3ab3de6eefdd040f8))
* **install**: Adding vfs install path to install parameters (#58) ([`60959c0`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/60959c098eda9bce46f83123b6b0957f6504432e))

### Bug Fixes
* use release environment in release workflow integ tests (#112) ([`3ae3a16`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/3ae3a169ddb3e738bf424597c2156d15714118cb))
* Adding os_user to FileSystemPermissionSettings for use in the deadline vfs (#94) ([`1625ce5`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1625ce51f2691a335bc3a1cd4c06a9a793876698))
* avoid truncating sessionaction logs (#86) ([`47dca3e`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/47dca3e35d332401c4ff365336736717666d0532))

## 0.17.5 (2023-10-30)




## 0.17.4 (2023-10-27)



### Bug Fixes
* mock HostMetricsLogger in entrypoint tests (#70) ([`b9df0a9`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/b9df0a91e8bca4c75ac3571e9b4248800d759158))
* use NEVER_ATTEMPTED after session action failures (#74) ([`5060e16`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/5060e16447a35fa8cf543e54f07b43223e710b7e))

## 0.17.3 (2023-10-27)




## 0.17.2 (2023-10-26)



### Bug Fixes
* handle CONCURRENT_MODIFICATION conflict from UpdateWorkerSchedule (#69) ([`da4da41`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/da4da414c6268d8081526dcfb542ca4aa4fc30af))

## 0.17.1 (2023-10-20)


### Features
* add host metrics logging (#45) ([`1718c57`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1718c575ae73deb5f6a324da78bf7c5f24ab75d8))

### Bug Fixes
* mock host metrics logger in worker module for tests (#64) ([`0628cd2`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/0628cd2be49031268c94529ad88a05e85e2dc193))
* set RunStepTaskAction end time to asset sync completion time (#11) ([`2fd6c47`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/2fd6c47b18175e44c8593aa31899e67109ebe343))
* respect service&#39;s suggested retryAfter when throttled (#39) ([`d83066a`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/d83066adaa7f17990ece7f8e90a026b113bbc1ae))
* add job_run_as_user to integ test (#57) ([`962808f`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/962808fb850df31fcd7827a605d5e331045bc938))

## 0.17.0 (2023-10-10)

### BREAKING CHANGES
* Rename File system options from PRELOAD/ON_DEMAND to COPIED/VIRTUAL ([`fd71650`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/fd71650eec7d1b1431424b1ade565a50f25910df))
* session lifecycle log improvements (#51) ([`f658fff`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/f658fff5c90df32fc0b9d650072b9e32eaf2990c))

### Features
* log completed session actions (#49) ([`bb4e3bd`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/bb4e3bd37d69fc09dd75bca54108ccf4232a7013))


## 0.16.0 (2023-09-29)

### BREAKING CHANGES
* Adds telemetry client calls to worker (#42) ([`1e96738`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1e967381832d45fc173a25dcd8efd0278b206024))



## 0.15.1 (2023-09-28)

### BREAKING CHANGES
* **api**: rename jobsRunAs to jobRunAsUser (#41) ([`c34a3af`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c34a3af543aca788b1362a3d4bc7241322c64189))



## 0.15.0 (2023-09-25)

### BREAKING CHANGES
* **job_attachment**: Change osType to rootPathFormat (#37) ([`5a39c25`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/5a39c251919a3bc72f2a1dfb9830f4a5e4a9fc50))



## 0.14.1 (2023-09-15)



### Bug Fixes
* remove the recursive ownership/permissions changes (#26) ([`1c3a9b5`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/1c3a9b54460a33e973305f8d48dfacbf2c237e7e))
* remove old schema workaround (#32) ([`c37bb47`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c37bb47b818094a90c00c38ffe5b7940fcc6fab3))
* Use status instead of targetStatus in UpdateWorker. (#29) ([`a22e7df`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/a22e7df0dd30e0b9fd4a2ac3f8f495523fd656fb))
* duplicate layers of API request retries and integer overflow in backoff ([`4905915`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/4905915a1d604ae03091d223a638b6c4a9ae0033))

## 0.14.0 (2023-09-06)


### Features
* update to the new job schema ([`9aafbbe`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/9aafbbe57c1c07b19f5db1a6de2d8d135ef0a2ea))
* add --no-install-service to installer and add integration test (#13) ([`4237090`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/4237090ef6ec52e394ad6609f29c99dc6ac3fd94))

### Bug Fixes
* Fixing name mismatch in testing scripts (#19) ([`20c3b72`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/20c3b72894214d0b08a0188f3cf4f0500b3ffd23))
* use the proper entity key variable (#15) ([`8f6f9b4`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/8f6f9b4ada3f94ff57658908c8ec3310a7297f8c))

## 0.13.0 (2023-08-30)

### BREAKING CHANGES
* Support Storage Profiles path mapping rules when syncing inputs/outputs (#10) ([`c6e549d`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/c6e549d2ac05b648365a74d188efd3647c89bbca))


### Bug Fixes
* use try/except/finally clause in SyncInputJobAttachmentsAction, to ensure that the update action happens (#12) ([`0d64cbd`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/0d64cbd6a567029e99537369728b3f1aa62b1882))

## 0.12.1 (2023-08-27)



### Bug Fixes
* Update worker binary to deadline-worker-agent where necessary (#7) ([`044f0fa`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/044f0fa5ce28e9764589373f09c2e07195492764))
* fail installer when worker agent path is not found (#4) ([`f397f80`](https://github.com/casillas2/deadline-cloud-worker-agent/commit/f397f801c2e63e8e8f55432057aa2f71a8f70aed))

## 0.12.0 (2023-08-25)





