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

