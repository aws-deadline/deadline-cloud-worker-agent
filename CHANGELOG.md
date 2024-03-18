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

