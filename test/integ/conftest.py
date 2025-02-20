# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys

# Ignore platform-specific tests on other platforms
# https://docs.pytest.org/en/stable/example/pythoncollection.html#customizing-test-collection
collect_ignore: list[str] = []
if sys.platform != "win32":
    collect_ignore.append("windows")
elif sys.platform != "linux":
    collect_ignore.append("linux")
