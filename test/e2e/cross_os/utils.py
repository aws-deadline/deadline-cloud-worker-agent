# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import sys


def get_operating_system_name() -> str:
    if sys.platform == "win32":
        return "windows"
    else:
        return "linux"
