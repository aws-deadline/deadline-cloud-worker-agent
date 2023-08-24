# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from .startup.entrypoint import entrypoint


def init():
    """
    This function calls the entrypoint if the deadline_worker_agent package is
    the Python entrypoint.

    The function only exists for test-ability - otherwise this code would be
    inlined into the module itself.
    """

    if __name__ == "__main__":
        entrypoint()


init()
