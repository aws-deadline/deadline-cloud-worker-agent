# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import socket
import logging
from functools import cache
from threading import Event

import win32process
import win32serviceutil
import win32service
import win32ts
import servicemanager

from deadline_worker_agent.startup.entrypoint import entrypoint


logger = logging.getLogger(__name__)


class WorkerAgentWindowsService(win32serviceutil.ServiceFramework):
    # Pywin32 Service Configuration
    _exe_name_ = "DeadlineWorkerService.exe"
    _svc_name_ = "DeadlineWorker"
    _svc_display_name_ = "AWS Deadline Cloud Worker"
    _svc_description_ = (
        "Service hosting the AWS Deadline Cloud Worker Agent. Connects to AWS "
        "Deadline Cloud and runs jobs as a worker in a fleet."
    )

    _stop_event: Event

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self._stop_event = Event()
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        """Invoked when the Windows Service is being stopped"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        logger.info("Windows Service is being stopped")
        self._stop_event.set()

    def SvcDoRun(self):
        """The main entrypoint called after the service is started"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        try:
            entrypoint(cli_args=[], stop=self._stop_event)
        except Exception as e:
            logging.exception(e)
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STOPPED,
            (self._svc_name_, ""),
        )
        logger.info("Stop status sent to Windows Service Controller")


def _get_current_process_session() -> int:
    """Returns the Windows session ID number for the current process

    Returns
    -------
    int
        The session ID of the current process
    """
    process_id = win32process.GetCurrentProcessId()
    return win32ts.ProcessIdToSessionId(process_id)


@cache
def is_service() -> bool:
    """Returns whether the current Python process is running in a Windows Service.

    This check is done by getting the Windows Session ID of the current process.
    All Windows Service processes run in session 0.

    Returns
    -------
    bool
        True if the current process is running in a Windows Service
    """
    return _get_current_process_session() == 0


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(WorkerAgentWindowsService)
