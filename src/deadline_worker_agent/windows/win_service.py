# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import socket
import logging
from threading import Event

import win32serviceutil
import win32service
import servicemanager

from ..startup.entrypoint import entrypoint


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

    def SvcShutdown(self):
        """Invoked when the system is shutdown"""
        self.SvcStop()

    def SvcDoRun(self):
        """The main entrypoint called after the service is started"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        entrypoint(cli_args=[], stop=self._stop_event)
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STOPPED,
            (self._svc_name_, ""),
        )
        logger.info("Stop status sent to Windows Service Controller")


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(WorkerAgentWindowsService)
