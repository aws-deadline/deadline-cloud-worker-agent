# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from logging import Logger, getLogger
from threading import Timer
from typing import Any

import os
import psutil

from .log_messages import MetricsLogEvent, MetricsLogEventSubtype

module_logger = getLogger(__name__)


class HostMetricsLogger:
    """Context manager that regularly logs host metrics"""

    logger: Logger
    interval_s: float
    _timer: Timer | None
    _prev_network: Any | None
    _prev_disk_counters: Any | None

    def __init__(self, logger: Logger, interval_s: float) -> None:
        assert interval_s > 0, "interval_s must be a positive number"
        self._timer = None
        self._prev_network = None
        self._prev_disk_counters = None
        self.logger = logger
        self.interval_s = interval_s

    def __enter__(self) -> HostMetricsLogger:
        self.log_metrics()
        return self

    def __exit__(self, type, value, traceback) -> None:
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def log_metrics(self):
        """
        Queries information about the host machine and logs the information as a space-delimited
        line of the form: <label> <value> ...
        """
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            disk = psutil.disk_usage(os.sep)
            disk_counters = psutil.disk_io_counters(nowrap=True)
            network = psutil.net_io_counters(nowrap=True)
        except Exception as e:
            module_logger.warning(
                f"Failed to get host metrics. Skipping host metrics log message. Error: {e}"
            )
        else:
            # On Windows it may be necessary to issue diskperf -y command from cmd.exe first in order to enable IO counters
            if disk_counters is None:
                disk_read = disk_write = "NOT_AVAILABLE"
            elif not (
                hasattr(disk_counters, "read_bytes") and hasattr(disk_counters, "write_bytes")
            ):
                # TODO: Support disk speed on NetBSD and OpenBSD
                disk_read = disk_write = "NOT_SUPPORTED"
            else:
                if self._prev_disk_counters:
                    disk_read_bps = round(
                        (disk_counters.read_bytes - self._prev_disk_counters.read_bytes)
                        / self.interval_s
                    )
                    disk_write_bps = round(
                        (disk_counters.write_bytes - self._prev_disk_counters.write_bytes)
                        / self.interval_s
                    )
                else:
                    disk_read_bps = disk_write_bps = 0
                disk_read = str(disk_read_bps)
                disk_write = str(disk_write_bps)
            self._prev_disk_counters = disk_counters

            # We need to poll network IO to get rate
            if network is None:
                network_sent = network_recv = "NOT_AVAILABLE"
            else:
                if self._prev_network:
                    network_sent_bps = round(
                        (network.bytes_sent - self._prev_network.bytes_sent) / self.interval_s
                    )
                    network_recv_bps = round(
                        (network.bytes_recv - self._prev_network.bytes_recv) / self.interval_s
                    )
                else:
                    network_sent_bps = network_recv_bps = 0
                network_sent = str(network_sent_bps)
                network_recv = str(network_recv_bps)
            self._prev_network = network

            stats = {
                "cpu-usage-percent": str(cpu_percent),
                "memory-total-bytes": str(memory.total),
                "memory-used-bytes": str(memory.total - memory.available),
                "memory-used-percent": str(memory.percent),
                "swap-used-bytes": str(swap.used),
                "total-disk-bytes": str(disk.total),
                "total-disk-used-bytes": str(disk.used),
                "total-disk-used-percent": str(round(disk.used / disk.total, ndigits=1)),
                "user-disk-available-bytes": str(disk.free),
                "network-sent-bytes-per-second": network_sent,
                "network-recv-bytes-per-second": network_recv,
                "disk-read-bytes-per-second": disk_read,
                "disk-write-bytes-per-second": disk_write,
            }

            self.logger.info(MetricsLogEvent(subtype=MetricsLogEventSubtype.SYSTEM, metrics=stats))
        finally:
            self._set_timer()

    def _set_timer(self) -> None:
        """
        Sets the timer to log the host metrics at a regular interval.

        Args:
            interval_s (float): The interval in seconds to print the host metrics at.
        """
        self._timer = Timer(self.interval_s, self.log_metrics)
        self._timer.start()
