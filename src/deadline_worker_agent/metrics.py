# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from logging import Logger, getLogger
from threading import Event, Thread
from typing import Any, Dict

import os
import psutil
import subprocess

from .log_messages import MetricsLogEvent, MetricsLogEventSubtype

module_logger = getLogger(__name__)


class HostMetricsLogger:
    """Context manager that regularly logs host metrics"""

    # How long to wait for the metrics thread to exit during shutdown before abandoning it.
    # Kept short because it is serial latency on every shutdown path, and only ever paid when
    # a collection is in flight -- the stop event wakes an idle thread immediately.
    JOIN_TIMEOUT_S = 1.0

    # How long to wait for nvidia-smi, so an unresponsive driver cannot keep the metrics
    # thread from observing the stop event. Bounds a slow nvidia-smi, not a wedged one -- on
    # timeout subprocess waits on the killed child unbounded -- which is why the thread is a
    # daemon and the shutdown join is bounded.
    GPU_QUERY_TIMEOUT_S = 5.0

    logger: Logger
    interval_s: float
    _thread: Thread | None
    _stop_event: Event
    _prev_network: Any | None
    _prev_disk_counters: Any | None
    _host_has_no_gpu: bool | None = None

    def __init__(self, logger: Logger, interval_s: float) -> None:
        assert interval_s > 0, "interval_s must be a positive number"
        self._thread = None
        self._stop_event = Event()
        self._prev_network = None
        self._prev_disk_counters = None
        self.logger = logger
        self.interval_s = interval_s

    def __enter__(self) -> HostMetricsLogger:
        # Re-entry is not expected, but stop any running thread rather than replacing the only
        # references to it: its successor overwrites both _thread and _stop_event, leaving
        # nothing able to signal or join it, and it would log alongside its successor forever.
        if self._thread:
            self.__exit__(None, None, None)

        # A fresh event, handed to the thread explicitly rather than read back off self:
        # if a previous __exit__ gave up waiting and abandoned a thread that was still
        # running, that thread keeps observing its own already-set event and exits, rather
        # than being revived by a reset event and double-logging alongside its successor.
        stop_event = Event()
        self._stop_event = stop_event
        thread = Thread(
            target=self._run,
            args=(stop_event,),
            name="HostMetricsLogger",
            daemon=True,
        )
        try:
            thread.start()
        except RuntimeError as e:
            # Host metrics are best-effort observability, so degrade to logging no metrics
            # rather than failing the Worker when the host cannot spare a thread.
            module_logger.warning(
                f"Failed to start the host metrics thread. Host metrics will not be logged. "
                f"Error: {e}"
            )
        else:
            self._thread = thread
        return self

    def __exit__(self, type, value, traceback) -> None:
        self._stop_event.set()
        if self._thread:
            # Bounded join so that an unresponsive metrics collection (e.g. a hung
            # nvidia-smi) cannot block Worker shutdown. The thread is a daemon, so it
            # will not keep the process alive if it outlives this join.
            self._thread.join(timeout=self.JOIN_TIMEOUT_S)
            if self._thread.is_alive():
                module_logger.warning(
                    "Host metrics thread did not exit within "
                    f"{self.JOIN_TIMEOUT_S} seconds. Abandoning it."
                )
            self._thread = None

    def _run(self, stop_event: Event) -> None:
        self._prime_metrics()
        while not stop_event.wait(self.interval_s):
            try:
                self.log_metrics()
            except Exception as e:
                # Never let an unexpected error end the metrics thread; a single bad
                # collection should not silently stop host metrics for the lifetime of
                # the Worker.
                module_logger.warning(f"Failed to log host metrics. Error: {e}")

    def _prime_metrics(self) -> None:
        """
        Establishes the baselines that the first logged sample is measured against.

        psutil tracks non-blocking CPU samples per thread, so priming has to happen on this
        long-lived thread: without it every sample would be that thread's first call and
        report psutil's uninitialized zero. The network and disk counters are primed here too,
        so the first logged sample reports measured rates rather than zeroes.

        Note that a sample covers one interval plus however long the collection itself takes,
        because the wait starts after log_metrics returns, while the reported rates divide by
        interval_s alone. Pre-existing, and small at the default interval.
        """
        try:
            psutil.cpu_percent()
            self._prev_network = psutil.net_io_counters(nowrap=True)
            self._prev_disk_counters = psutil.disk_io_counters(nowrap=True)
        except Exception as e:
            module_logger.warning(
                f"Failed to prime host metrics baselines. The first host metrics log message "
                f"may report zeroed rates. Error: {e}"
            )

    def _get_gpu_metrics(self) -> Dict[str, str]:
        """
        Get GPU metrics using nvidia-smi.

        Returns:
            Dict[str, str]: A dictionary of GPU metrics or empty dict if nvidia-smi is not available.
        """
        if self._host_has_no_gpu:
            return {}

        gpu_metrics = {}

        try:
            # Query GPU metrics for all GPUs
            metrics_to_query = [
                "utilization.gpu",
                "memory.used",
                "memory.total",
                "utilization.memory",
            ]

            query_str = ",".join(metrics_to_query)

            # Query GPU metrics
            output = subprocess.check_output(
                ["nvidia-smi", f"--query-gpu={query_str}", "--format=csv,noheader,nounits"],
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=self.GPU_QUERY_TIMEOUT_S,
            )

            # Variables to sum metrics across GPUs
            gpu_util_sum = mem_used_sum = mem_total_sum = mem_util_sum = valid_gpu_count = 0.0

            # Process each GPU
            for line in output.strip().split("\n"):
                try:
                    gpu_util, mem_used, mem_total, mem_util = (
                        float(v.strip()) for v in line.split(",")
                    )
                except ValueError:
                    module_logger.debug(
                        "nvidia-smi output was not able to be parsed into GPU utilization metrics"
                    )
                    continue

                mem_used_sum += mem_used
                mem_total_sum += mem_total
                mem_util_sum += mem_util
                gpu_util_sum += gpu_util
                valid_gpu_count += 1

            # Calculate consolidated metrics
            if valid_gpu_count > 0:
                avg_gpu_util = round(gpu_util_sum / valid_gpu_count, 1)
                gpu_metrics["gpu-utilization-percent"] = str(avg_gpu_util)

                gpu_metrics["gpu-memory-used-mib"] = str(int(mem_used_sum))
                gpu_metrics["gpu-memory-total-mib"] = str(int(mem_total_sum))
                avg_mem_used_percent = round((mem_used_sum / mem_total_sum) * 100, 1)
                gpu_metrics["gpu-memory-used-percent"] = str(avg_mem_used_percent)

                avg_mem_util = round(mem_util_sum / valid_gpu_count, 1)
                gpu_metrics["gpu-memory-utilization-percent"] = str(avg_mem_util)
        except subprocess.TimeoutExpired:
            # Returned without latching _host_has_no_gpu so that a single unresponsive
            # nvidia-smi does not disable GPU metrics for the lifetime of the process.
            module_logger.debug(
                f"nvidia-smi did not respond within {self.GPU_QUERY_TIMEOUT_S} seconds, "
                "skipping GPU metrics collection"
            )
            return {}
        except FileNotFoundError:
            module_logger.debug("nvidia-smi not found, skipping GPU metrics collection")
        except subprocess.CalledProcessError:
            module_logger.debug("Error running nvidia-smi, skipping GPU metrics collection")
        except Exception as e:
            module_logger.debug(f"Unexpected error collecting GPU metrics: {e}")
        if not gpu_metrics:
            self._host_has_no_gpu = True

        return gpu_metrics

    def log_metrics(self) -> None:
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
            gpu_metrics = self._get_gpu_metrics()
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
                # Computed from the root-based total/used values reported above rather than
                # using psutil's disk.percent, which is measured against user-available
                # space and so would not agree with the other total-disk-* metrics.
                "total-disk-used-percent": str(round(disk.used / disk.total * 100, ndigits=1)),
                "user-disk-available-bytes": str(disk.free),
                "network-sent-bytes-per-second": network_sent,
                "network-recv-bytes-per-second": network_recv,
                "disk-read-bytes-per-second": disk_read,
                "disk-write-bytes-per-second": disk_write,
            }

            # Add GPU metrics to stats
            stats.update(gpu_metrics)

            self.logger.info(MetricsLogEvent(subtype=MetricsLogEventSubtype.SYSTEM, metrics=stats))
