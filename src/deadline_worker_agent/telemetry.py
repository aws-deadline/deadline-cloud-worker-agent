# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Telemetry client for the worker agent.

Sends non-personally-identifiable telemetry events to the Deadline Cloud telemetry
service in the background. Telemetry can be opted out of by setting the environment
variable DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true.
"""

import atexit
import json
import logging
import os
import platform
import random
import time
import uuid
from configparser import ConfigParser
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Full, Queue
from threading import Thread
from typing import Any, Dict, Optional
from urllib import error, request

import boto3

logger = logging.getLogger(__name__)

_TRUE_VALUES = {"true", "yes", "on", "1"}

# Default config file path, matching the deadline client convention
_CONFIG_FILE_PATH = os.path.join("~", ".deadline", "config")
_CONFIG_FILE_PATH_ENV_VAR = "DEADLINE_CONFIG_FILE_PATH"


def _get_config_file_path() -> Path:
    return Path(os.path.expanduser(os.environ.get(_CONFIG_FILE_PATH_ENV_VAR, _CONFIG_FILE_PATH)))


def _read_config() -> ConfigParser:
    config = ConfigParser()
    config_path = _get_config_file_path()
    if config_path.is_file():
        config.read(str(config_path))
    return config


def _get_setting(setting_name: str, config: Optional[ConfigParser] = None) -> str:
    """Read a setting from the deadline config file. Returns empty string if not found."""
    if "." not in setting_name:
        return ""
    section, name = setting_name.split(".", 1)
    if config is None:
        config = _read_config()
    for config_section in config.sections():
        if config_section == section or config_section.endswith(f" {section}"):
            if config.has_option(config_section, name):
                return config.get(config_section, name)
    return ""


@dataclass
class TelemetryEvent:
    event_type: str = "com.amazon.rum.deadline.uncategorized"
    event_details: Dict[str, Any] = field(default_factory=dict)


def _swallow_exceptions(func):
    """Decorator that catches all exceptions in telemetry functions to prevent
    telemetry issues from affecting the main application flow."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.debug(
                "Swallowed exception in telemetry function %s", func.__name__, exc_info=True
            )
            return None

    return wrapper


class TelemetryClient:
    """
    Sends telemetry events to the Deadline Cloud telemetry service in the background.

    Telemetry collection can be opted out of by setting the environment variable
    DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true.
    """

    MAX_QUEUE_SIZE = 25
    BASE_TIME = 0.5
    MAX_BACKOFF_SECONDS = 10
    MAX_RETRY_ATTEMPTS = 4
    ENDPOINT_PREFIX = "management."

    def __init__(self, package_name: str, package_ver: str) -> None:
        self._initialized: bool = False
        self.package_name = package_name
        self.package_ver = ".".join(package_ver.split(".")[:3])
        self._common_details: Dict[str, Any] = {}

        self.session_id: str = str(uuid.uuid4())
        self.telemetry_id: str = self._get_telemetry_identifier()
        self._system_metadata = self._get_system_metadata()
        self._set_opt_out()
        self._initialize()

    @_swallow_exceptions
    def _set_opt_out(self) -> None:
        # Priority: env var > worker agent config > deadline client config > default (enabled)
        env_var_value = os.environ.get("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", "")
        if env_var_value:
            self.telemetry_opted_out = env_var_value.lower() in _TRUE_VALUES
        else:
            self.telemetry_opted_out = self._read_opt_out_from_config()
        logger.info(
            "Deadline Cloud telemetry is "
            + ("not enabled." if self.telemetry_opted_out else "enabled.")
        )

    @staticmethod
    def _read_opt_out_from_config() -> bool:
        """Check the worker agent config file for telemetry opt-out, falling back to
        the deadline client config (~/.deadline/config) if not set."""
        try:
            from .config.config_file import ConfigFile

            config_file = ConfigFile.load()
            if config_file.telemetry.opt_out is not None:
                return config_file.telemetry.opt_out
        except Exception:
            pass

        # Fall back to legacy deadline client config (~/.deadline/config)
        return _get_setting("telemetry.opt_out").lower() in _TRUE_VALUES

    @_swallow_exceptions
    def _initialize(self) -> None:
        if self.telemetry_opted_out:
            return
        try:
            endpoint_url = boto3.client("deadline").meta.endpoint_url
            self.endpoint: str = self._get_prefixed_endpoint(
                f"{endpoint_url}/2023-10-12/telemetry",
                self.ENDPOINT_PREFIX,
            )

            from botocore.httpsession import create_urllib3_context, get_cert_path

            self._urllib3_context = create_urllib3_context()
            self._urllib3_context.load_verify_locations(cafile=get_cert_path(True))

            self._initialized = True
            self._start_threads()
        except Exception:
            return

    def _get_prefixed_endpoint(self, endpoint: str, prefix: str) -> str:
        if endpoint.startswith("https://"):
            return endpoint[:8] + prefix + endpoint[8:]
        return endpoint

    def _get_telemetry_identifier(self) -> str:
        """Get or create a persistent telemetry identifier.
        Checks worker.toml, then ~/.deadline/config, then generates and persists a new one."""
        # Check worker agent config
        try:
            from .config.config_file import ConfigFile

            config_file = ConfigFile.load()
            if config_file.telemetry.identifier is not None:
                return config_file.telemetry.identifier
        except Exception:
            pass

        # Fall back to legacy deadline client config
        identifier = _get_setting("telemetry.identifier")
        try:
            uuid.UUID(identifier, version=4)
        except ValueError:
            identifier = str(uuid.uuid4())

        # Persist to worker.toml for future runs
        try:
            from .config.config_file import (
                ConfigFile,
                ModifiableSetting,
                SettingModification,
            )

            ConfigFile.modify_config_file_settings(
                settings_to_modify=[
                    SettingModification(
                        setting=ModifiableSetting.TELEMETRY_IDENTIFIER,
                        value=identifier,
                    )
                ],
            )
        except Exception:
            logger.debug("Failed to persist telemetry identifier to worker.toml")

        return identifier

    def _get_system_metadata(self) -> Dict[str, Any]:
        platform_info = platform.uname()
        return {
            "service": self.package_name,
            "version": self.package_ver,
            "python_version": platform.python_version(),
            "osName": "macOS" if platform_info.system == "Darwin" else platform_info.system,
            "osVersion": platform_info.release,
        }

    def _start_threads(self) -> None:
        self.event_queue: Queue[Optional[TelemetryEvent]] = Queue(maxsize=self.MAX_QUEUE_SIZE)
        atexit.register(self._exit_cleanly)
        self.processing_thread: Thread = Thread(
            target=self._process_event_queue_thread, daemon=True
        )
        self.processing_thread.start()

    def _exit_cleanly(self) -> None:
        try:
            self.event_queue.put_nowait(None)
        except Full:
            pass
        self.processing_thread.join()

    def _send_request(self, req: request.Request) -> None:
        attempts = 0
        success = False
        while not success:
            try:
                with request.urlopen(req, context=self._urllib3_context):
                    logger.debug("Successfully sent telemetry.")
                    success = True
            except error.HTTPError as httpe:
                if httpe.code in (429, 500):
                    attempts += 1
                    if attempts >= self.MAX_RETRY_ATTEMPTS:
                        raise Exception("Max retries reached sending telemetry")
                    backoff_sleep = random.uniform(
                        0, min(self.MAX_BACKOFF_SECONDS, self.BASE_TIME * 2**attempts)
                    )
                    time.sleep(backoff_sleep)
                else:
                    raise

    def _process_event_queue_thread(self) -> None:
        while True:
            event_data: Optional[TelemetryEvent] = self.event_queue.get()
            if event_data is None:
                return
            try:
                request_body = {
                    "BatchId": str(uuid.uuid4()),
                    "RumEvents": [
                        {
                            "details": json.dumps(event_data.event_details),
                            "id": str(uuid.uuid4()),
                            "metadata": json.dumps(self._system_metadata),
                            "timestamp": int(datetime.now().timestamp()),
                            "type": event_data.event_type,
                        },
                    ],
                    "UserDetails": {
                        "sessionId": self.session_id,
                        "userId": self.telemetry_id,
                    },
                }
                request_body_encoded = json.dumps(request_body).encode("utf-8")
            except Exception as exc:
                logger.debug(f"Failed to serialize telemetry data. {exc}")
                continue

            headers = {"Accept": "application-json", "Content-Type": "application-json"}
            req = request.Request(url=self.endpoint, data=request_body_encoded, headers=headers)
            try:
                logger.debug("Sending telemetry data: %s", request_body)
                self._send_request(req)
            except Exception as exc:
                logger.debug(f"Error received from service. {exc}")
                return
            self.event_queue.task_done()

    def _put_telemetry_record(self, event: TelemetryEvent) -> None:
        if not self._initialized or self.telemetry_opted_out:
            return
        try:
            self.event_queue.put_nowait(event)
        except Full:
            pass

    @_swallow_exceptions
    def record_error(self, event_details: Dict[str, Any], exception_type: str) -> None:
        event_details["exception_type"] = exception_type
        self.record_event("com.amazon.rum.deadline.error", event_details)

    @_swallow_exceptions
    def record_event(self, event_type: str, event_details: Dict[str, Any], **kwargs: Any) -> None:
        event_details.update(self._common_details)
        self._put_telemetry_record(
            TelemetryEvent(event_type=event_type, event_details=event_details)
        )

    def update_common_details(self, details: Dict[str, Any]) -> None:
        self._common_details.update(details)
