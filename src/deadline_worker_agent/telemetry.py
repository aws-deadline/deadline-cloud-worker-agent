# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import atexit
from functools import lru_cache, wraps
import json
import logging
import os
import platform
import uuid
import random
import time

from botocore.config import Config as BotocoreConfig
from configparser import ConfigParser
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue, Full
from threading import Thread
from typing import Any, Callable, Dict, Optional, TypeVar, cast
from urllib import request, error

import boto3

logger = logging.getLogger(__name__)

# Generic function return type.
F = TypeVar("F", bound=Callable[..., Any])

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


def _get_setting(setting_name: str) -> str:
    """Read a setting from the legacy deadline config file. Returns empty string if not found."""
    if "." not in setting_name:
        return ""
    section, name = setting_name.split(".", 1)
    config = _read_config()
    for config_section in config.sections():
        if config_section == section or config_section.endswith(f" {section}"):
            if config.has_option(config_section, name):
                return config.get(config_section, name)
    return ""


def _swallow_exceptions(func: F) -> F:
    """Decorator that catches all exceptions in telemetry functions to prevent
    telemetry issues from affecting the main application flow."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.debug(
                "Swallowed exception in telemetry function %s", func.__name__, exc_info=True
            )
            return None

    return cast(F, wrapper)


@dataclass
class TelemetryEvent:
    """Base class for telemetry events"""

    event_type: str = "com.amazon.rum.deadline.uncategorized"
    event_details: Dict[str, Any] = field(default_factory=dict)


class TelemetryClient:
    """
    Sends telemetry events periodically to the Deadline Cloud telemetry service.

    This client holds a queue of events which is written to synchronously, and processed
    asynchronously, where events are sent in the background, so that it does not slow
    down user interactivity.

    Telemetry events contain non-personally-identifiable information that helps us
    understand how users interact with our software so we know what features our
    customers use, and/or what existing pain points are.

    Data is aggregated across a session ID (a UUID created at runtime), used to mark every
    telemetry event for the lifetime of the application), and a 'telemetry identifier' (a
    UUID recorded in the configuration file), to aggregate data across multiple application
    lifetimes on the same machine.

    Telemetry collection can be opted-out of by setting opt_out = true in the [telemetry]
    section of worker.toml, or setting the environment variable
    'DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true'
    """

    # Used for backing off requests if we encounter errors from the service.
    # See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    MAX_QUEUE_SIZE = 25
    BASE_TIME = 0.5
    MAX_BACKOFF_SECONDS = 10  # The maximum amount of time to wait between retries
    MAX_RETRY_ATTEMPTS = 4

    ENDPOINT_PREFIX = "management."

    def __init__(
        self,
        package_name: str,
        package_ver: str,
    ):
        # Instance-level dicts so every TelemetryClient has its own state
        self._common_details: Dict[str, Any] = {}
        self._system_metadata: Dict[str, Any] = {}

        self._initialized: bool = False
        self.package_name = package_name
        self.package_ver = ".".join(package_ver.split(".")[:3])

        # IDs for this session
        self.session_id: str = str(uuid.uuid4())
        try:
            self.telemetry_id: str = self._get_telemetry_identifier()
        except Exception:
            logger.debug("Swallowed exception in telemetry __init__", exc_info=True)
            self.telemetry_id = str(uuid.uuid4())
        try:
            self._system_metadata = self._get_system_metadata()
        except Exception:
            logger.debug("Swallowed exception in telemetry __init__", exc_info=True)
            self._system_metadata = {}
        self.set_opt_out()
        self.initialize()

    @_swallow_exceptions
    def set_opt_out(self) -> None:
        """
        Checks whether telemetry has been opted out.
        Priority: env var > worker agent config (worker.toml) > legacy config (~/.deadline/config)
        """
        env_var_value = os.environ.get("DEADLINE_CLOUD_TELEMETRY_OPT_OUT")
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
        the deadline client config (~/.deadline/config) if not set.
        If found in legacy config, persists to worker.toml."""
        try:
            from .config.config_file import ConfigFile

            config_file = ConfigFile.load()
            if config_file.telemetry.opt_out is not None:
                return config_file.telemetry.opt_out
        except Exception:
            pass

        # Fall back to legacy deadline client config (~/.deadline/config)
        legacy_value = _get_setting("telemetry.opt_out").lower() in _TRUE_VALUES

        # Persist to worker.toml if opted out via legacy config
        if legacy_value:
            try:
                from .config.config_file import (
                    ConfigFile,
                    ModifiableSetting,
                    SettingModification,
                )

                ConfigFile.modify_config_file_settings(
                    settings_to_modify=[
                        SettingModification(
                            setting=ModifiableSetting.TELEMETRY_OPT_OUT,
                            value=True,
                        )
                    ],
                )
            except Exception:
                logger.debug("Failed to persist telemetry opt-out to worker.toml")

        return legacy_value

    @_swallow_exceptions
    def initialize(self) -> None:
        """
        Starts up the telemetry background thread after getting settings from the boto3 client.
        Note that if this is called before boto3 is successfully configured / initialized,
        an error can be raised. In that case we silently fail and don't mark the client as
        initialized.
        """
        if self.telemetry_opted_out:
            return

        endpoint_url = boto3.client("deadline").meta.endpoint_url
        self.endpoint: str = self._get_prefixed_endpoint(
            f"{endpoint_url}/2023-10-12/telemetry",
            TelemetryClient.ENDPOINT_PREFIX,
        )

        # Some environments might not have SSL, so we'll use the vendored botocore SSL context
        from botocore.httpsession import create_urllib3_context, get_cert_path

        self._urllib3_context = create_urllib3_context()
        self._urllib3_context.load_verify_locations(cafile=get_cert_path(True))

        self._initialized = True
        self._start_threads()

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def _get_prefixed_endpoint(self, endpoint: str, prefix: str) -> str:
        """Insert the prefix right after 'https://'"""
        if endpoint.startswith("https://"):
            prefixed_endpoint = endpoint[:8] + prefix + endpoint[8:]
            return prefixed_endpoint
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

    def _start_threads(self) -> None:
        """Set up background threads for shutdown checking and request sending"""
        self.event_queue: Queue[Optional[TelemetryEvent]] = Queue(
            maxsize=TelemetryClient.MAX_QUEUE_SIZE
        )
        atexit.register(self._exit_cleanly)
        self.processing_thread: Thread = Thread(
            target=self._process_event_queue_thread, daemon=True
        )
        self.processing_thread.start()

    def _get_system_metadata(self) -> Dict[str, Any]:
        """
        Builds up a dict of non-identifiable metadata about the system environment.
        """
        platform_info = platform.uname()
        return {
            "service": self.package_name,
            "version": self.package_ver,
            "python_version": platform.python_version(),
            "osName": "macOS" if platform_info.system == "Darwin" else platform_info.system,
            "osVersion": platform_info.release,
        }

    @_swallow_exceptions
    def _exit_cleanly(self):
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
                if httpe.code == 429 or httpe.code == 500:
                    logger.debug(f"Error received from service. Waiting to retry: {str(httpe)}")

                    attempts += 1
                    if attempts >= TelemetryClient.MAX_RETRY_ATTEMPTS:
                        raise Exception("Max retries reached sending telemetry")

                    backoff_sleep = random.uniform(
                        0,
                        min(
                            TelemetryClient.MAX_BACKOFF_SECONDS,
                            TelemetryClient.BASE_TIME * 2**attempts,
                        ),
                    )
                    time.sleep(backoff_sleep)
                else:  # Reraise any exceptions we didn't expect
                    raise

    def _process_event_queue_thread(self):
        """Background thread for processing the telemetry event data queue and sending telemetry requests."""
        # Resolve the AWS account ID once on this background thread so callers
        # of record_event() are never blocked.
        try:
            session = boto3.Session()
            account_id = self.get_account_id(session)
            if account_id:
                self.update_common_details({"accountId": account_id})
        except Exception:
            logger.debug("Could not resolve account ID for telemetry", exc_info=True)

        while True:
            # Blocks until we get a new entry in the queue
            event_data: Optional[TelemetryEvent] = self.event_queue.get()
            # We've received the shutdown signal
            if event_data is None:
                return

            headers = {"Accept": "application-json", "Content-Type": "application-json"}
            try:
                # Merge _common_details into the per-event details at send time
                details = {**event_data.event_details, **self._common_details}
                request_body = {
                    "BatchId": str(uuid.uuid4()),
                    "RumEvents": [
                        {
                            "details": str(json.dumps(details)),
                            "id": str(uuid.uuid4()),
                            "metadata": str(json.dumps(self._system_metadata)),
                            "timestamp": int(datetime.now().timestamp()),
                            "type": event_data.event_type,
                        },
                    ],
                    "UserDetails": {"sessionId": self.session_id, "userId": self.telemetry_id},
                }
                request_body_encoded = str(json.dumps(request_body)).encode("utf-8")
            except Exception as exc:
                logger.debug(f"Failed to serialize telemetry data. {str(exc)}")
                continue

            req = request.Request(url=self.endpoint, data=request_body_encoded, headers=headers)
            try:
                logger.debug("Sending telemetry data: %s", request_body)
                self._send_request(req)
            except Exception as exc:
                # Swallow any kind of uncaught exception and stop sending telemetry
                logger.debug(f"Error received from service. {str(exc)}")
                return
            self.event_queue.task_done()

    def _put_telemetry_record(self, event: TelemetryEvent) -> None:
        if not self._initialized or self.telemetry_opted_out:
            return
        try:
            self.event_queue.put_nowait(event)
        except Full:
            # Silently swallow the error if the event queue is full (due to throttling of the service)
            pass

    def record_error(self, event_details: Dict[str, Any], exception_type: str):
        event_details["exception_type"] = exception_type
        self.record_event("com.amazon.rum.deadline.error", event_details)

    @_swallow_exceptions
    def record_event(self, event_type: str, event_details: Dict[str, Any], **kwargs: Any):
        self._put_telemetry_record(
            TelemetryEvent(
                event_type=event_type,
                event_details=event_details,
            )
        )

    @lru_cache
    def get_account_id(self, boto3_session) -> Optional[str]:
        """Best-effort AWS account ID lookup for telemetry, cached per session."""
        try:
            credentials = boto3_session.get_credentials()
            account_id = getattr(credentials, "account_id", None) if credentials else None
            if account_id:
                return account_id
            sts = boto3_session.client(
                "sts",
                config=BotocoreConfig(
                    connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}
                ),
            )
            return sts.get_caller_identity()["Account"]
        except Exception:
            logger.debug("Could not resolve account ID for telemetry", exc_info=True)
            return None

    def update_common_details(self, details: Dict[str, Any]):
        """Updates the dict of common data that is included in every telemetry request."""
        self._common_details.update(details)
