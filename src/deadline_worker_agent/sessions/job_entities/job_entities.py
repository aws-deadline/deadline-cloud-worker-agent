# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass
from itertools import islice
from logging import getLogger
from threading import Event, Thread
from typing import Any, Iterator, Iterable, TYPE_CHECKING, TypeVar, Union, cast, Optional
import sys

from ...api_models import (
    EntityIdentifier,
    EntityError,
    EnvironmentDetailsData,
    EnvironmentDetailsIdentifier,
    EnvironmentDetailsIdentifierFields,
    JobAttachmentDetailsData,
    JobAttachmentDetailsIdentifier,
    JobAttachmentDetailsIdentifierFields,
    JobDetailsData,
    JobDetailsIdentifier,
    JobDetailsIdentifierFields,
    StepDetailsData,
    StepDetailsIdentifier,
    StepDetailsIdentifierFields,
)
from ...aws.deadline import (
    DeadlineRequestWorkerNotFound,
    DeadlineRequestUnrecoverableError,
    batch_get_job_entity,
)
from ...boto import DeadlineClient
from ...config import JobsRunAsUserOverride
from .job_attachment_details import JobAttachmentDetails
from .job_details import JobDetails
from .job_entity_type import JobEntityType
from .step_details import StepDetails
from .environment_details import EnvironmentDetails

if TYPE_CHECKING:
    from ...api_models import (
        BaseEntityErrorFields,
        EntityDetails,
    )

    if sys.platform == "win32":
        from ...windows.win_credentials_resolver import WindowsCredentialsResolver
    else:
        WindowsCredentialsResolver = Any
else:
    BaseEntityErrorFields = Any
    WindowsCredentialsResolver = Any


S = TypeVar(
    "S",
    EnvironmentDetailsIdentifier,
    JobAttachmentDetailsIdentifier,
    JobDetailsIdentifier,
    StepDetailsIdentifier,
)
F = TypeVar("F", JobDetails, JobAttachmentDetails, StepDetails, EnvironmentDetails)
DetailsData = Union[
    EnvironmentDetailsData, JobAttachmentDetailsData, JobDetailsData, StepDetailsData
]


logger = getLogger(__name__)


@dataclass
class EntityRecord:
    identifier: EntityIdentifier
    data: dict[str, Any] | None = None
    error: BaseEntityErrorFields | None = None


def _batched(iterable, n) -> Iterator[tuple]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 5) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


class JobEntities:
    """Class for accessing job details from Deadline.

    Internally, this class makes BatchGetJobEntity Deadline API requests and caches the results
    in-memory for future access.
    """

    _deadline_client: DeadlineClient
    _farm_id: str
    _fleet_id: str
    _worker_id: str
    _job_id: str
    _entity_record_map: dict[str, EntityRecord]
    _thread: Thread
    _stop: Event

    def __init__(
        self,
        *,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        job_id: str,
        deadline_client: DeadlineClient,
        windows_credentials_resolver: Optional[WindowsCredentialsResolver],
        job_run_as_user_override: Optional[JobsRunAsUserOverride],
    ) -> None:
        self._job_id = job_id
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._deadline_client = deadline_client
        self._windows_credentials_resolver = windows_credentials_resolver
        self._entity_record_map = {}
        self._job_run_as_user_override = job_run_as_user_override

    def request(self, *, identifier: EntityIdentifier) -> dict[str, Any]:
        """Given an identifier, grab the associated data from the
        BatchGetJobEntity response, if it already exists, otherwise
        query the service.
        """
        key = self._entity_key(identifier)

        # Get the entity record (initialize if doesn't exist)
        if not (entity_record := self._entity_record_map.get(key, None)):
            entity_record = EntityRecord(identifier=identifier)
            self._entity_record_map[key] = entity_record

        # Check for entity details data from BatchGetJobEntity
        if entity_record.data is not None:
            # Already successfully retrieved details
            return entity_record.data

        # Get a response, and populate the entity_record
        try:
            self.cache_entities([identifier])
        except Exception as e:
            # non-recoverable top-level batch_get_job_entities
            raise RuntimeError(
                f"Entity {identifier} failed in an unrecoverable way: {str(e)}"
            ) from e

        if entity_record.data is not None:
            # Yay, we have entity data!
            return entity_record.data
        if entity_record.error is not None:
            raise RuntimeError(
                f"Entity {identifier} failed with: {entity_record.error['code']} {entity_record.error['message']}"
            )
        # Should be impossible. Addresses linter warning
        raise RuntimeError(
            "Failed to get details or errors for a job entity, no exceptions thrown when caching. Should be impossible"
        )

    def _entity_key(self, entity: EntityIdentifier | EntityDetails | EntityError) -> str:
        entity_keys = list(entity.keys())

        # Validate the entity tagged-union key
        if len(entity_keys) < 1:
            raise ValueError("Entity contains no keys")
        elif len(entity_keys) > 1:
            entity_keys_str = ", ".join(entity_keys)
            raise ValueError(f"Entity contains multiple keys ({entity_keys_str}) but expected one")

        entity_type = JobEntityType(entity_keys[0])
        entity_identifier_fields = list(entity.values())[0]

        if entity_type == JobEntityType.ENVIRONMENT_DETAILS:
            entity_identifier_fields = cast(
                EnvironmentDetailsIdentifierFields, entity_identifier_fields
            )
            return entity_identifier_fields["environmentId"]
        elif entity_type == JobEntityType.STEP_DETAILS:
            entity_identifier_fields = cast(StepDetailsIdentifierFields, entity_identifier_fields)
            return entity_identifier_fields["stepId"]
        elif entity_type == JobEntityType.JOB_DETAILS:
            entity_identifier_fields = cast(JobDetailsIdentifierFields, entity_identifier_fields)
            return entity_identifier_fields["jobId"]
        elif entity_type == JobEntityType.JOB_ATTACHMENT_DETAILS:
            entity_identifier_fields = cast(
                JobAttachmentDetailsIdentifierFields, entity_identifier_fields
            )
            return f"JA({entity_identifier_fields['jobId']})"
        else:
            raise ValueError(f'Unexpected entity type "{entity_type}"')

    def _get_max_entities_per_batch_get_job_entity_request(self) -> int:
        """Introspects the service model and returns the maximum allowed entities that can be
        requested in a single BatchGetJobEntity API request

        Returns
        -------
        int:
            The maximum allowed entities that can be requested in a single BatchGetJobEntity API
            request
        """
        service_model = self._deadline_client._real_client._service_model
        operation_model = service_model.operation_model("BatchGetJobEntity")
        identifiers_request_field = operation_model.input_shape.members["identifiers"]
        return identifiers_request_field.metadata["max"]

    def _create_entity_records(self, entity_identifiers: Iterable[EntityIdentifier]):
        """Helper func to create the entity records when caching
        multiple identifiers at once"""
        for identifier in entity_identifiers:
            entity_key = self._entity_key(identifier)
            if not self._entity_record_map.get(entity_key, None):
                self._entity_record_map[entity_key] = EntityRecord(identifier=identifier)

    def cache_entities(self, entity_identifiers: list[EntityIdentifier]):
        # Determine how many entities can be requested in a single BatchGetJobEntities API call
        max_entities = self._get_max_entities_per_batch_get_job_entity_request()
        for batched_identifiers in _batched(entity_identifiers, max_entities):
            self._create_entity_records(batched_identifiers)

            try:
                response = batch_get_job_entity(
                    deadline_client=self._deadline_client,
                    farm_id=self._farm_id,
                    fleet_id=self._fleet_id,
                    worker_id=self._worker_id,
                    identifiers=[identifier for identifier in batched_identifiers],
                )
            except (DeadlineRequestWorkerNotFound, DeadlineRequestUnrecoverableError):
                # Technically, the API log reports this information, but we'll log anyways just to
                # draw attention to it.
                logger.error("Errors from BatchGetJobEntity! See API log event for details.")
                continue
                # Remaining responses: AccessDenied, InternalServerErrorException, ValidationException
                # May be some race-ish conditions with the scheduler. Others may be recoverable, some not
                # ie. malformed entities

            # save each successful entity response in its EntityRecord
            for entity in response["entities"]:
                # entity is a dict that is a tagged union, so one of:
                #    { "environmentDetails":   ... }
                #    { "jobAttachmentDetails": ... }
                #    { "jobDetails":           ... }
                #    { "stepDetails":          ... }
                entity_items = list(entity.items())
                if len(entity_items) != 1:
                    # Only happens if there's a service bug.
                    raise ValueError(
                        f"Expected a single key in entity, but got {', '.join(entity.keys())}"
                    )
                entity_item = entity_items[0]
                entity_data = cast(dict[str, Any], entity_item[1])
                entity_key = self._entity_key(entity)

                entity_record = self._entity_record_map[entity_key]
                entity_record.data = entity_data

            for failed_entity in response["errors"]:
                # failed_entity is a dict that is a tagged union, so one of:
                #    { "environmentDetails":   ... }
                #    { "jobAttachmentDetails": ... }
                #    { "jobDetails":           ... }
                #    { "stepDetails":          ... }
                failed_entity_values = cast(
                    list[BaseEntityErrorFields], list(failed_entity.values())
                )
                # Assert only fails if there's a service bug.
                assert len(failed_entity_values) == 1, (
                    f"Entity errors should contain a single key, but got {failed_entity.keys()}"
                )

                failed_entity_value = failed_entity_values[0]
                if failed_entity_value["code"] == "MaxPayloadSizeExceeded":
                    # ignore MaxPayloadSizeExceeded, only matters for batch caching
                    continue
                # InternalServerException, ValidationException, ResourceNotFoundException,

                failed_entity_key = self._entity_key(failed_entity)
                entity_record = self._entity_record_map[failed_entity_key]
                entity_record.error = failed_entity_value
                logger.error("Errors from BatchGetJobEntity! See API log event for details.")

    def job_attachment_details(self) -> JobAttachmentDetails:
        """Returns a future for the job attachment details.

        Raises
        ------
        ValueError:
            JobAttachmentDetailsData did not successfully validate


        Returns
        -------
        JobAttachmentDetails
        """
        identifier = JobAttachmentDetailsIdentifier(
            jobAttachmentDetails=JobAttachmentDetailsIdentifierFields(
                jobId=self._job_id,
            ),
        )

        result = self.request(identifier=identifier)
        job_attachment_details_data = JobAttachmentDetails.validate_entity_data(result)
        return JobAttachmentDetails.from_boto(job_attachment_details_data)

    def job_details(self) -> JobDetails:
        """Returns job details.

        Raises
        ------
        ValueError:
            JobDetailsData did not successfully validate

        Returns
        -------
        JobDetails
        """

        identifier = JobDetailsIdentifier(
            jobDetails=JobDetailsIdentifierFields(
                jobId=self._job_id,
            ),
        )

        result = self.request(identifier=identifier)
        job_details_data = JobDetails.validate_entity_data(result, self._job_run_as_user_override)
        job_details = JobDetails.from_boto(job_details_data)

        # if JobRunAsUser specifies a windows user resolve the credentials here
        # so that we don't have to pass the resolver all through the JobDetails data classes
        if job_details.job_run_as_user is not None:
            windows_settings = job_details.job_run_as_user.windows_settings
            if windows_settings is not None and self._windows_credentials_resolver is not None:
                job_details.job_run_as_user.windows = (
                    self._windows_credentials_resolver.get_windows_session_user(
                        windows_settings.user, windows_settings.passwordArn
                    )
                )

        return job_details

    def step_details(self, *, step_id: str) -> StepDetails:
        """Returns step details.

        Parameters
        ----------
        step_id : str
            The step ID

        Raises
        ------
        UnsupportedSchema:
            The StepDetailsData uses a schema that is not supported
            by the Worker Agent
        ValueError:
            StepDetailsData did not successfully validate

        Returns
        -------
        StepDetails
        """

        identifier = StepDetailsIdentifier(
            stepDetails=StepDetailsIdentifierFields(
                jobId=self._job_id,
                stepId=step_id,
            ),
        )

        result = self.request(identifier=identifier)
        step_details_data = StepDetails.validate_entity_data(result)
        return StepDetails.from_boto(step_details_data)

    def environment_details(self, *, environment_id: str) -> EnvironmentDetails:
        """Returns the environment details.

        Parameters
        ----------
        environment_id : str
            The environment ID

        Raises
        ------
        UnsupportedSchema
            The EnvironmentDetailsData uses a schema that is not supported
            by the Worker Agent
        ValueError:
            EnvironmentDetailsData did not successfully validate

        Returns
        -------
        EnvironmentDetails
        """

        identifier = EnvironmentDetailsIdentifier(
            environmentDetails=EnvironmentDetailsIdentifierFields(
                jobId=self._job_id,
                environmentId=environment_id,
            ),
        )

        result = self.request(identifier=identifier)
        environment_details_data = EnvironmentDetails.validate_entity_data(result)
        return EnvironmentDetails.from_boto(environment_details_data)
