# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Generator
from unittest.mock import MagicMock, patch

from deadline.job_attachments.models import JobAttachmentsFileSystem
from openjd.model import SchemaVersion
from openjd.model.v2023_09 import (
    Action,
    Environment,
    EnvironmentActions,
    EnvironmentScript,
    StepActions,
    StepScript,
)
from openjd.sessions import PosixSessionUser


import pytest

from deadline_worker_agent.api_models import (
    Attachments,
    BatchGetJobEntityResponse,
    EntityIdentifier,
    EnvironmentDetails as EnvironmentDetailsBoto,
    EnvironmentDetailsData,
    EnvironmentDetailsIdentifier,
    JobAttachmentDetails as JobAttachmentDetailsBoto,
    JobAttachmentDetailsData,
    JobAttachmentDetailsIdentifier,
    JobDetails as JobDetailsBoto,
    JobDetailsData,
    JobDetailsIdentifier,
    PathMappingRule,
    StepDetails as StepDetailsBoto,
    StepDetailsData,
    StepDetailsIdentifier,
)
from deadline_worker_agent.sessions.job_entities import (
    EnvironmentDetails,
    JobAttachmentDetails,
    JobDetails,
    JobEntities,
    StepDetails,
)
from deadline_worker_agent.sessions.job_entities.job_details import JobRunAsUser
import deadline_worker_agent.sessions.job_entities.job_entities as job_entities_mod


@pytest.fixture
def job_id() -> str:
    return "job-1234567890abcdef1234567890abcdef"


@pytest.fixture
def deadline_client() -> MagicMock:
    client = MagicMock()
    client.batch_get_job_entity.return_value = {
        "entities": [],
        "errors": [],
    }
    return client


@pytest.fixture(autouse=True)
def mock_client_batch_get_job_entity_max_identifiers(
    deadline_client: MagicMock,
) -> None:
    """Mocks boto introspection of the BatchGetJobEntity request model. The JobEntities code
    introspects this to dynamically determine the maximum number of entities that can be
    requested in a single request.

    DO NOT REMOVE OR TURN OFF autouse=True. Without this, the tests hang
    """
    service_model = deadline_client._real_client._service_model
    operation_model = service_model.operation_model.return_value
    identifiers_request_field = MagicMock()
    identifiers_request_field.metadata = {"max": 5}
    operation_model.input_shape.members = {"identifiers": identifiers_request_field}


@pytest.fixture
def job_entities(
    deadline_client: MagicMock,
    job_id: str,
) -> Generator[JobEntities, None, None]:
    job_entities = JobEntities(
        farm_id="farm-id",
        fleet_id="fleet-id",
        worker_id="worker-id",
        job_id=job_id,
        deadline_client=deadline_client,
    )

    yield job_entities


class TestJobEntity:
    @pytest.mark.parametrize(
        "path_mapping_rules",
        (
            pytest.param(None, id="no list"),
            pytest.param([], id="empty list"),
            pytest.param(
                [
                    {
                        "sourcePathFormat": "windows",
                        "sourcePath": "C:/windows/path",
                        "destinationPath": "/linux/path",
                    }
                ],
                id="One Rule",
            ),
            pytest.param(
                [
                    {
                        "sourcePathFormat": "windows",
                        "sourcePath": "Z:/artist/windows/path",
                        "destinationPath": "/mnt/worker/windows/path",
                    },
                    {
                        "sourcePathFormat": "posix",
                        "sourcePath": "/artist/linux",
                        "destinationPath": "/mnt/worker/linux",
                    },
                    {
                        "sourcePathFormat": "posix",
                        "sourcePath": "/artist/linux/path",
                        "destinationPath": "/mnt/worker/linux/path",
                    },
                ],
                id="Multiple Rules",
            ),
        ),
    )
    def test_has_path_mapping_rules(
        self,
        deadline_client: MagicMock,
        path_mapping_rules: list[PathMappingRule] | None,
    ) -> None:
        # GIVEN
        job_id = "job-fedcba0987654321fedcba0987654321"
        job_details_boto = JobDetailsBoto(
            jobDetails={
                "jobId": job_id,
                "schemaVersion": "jobtemplate-2023-09",
                "logGroupName": "fake-name",
                "jobRunAsUser": {
                    "posix": {
                        "user": "job-user",
                        "group": "job-group",
                    },
                },
            },
        )
        response: BatchGetJobEntityResponse = {
            "entities": [job_details_boto],
            "errors": [],
        }
        if path_mapping_rules is not None:
            job_details_boto["jobDetails"]["pathMappingRules"] = path_mapping_rules
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
        )

        # WHEN
        job_details = job_entities.job_details()

        # THEN
        if path_mapping_rules in (None, []):
            assert job_details.path_mapping_rules == []
        else:
            assert path_mapping_rules is not None
            assert job_details.path_mapping_rules not in (None, [])
            assert len(job_details.path_mapping_rules) == len(path_mapping_rules)

    def test_job_run_as_user(self) -> None:
        """Ensures that if we receive a job_run_as_user field in the response,
        that the created entity has a (Posix) SessionUser created with the
        proper values"""
        # GIVEN
        expected_user = "job-user"
        expected_group = "job-group"
        api_response: dict = {
            "jobId": "job-123",
            "jobRunAsUser": {
                "posix": {
                    "user": expected_user,
                    "group": expected_group,
                },
            },
            "logGroupName": "TEST",
            "schemaVersion": SchemaVersion.v2023_09.value,
        }

        # WHEN
        job_details_data = JobDetails.validate_entity_data(api_response)
        entity_obj = JobDetails.from_boto(job_details_data)

        # THEN
        assert entity_obj.job_run_as_user is not None
        assert isinstance(entity_obj.job_run_as_user.posix, PosixSessionUser)
        assert entity_obj.job_run_as_user.posix.user == expected_user
        assert entity_obj.job_run_as_user.posix.group == expected_group


class TestDetails:
    def test_job_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        job_details_boto = JobDetailsBoto(
            jobDetails={
                "jobId": job_id,
                "schemaVersion": "jobtemplate-2023-09",
                "logGroupName": "fake-name",
                "jobRunAsUser": {
                    "posix": {
                        "user": "job-user",
                        "group": "job-group",
                    },
                },
            },
        )
        response: BatchGetJobEntityResponse = {
            "entities": [job_details_boto],
            "errors": [],
        }
        expected_details = JobDetails(
            schema_version=SchemaVersion("jobtemplate-2023-09"),
            log_group_name="fake-name",
            job_run_as_user=JobRunAsUser(
                posix=PosixSessionUser(user="job-user", group="job-group")
            ),
        )
        assert expected_details.job_run_as_user is not None  # For type checker
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
        )

        # WHEN
        details = job_entities.job_details()

        # THEN
        assert details.log_group_name == expected_details.log_group_name
        assert details.schema_version == expected_details.schema_version
        assert details.job_run_as_user is not None
        assert details.job_run_as_user.posix.user == expected_details.job_run_as_user.posix.user
        assert details.job_run_as_user.posix.group == expected_details.job_run_as_user.posix.group
        assert details.job_attachment_settings == expected_details.job_attachment_settings
        assert details.parameters == expected_details.parameters
        assert details.path_mapping_rules == expected_details.path_mapping_rules
        assert details.queue_role_arn == expected_details.queue_role_arn

    def test_environment_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        environment_id = "env-id"
        env_name = "TestEnv"
        details_boto = EnvironmentDetailsBoto(
            environmentDetails=EnvironmentDetailsData(
                jobId=job_id,
                environmentId=environment_id,
                schemaVersion="jobtemplate-2023-09",
                template={
                    "name": env_name,
                    "script": {
                        "actions": {
                            "onEnter": {
                                "command": "test",
                            },
                        }
                    },
                },
            ),
        )
        response: BatchGetJobEntityResponse = {
            "entities": [details_boto],
            "errors": [],
        }
        expected_details = EnvironmentDetails(
            environment=Environment(
                name=env_name,
                script=EnvironmentScript(
                    actions=EnvironmentActions(onEnter=Action(command="test"))
                ),
            )
        )
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
        )

        # WHEN
        details = job_entities.environment_details(environment_id=environment_id)

        # THEN
        assert details == expected_details

    def test_job_attachment_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        details_boto = JobAttachmentDetailsBoto(
            jobAttachmentDetails=JobAttachmentDetailsData(
                jobId=job_id,
                attachments=Attachments(manifests=[], fileSystem=JobAttachmentsFileSystem.COPIED),
            )
        )
        response: BatchGetJobEntityResponse = {
            "entities": [details_boto],
            "errors": [],
        }
        expected_details = JobAttachmentDetails(
            manifests=[], job_attachments_file_system=JobAttachmentsFileSystem.COPIED
        )
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
        )

        # WHEN
        details = job_entities.job_attachment_details()

        # THEN
        assert details == expected_details

    def test_step_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        step_id = "step-id"
        dependency = "stepId-1234"
        details_boto = StepDetailsBoto(
            stepDetails=StepDetailsData(
                jobId=job_id,
                stepId=step_id,
                schemaVersion="jobtemplate-2023-09",
                template={
                    "actions": {
                        "onRun": {
                            "command": "test.exe",
                        },
                    }
                },
                dependencies=[dependency],
            )
        )
        response: BatchGetJobEntityResponse = {
            "entities": [details_boto],
            "errors": [],
        }

        expected_details = StepDetails(
            script=StepScript(actions=StepActions(onRun=Action(command="test.exe"))),
            dependencies=[dependency],
        )
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
        )

        # WHEN
        details = job_entities.step_details(step_id=step_id)

        # THEN
        assert details == expected_details


class TestCaching:
    @pytest.fixture(autouse=True)
    def mock_batch_get_job_entity(self) -> Generator[MagicMock, None, None]:
        with patch.object(job_entities_mod, "batch_get_job_entity") as mock:
            yield mock

    def test_cache_entities(
        self,
        job_id: str,
        job_entities: JobEntities,
        mock_batch_get_job_entity: MagicMock,
    ):
        # Test that we store entities when we request them and they are returned successfully.

        # GIVEN
        environment_id = "env:1234"
        step_id = "step-1234"

        request: list[EntityIdentifier] = [
            JobDetailsIdentifier(
                {
                    "jobDetails": {
                        "jobId": job_id,
                    }
                }
            ),
            EnvironmentDetailsIdentifier(
                {"environmentDetails": {"jobId": job_id, "environmentId": environment_id}}
            ),
            StepDetailsIdentifier({"stepDetails": {"jobId": job_id, "stepId": step_id}}),
            JobAttachmentDetailsIdentifier(
                {"jobAttachmentDetails": {"jobId": job_id, "stepId": step_id}}
            ),
        ]
        expected_job_details: JobDetailsData = {
            "jobId": job_id,
            "logGroupName": "/aws/service/loggroup",
            "schemaVersion": "jobtemplate-2023-09",
            "jobRunAsUser": {
                "posix": {
                    "user": "job-user",
                    "group": "job-group",
                },
            },
        }
        expected_environment_details: EnvironmentDetailsData = {
            "jobId": job_id,
            "environmentId": environment_id,
            "schemaVersion": "jobtemplate-2023-09",
            # Don't actually need the full template for a test
            "template": {},
        }
        expected_step_details: StepDetailsData = {
            "jobId": job_id,
            "stepId": step_id,
            "schemaVersion": "jobtemplate-2023-09",
            # Don't actually need the full template for a test
            "template": {},
        }
        expected_attachment_details: JobAttachmentDetailsData = {
            "jobId": job_id,
            "stepId": step_id,
            "attachments": {
                "manifests": [
                    {
                        "rootPath": "/mnt/share",
                        "rootPathFormat": "posix",
                        "outputRelativeDirectories": ["output"],
                    }
                ]
            },
        }
        response: BatchGetJobEntityResponse = {
            "entities": [
                JobDetailsBoto({"jobDetails": expected_job_details}),
                EnvironmentDetailsBoto({"environmentDetails": expected_environment_details}),
                StepDetailsBoto({"stepDetails": expected_step_details}),
                JobAttachmentDetailsBoto({"jobAttachmentDetails": expected_attachment_details}),
            ],
            "errors": [],
        }
        mock_batch_get_job_entity.return_value = response

        # WHEN
        job_entities.cache_entities(request)

        # THEN
        assert job_entities._entity_record_map.get(f"jobDetails({job_id})") is not None
        assert (
            job_entities._entity_record_map.get(f"environmentDetails({environment_id})") is not None
        )
        assert job_entities._entity_record_map.get(f"stepDetails({step_id})") is not None
        assert job_entities._entity_record_map.get(f"job_attachments({job_id})") is not None
        assert job_entities._entity_record_map[f"jobDetails({job_id})"].data == expected_job_details
        assert (
            job_entities._entity_record_map[f"environmentDetails({environment_id})"].data
            == expected_environment_details
        )
        assert (
            job_entities._entity_record_map[f"stepDetails({step_id})"].data == expected_step_details
        )
        assert (
            job_entities._entity_record_map[f"job_attachments({job_id})"].data
            == expected_attachment_details
        )

    @pytest.mark.parametrize("request_size", [i + 1 for i in range(0, 14)])
    def test_cache_entities_is_batched(
        self,
        job_id: str,
        job_entities: JobEntities,
        mock_batch_get_job_entity: MagicMock,
        request_size: int,
    ):
        # Test that when we make a request with more than the maximum that the API allows (5 in these tests)
        # then we divide the request in to batches

        # GIVEN
        job_ids = [f"{job_id}-{i}" for i in range(0, request_size)]
        request: list[EntityIdentifier] = [
            JobDetailsIdentifier(
                {
                    "jobDetails": {
                        "jobId": id,
                    }
                }
            )
            for id in job_ids
        ]
        expected_batches = [request[i : i + 5] for i in range(0, request_size, 5)]
        mock_batch_get_job_entity.return_value = {
            "entities": [],
            "errors": [],
        }

        # WHEN
        job_entities.cache_entities(request)

        # THEN
        assert mock_batch_get_job_entity.call_count == len(expected_batches)
        for i, batch in enumerate(expected_batches):
            assert mock_batch_get_job_entity.call_args_list[i].kwargs["identifiers"] == batch

    def test_request_cached_details_response(
        self,
        job_id: str,
        job_entities: JobEntities,
        mock_batch_get_job_entity: MagicMock,
    ):
        # GIVEN
        environment_id = "env:1234"
        details = EnvironmentDetailsData(
            jobId=job_id,
            environmentId=environment_id,
            schemaVersion="jobtemplate-2023-09",
            template={
                "name": "TestEnv",
                "script": {
                    "actions": {
                        "onEnter": {
                            "command": "test",
                        },
                    }
                },
            },
        )

        response: BatchGetJobEntityResponse = {
            "entities": [
                EnvironmentDetailsBoto({"environmentDetails": details}),
            ],
            "errors": [],
        }
        mock_batch_get_job_entity.return_value = response

        # WHEN
        job_entities.environment_details(environment_id=environment_id)
        mock_batch_get_job_entity.assert_called_once()

        # THEN
        job_entities.environment_details(environment_id=environment_id)
        mock_batch_get_job_entity.assert_called_once()
