# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Generator
from unittest.mock import MagicMock, patch

from deadline.job_attachments.utils import AssetLoadingMethod
from openjobio.model import SchemaVersion
from openjobio.model.v2022_09_01 import (
    Action,
    Environment,
    EnvironmentActions,
    EnvironmentScript,
    StepActions,
    StepScript,
)
from openjobio.sessions import PosixSessionUser


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
    JobsRunAs,
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
                        # TODO: swap to sourcePathFormat once sourceOs removed
                        "sourceOs": "windows",
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
                "schemaVersion": "2022-09-01",
                "logGroupName": "fake-name",
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

    def test_jobs_run_as(self) -> None:
        """Ensures that if we receive a jobs_run_as field in the response,
        that the created entity has a (Posix) SessionUser created with the
        proper values"""
        # GIVEN
        expected_user = "job-user"
        expected_group = "job-group"
        entity_data: JobDetailsData = {
            "jobId": "job-123",
            "jobsRunAs": {
                "posix": {
                    "user": expected_user,
                    "group": expected_group,
                },
            },
            "logGroupName": "TEST",
            "schemaVersion": SchemaVersion.v2022_09_01.value,
        }

        # WHEN
        entity_obj = JobDetails.from_boto(entity_data)

        # THEN
        assert entity_obj.jobs_run_as is not None
        assert isinstance(entity_obj.jobs_run_as.posix, PosixSessionUser)
        assert entity_obj.jobs_run_as.posix.user == expected_user
        assert entity_obj.jobs_run_as.posix.group == expected_group

    @pytest.mark.parametrize(
        ("jobs_run_as_data"),
        (
            pytest.param(
                {
                    "posix": {
                        "user": "",
                        "group": "",
                    }
                },
                id="empty user and group",
            ),
            pytest.param(
                {
                    "posix": {
                        "user": "job-user",
                        "group": "",
                    }
                },
                id="empty group",
            ),
            pytest.param(
                {
                    "posix": {
                        "user": "",
                        "group": "job-group",
                    }
                },
                id="empty user",
            ),
            pytest.param({"posix": {}}, id="no user/group entries"),
            pytest.param({}, id="no posix"),
        ),
    )
    def test_jobs_run_empty_values(self, jobs_run_as_data: JobsRunAs | None) -> None:
        """Ensures that if we are missing values in the jobs_run_as fields
        that created entity does not have it set (ie. old queues)"""
        # GIVEN
        entity_data: JobDetailsData = {
            "jobId": "job-123",
            "jobsRunAs": jobs_run_as_data,
            "logGroupName": "TEST",
            "schemaVersion": SchemaVersion.v2022_09_01.value,
        }

        # WHEN
        entity_obj = JobDetails.from_boto(entity_data)

        # THEN
        assert entity_obj.jobs_run_as is None

    def test_jobs_run_as_not_provided(self) -> None:
        """Ensures that if we somehow don't receive a jobs_run_as field
        that the created entity does not have it set (shouldn't happen)"""
        # GIVEN
        entity_data: JobDetailsData = {
            "jobId": "job-123",
            "logGroupName": "TEST",
            "schemaVersion": SchemaVersion.v2022_09_01.value,
        }

        # WHEN
        entity_obj = JobDetails.from_boto(entity_data)

        # THEN
        assert entity_obj.jobs_run_as is None


class TestDetails:
    def test_job_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        job_details_boto = JobDetailsBoto(
            jobDetails={
                "jobId": job_id,
                "schemaVersion": "2022-09-01",
                "logGroupName": "fake-name",
            },
        )
        response: BatchGetJobEntityResponse = {
            "entities": [job_details_boto],
            "errors": [],
        }
        expected_details = JobDetails(
            schema_version=SchemaVersion("2022-09-01"),
            log_group_name="fake-name",
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
        details = job_entities.job_details()

        # THEN
        assert details == expected_details

    def test_environment_details(self, deadline_client: MagicMock, job_id: str):
        # GIVEN
        environment_id = "env-id"
        env_name = "TestEnv"
        details_boto = EnvironmentDetailsBoto(
            environmentDetails=EnvironmentDetailsData(
                jobId=job_id,
                environmentId=environment_id,
                schemaVersion="2022-09-01",
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
                attachments=Attachments(
                    manifests=[], assetLoadingMethod=AssetLoadingMethod.PRELOAD
                ),
            )
        )
        response: BatchGetJobEntityResponse = {
            "entities": [details_boto],
            "errors": [],
        }
        expected_details = JobAttachmentDetails(
            manifests=[], asset_loading_method=AssetLoadingMethod.PRELOAD
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
                schemaVersion="2022-09-01",
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
            "schemaVersion": "2022-09-01",
        }
        expected_environment_details: EnvironmentDetailsData = {
            "jobId": job_id,
            "environmentId": environment_id,
            "schemaVersion": "2022-09-01",
            # Don't actually need the full template for a test
            "template": {},
        }
        expected_step_details: StepDetailsData = {
            "jobId": job_id,
            "stepId": step_id,
            "schemaVersion": "2022-09-01",
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
                        "osType": "linux",
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
            schemaVersion="2022-09-01",
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
