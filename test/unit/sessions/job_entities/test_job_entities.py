# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Generator, Optional, cast
from unittest.mock import MagicMock, patch

from deadline.job_attachments.models import JobAttachmentsFileSystem
from openjd.model import (
    ParameterValue,
    ParameterValueType,
    SpecificationRevision,
    TemplateSpecificationVersion,
)
from openjd.model.v2023_09 import (
    Action,
    Environment,
    EnvironmentActions,
    EnvironmentScript,
    StepActions,
    StepScript,
    StepTemplate,
)
from openjd.sessions import PosixSessionUser, WindowsSessionUser, SessionUser


import pytest
import os

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
from deadline_worker_agent.config import JobsRunAsUserOverride
from deadline_worker_agent.sessions.job_entities import (
    EnvironmentDetails,
    JobAttachmentDetails,
    JobDetails,
    JobEntities,
    StepDetails,
)
from deadline_worker_agent.sessions.job_entities.job_details import (
    JobRunAsUser,
    JobRunAsWindowsUser,
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


@pytest.fixture
def os_user() -> SessionUser:
    if os.name == "posix":
        return PosixSessionUser(user="user", group="group")
    else:
        return WindowsSessionUser(user="user", password="fakepassword")


@pytest.fixture
def windows_credentials_resolver(os_user: MagicMock) -> Optional[MagicMock]:
    if os.name == "nt":
        resolver = MagicMock()
        resolver.get_windows_session_user.return_value = os_user
        return resolver
    else:
        return None


@pytest.fixture
def job_details_parameters() -> dict[str, ParameterValue]:
    return {
        "p_string": ParameterValue(type=ParameterValueType.STRING, value="string_value"),
        "p_int": ParameterValue(type=ParameterValueType.INT, value="1"),
        "p_float": ParameterValue(type=ParameterValueType.FLOAT, value="1.2"),
        "p_path": ParameterValue(type=ParameterValueType.PATH, value="/tmp/share"),
    }


@pytest.fixture
def job_details_with_user(
    os_user: SessionUser, job_details_parameters: dict[str, ParameterValue]
) -> JobDetails:
    if os.name == "posix":
        posix_user = cast(PosixSessionUser, os_user)
        return JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision("2023-09"),
            job_run_as_user=JobRunAsUser(posix=posix_user),
            parameters=job_details_parameters,
        )
    else:
        windows_user = cast(WindowsSessionUser, os_user)
        return JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision("2023-09"),
            job_run_as_user=JobRunAsUser(windows=windows_user),
            parameters=job_details_parameters,
        )


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
    windows_credentials_resolver: MagicMock,
    job_id: str,
) -> Generator[JobEntities, None, None]:
    job_entities = JobEntities(
        farm_id="farm-id",
        fleet_id="fleet-id",
        worker_id="worker-id",
        job_id=job_id,
        deadline_client=deadline_client,
        windows_credentials_resolver=windows_credentials_resolver,
        job_run_as_user_override=None,
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
        windows_credentials_resolver: MagicMock,
        path_mapping_rules: list[PathMappingRule] | None,
    ) -> None:
        # GIVEN
        job_id = "job-fedcba0987654321fedcba0987654321"
        job_details_boto = JobDetailsBoto(
            jobDetails={
                "jobId": job_id,
                "schemaVersion": "jobtemplate-2023-09",
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
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
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

    def test_job_run_as_user(
        self,
    ) -> None:
        """Ensures that if we receive a job_run_as_user field in the response,
        that the created entity has a SessionUser created with the
        proper values"""
        # GIVEN
        expected_user = "job-user"
        expected_group = "job-group"
        expected_password_arn = "job-password-arn"
        api_response: dict = {
            "jobId": "job-123",
            "jobRunAsUser": {
                "runAs": "QUEUE_CONFIGURED_USER",
                "posix": {
                    "user": expected_user,
                    "group": expected_group,
                },
                "windows": {
                    "user": expected_user,
                    "passwordArn": expected_password_arn,
                },
            },
            "logGroupName": "TEST",
            "schemaVersion": TemplateSpecificationVersion.JOBTEMPLATE_v2023_09.value,
        }

        # WHEN
        job_details_data = JobDetails.validate_entity_data(
            api_response, job_user_override=JobsRunAsUserOverride(run_as_agent=False, job_user=None)
        )
        entity_obj = JobDetails.from_boto(job_details_data)

        # THEN
        assert entity_obj.job_run_as_user is not None
        if os.name == "posix":
            assert isinstance(entity_obj.job_run_as_user.posix, PosixSessionUser)
            assert entity_obj.job_run_as_user.posix.user == expected_user
            assert entity_obj.job_run_as_user.posix.group == expected_group
        else:
            assert isinstance(entity_obj.job_run_as_user.windows_settings, JobRunAsWindowsUser)
            assert entity_obj.job_run_as_user.windows_settings.user == expected_user
            assert entity_obj.job_run_as_user.windows_settings.passwordArn == expected_password_arn

    @pytest.mark.parametrize(
        "job_run_as_user",
        [
            dict([key_val for key_val in (runAs, posix, windows) if key_val is not None])
            for runAs in (("runAs", "QUEUE_CONFIGURED_USER"), ("runAs", "WORKER_AGENT_USER"))
            for posix in (
                (
                    "posix",
                    {
                        "user": "job-user",
                        "group": "job-group",
                    },
                ),
                None,
            )
            for windows in (
                (
                    "windows",
                    {
                        "user": "job-user",
                        "passwordArn": "job-password-arn",
                    },
                ),
                None,
            )
        ],
    )
    def test_job_run_as_user_with_override(
        self, job_run_as_user: dict, job_run_as_user_overrides: JobsRunAsUserOverride
    ) -> None:
        """Ensures that if we receive a job_run_as_user field in the response,
        that the created entity has a SessionUser created with the
        proper values"""
        # GIVEN
        api_response: dict = {
            "jobId": "job-123",
            "jobRunAsUser": job_run_as_user,
            "logGroupName": "TEST",
            "schemaVersion": TemplateSpecificationVersion.JOBTEMPLATE_v2023_09.value,
        }

        # WHEN
        job_details_data = JobDetails.validate_entity_data(
            api_response, job_user_override=job_run_as_user_overrides
        )
        entity_obj = JobDetails.from_boto(job_details_data)

        # THEN
        assert "jobRunAsUser" not in job_details_data
        assert entity_obj.job_run_as_user is None


class TestDetails:
    def test_job_details(
        self,
        deadline_client: MagicMock,
        windows_credentials_resolver: MagicMock,
        job_details_with_user: MagicMock,
        os_user: MagicMock,
        job_id: str,
    ):
        # GIVEN
        job_details_boto = JobDetailsBoto(
            jobDetails={
                "jobId": job_id,
                "schemaVersion": "jobtemplate-2023-09",
                "logGroupName": "/aws/deadline/queue-0000",
                "jobRunAsUser": {
                    "runAs": "QUEUE_CONFIGURED_USER",
                    "posix": {
                        "user": "user",
                        "group": "group",
                    },
                    "windows": {
                        "user": "job-user",
                        "passwordArn": "job-password-arn",
                    },
                },
                "parameters": {
                    "p_string": {"string": "string_value"},
                    "p_int": {"int": "1"},
                    "p_float": {"float": "1.2"},
                    "p_path": {"path": "/tmp/share"},
                },
            },
        )
        response: BatchGetJobEntityResponse = {
            "entities": [job_details_boto],
            "errors": [],
        }
        expected_details = job_details_with_user
        assert expected_details.job_run_as_user is not None  # For type checker
        if os.name == "posix":
            assert expected_details.job_run_as_user.posix is not None  # For type checker
        else:
            assert expected_details.job_run_as_user.windows is not None  # For type checker
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
        )

        # WHEN
        details = job_entities.job_details()

        # THEN
        assert details.log_group_name == expected_details.log_group_name
        assert details.schema_version == expected_details.schema_version
        assert details.job_run_as_user is not None
        if os.name == "posix":
            assert details.job_run_as_user.windows is None
            assert details.job_run_as_user.posix is not None
            assert details.job_run_as_user.posix.user == expected_details.job_run_as_user.posix.user
            assert (
                details.job_run_as_user.posix.group == expected_details.job_run_as_user.posix.group
            )
        else:
            assert details.job_run_as_user.windows is not None
            assert details.job_run_as_user.posix is None
            assert (
                details.job_run_as_user.windows.user
                == expected_details.job_run_as_user.windows.user
            )
        assert details.job_attachment_settings == expected_details.job_attachment_settings
        assert details.parameters == expected_details.parameters
        assert details.path_mapping_rules == expected_details.path_mapping_rules
        assert details.queue_role_arn == expected_details.queue_role_arn

    def test_environment_details(
        self, deadline_client: MagicMock, windows_credentials_resolver: MagicMock, job_id: str
    ):
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
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
        )

        # WHEN
        details = job_entities.environment_details(environment_id=environment_id)

        # THEN
        assert details == expected_details

    def test_job_attachment_details(
        self, deadline_client: MagicMock, windows_credentials_resolver: MagicMock, job_id: str
    ):
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
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
        )

        # WHEN
        details = job_entities.job_attachment_details()

        # THEN
        assert details == expected_details

    def test_step_details_backwards_compat(
        self, deadline_client: MagicMock, windows_credentials_resolver: MagicMock, job_id: str
    ):
        # TODO - Delete this test once the StepDetails from BatchGetJobEntities is returning
        #  a StepTemplate instead of a StepScript.

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
            step_template=StepTemplate(
                name="Placeholder",
                script=StepScript(actions=StepActions(onRun=Action(command="test.exe"))),
            ),
            step_id=step_id,
            dependencies=[dependency],
        )
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
        )

        # WHEN
        details = job_entities.step_details(step_id=step_id)

        # THEN
        assert details == expected_details

    def test_step_details(
        self, deadline_client: MagicMock, windows_credentials_resolver: MagicMock, job_id: str
    ):
        # GIVEN
        step_id = "step-id"
        dependency = "stepId-1234"
        details_boto = StepDetailsBoto(
            stepDetails=StepDetailsData(
                jobId=job_id,
                stepId=step_id,
                schemaVersion="jobtemplate-2023-09",
                template={
                    "name": "Test",
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "test.exe",
                            },
                        },
                    },
                },
                dependencies=[dependency],
            )
        )
        response: BatchGetJobEntityResponse = {
            "entities": [details_boto],
            "errors": [],
        }

        expected_details = StepDetails(
            step_template=StepTemplate(
                name="Test",
                script=StepScript(actions=StepActions(onRun=Action(command="test.exe"))),
            ),
            step_id=step_id,
            dependencies=[dependency],
        )
        deadline_client.batch_get_job_entity.return_value = response
        job_entities = JobEntities(
            farm_id="farm-id",
            fleet_id="fleet-id",
            worker_id="worker-id",
            job_id=job_id,
            deadline_client=deadline_client,
            windows_credentials_resolver=windows_credentials_resolver,
            job_run_as_user_override=None,
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
                "runAs": "QUEUE_CONFIGURED_USER",
                "posix": {
                    "user": "job-user",
                    "group": "job-group",
                },
                "windows": {
                    "user": "job-user",
                    "group": "job-group",
                    "passwordArn": "job-password-arn",
                },
            },
            "parameters": {
                "p_string": {"string": "string_value"},
                "p_int": {"int": "1"},
                "p_float": {"float": "1.2"},
                "p_path": {"path": "/tmp/share"},
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
        assert job_entities._entity_record_map.get(job_id) is not None
        assert job_entities._entity_record_map.get(environment_id) is not None
        assert job_entities._entity_record_map.get(step_id) is not None
        assert job_entities._entity_record_map.get(f"JA({job_id})") is not None
        assert job_entities._entity_record_map[job_id].data == expected_job_details
        assert job_entities._entity_record_map[environment_id].data == expected_environment_details
        assert job_entities._entity_record_map[step_id].data == expected_step_details
        assert job_entities._entity_record_map[f"JA({job_id})"].data == expected_attachment_details

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
