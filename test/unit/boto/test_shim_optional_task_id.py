# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline_worker_agent.boto.shim import DeadlineClient


class TestDeadlineClientOptionalTaskId:
    """Tests for DeadlineClient handling optional task_id."""

    def test_parse_task_run_action_with_task_id(self):
        """Test parsing TaskRunAction with taskId field."""
        # Arrange
        response = {
            "assignedSessions": {
                "session-1": {
                    "queueId": "queue-123",
                    "jobId": "job-456",
                    "sessionActions": [
                        {
                            "sessionActionId": "action-789",
                            "definition": {
                                "taskRun": {
                                    "taskId": "task-123",
                                    "stepId": "step-456",
                                    "parameters": {"param1": "value1"},
                                }
                            },
                        }
                    ],
                }
            },
            "cancelSessionActions": {},
            "updateIntervalSeconds": 60,
        }

        # Act
        result = DeadlineClient._parse_update_worker_schedule_response(response)

        # Assert
        task_action = result["assignedSessions"]["session-1"]["sessionActions"][0]
        assert task_action["sessionActionId"] == "action-789"
        assert task_action["actionType"] == "TASK_RUN"
        assert task_action["stepId"] == "step-456"
        assert task_action["taskId"] == "task-123"
        assert task_action["parameters"] == {"param1": "value1"}

    def test_parse_task_run_action_without_task_id(self):
        """Test parsing TaskRunAction without taskId field."""
        # Arrange
        response = {
            "assignedSessions": {
                "session-1": {
                    "queueId": "queue-123",
                    "jobId": "job-456",
                    "sessionActions": [
                        {
                            "sessionActionId": "action-789",
                            "definition": {
                                "taskRun": {
                                    "stepId": "step-456",
                                    "parameters": {"param1": "value1"},
                                }
                            },
                        }
                    ],
                }
            },
            "cancelSessionActions": {},
            "updateIntervalSeconds": 60,
        }

        # Act
        result = DeadlineClient._parse_update_worker_schedule_response(response)

        # Assert
        task_action = result["assignedSessions"]["session-1"]["sessionActions"][0]
        assert task_action["sessionActionId"] == "action-789"
        assert task_action["actionType"] == "TASK_RUN"
        assert task_action["stepId"] == "step-456"
        assert "taskId" not in task_action
        assert task_action["parameters"] == {"param1": "value1"}

    def test_parse_task_run_action_with_none_task_id(self):
        """Test parsing TaskRunAction with None taskId."""
        # Arrange
        response = {
            "assignedSessions": {
                "session-1": {
                    "queueId": "queue-123",
                    "jobId": "job-456",
                    "sessionActions": [
                        {
                            "sessionActionId": "action-789",
                            "definition": {
                                "taskRun": {
                                    "taskId": None,
                                    "stepId": "step-456",
                                }
                            },
                        }
                    ],
                }
            },
            "cancelSessionActions": {},
            "updateIntervalSeconds": 60,
        }

        # Act
        result = DeadlineClient._parse_update_worker_schedule_response(response)

        # Assert
        task_action = result["assignedSessions"]["session-1"]["sessionActions"][0]
        assert task_action["sessionActionId"] == "action-789"
        assert task_action["actionType"] == "TASK_RUN"
        assert task_action["stepId"] == "step-456"
        assert "taskId" not in task_action
