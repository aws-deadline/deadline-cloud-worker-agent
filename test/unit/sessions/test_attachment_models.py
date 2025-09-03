# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Unit tests for worker-specific data structures.
"""

import pytest

from deadline_worker_agent.sessions.attachment_models import (
    WorkerManifestProperties,
)
from deadline.job_attachments.models import (
    ManifestProperties,
    PathFormat,
    PathMappingRule,
)


class TestWorkerManifestProperties:
    """Test cases for WorkerManifestProperties class."""

    def test_initialization_with_required_fields(self):
        """Test WorkerManifestProperties initialization with required fields."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared_storage",
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == ["/local/manifest.json"]

    def test_initialization_without_local_root_path_raises_error(self):
        """Test that missing local_root_path raises ValueError."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        with pytest.raises(ValueError, match="local_root_path must be set"):
            WorkerManifestProperties(manifest_properties=manifest_props, local_root_path="")

    def test_property_accessors(self):
        """Test property accessors for manifest properties."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.WINDOWS,
            fileSystemLocationName="shared_storage",
            inputManifestPath="input.json",
            inputManifestHash="hash123",
            outputRelativeDirectories=["out1", "out2"],
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.WINDOWS
        assert worker_props.file_system_location_name == "shared_storage"
        assert worker_props.input_manifest_path == "input.json"
        assert worker_props.input_manifest_hash == "hash123"
        assert worker_props.output_relative_directories == ["out1", "out2"]

    def test_to_path_mapping_rule(self):
        """Test conversion to path mapping rule."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        rule = worker_props.to_path_mapping_rule()

        assert isinstance(rule, PathMappingRule)
        assert rule.source_path_format == "posix"
        assert rule.source_path == "/source/path"
        assert rule.destination_path == "/local/root"

    def test_from_manifest_properties_class_method(self):
        """Test creating WorkerManifestProperties from class method."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        worker_props = WorkerManifestProperties.from_manifest_properties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == ["/local/manifest.json"]

    def test_from_manifest_properties_without_manifest_path(self):
        """Test creating WorkerManifestProperties without local manifest path."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        worker_props = WorkerManifestProperties.from_manifest_properties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == []

    def test_property_accessors_with_none_values(self):
        """Test property accessors when optional fields are None."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName=None,
            inputManifestPath=None,
            inputManifestHash=None,
            outputRelativeDirectories=None,
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.POSIX
        assert worker_props.file_system_location_name is None
        assert worker_props.input_manifest_path is None
        assert worker_props.input_manifest_hash is None
        assert worker_props.output_relative_directories is None

    def test_to_path_mapping_rule_with_windows_format(self):
        """Test conversion to path mapping rule with Windows path format."""
        manifest_props = ManifestProperties(
            rootPath="C:\\source\\path", rootPathFormat=PathFormat.WINDOWS
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="C:\\local\\root"
        )

        rule = worker_props.to_path_mapping_rule()

        assert isinstance(rule, PathMappingRule)
        assert rule.source_path_format == "windows"
        assert rule.source_path == "C:\\source\\path"
        assert rule.destination_path == "C:\\local\\root"

    def test_initialization_with_empty_local_root_path_raises_error(self):
        """Test that empty string local_root_path raises ValueError."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        with pytest.raises(ValueError, match="local_root_path must be set"):
            WorkerManifestProperties(manifest_properties=manifest_props, local_root_path="")

    def test_manifest_properties_with_complex_paths(self):
        """Test WorkerManifestProperties with complex file paths."""
        manifest_props = ManifestProperties(
            rootPath="/complex/path/with spaces/and-dashes_underscores",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="complex_storage_name",
            inputManifestPath="manifests/complex/input_manifest_v2.json",
            inputManifestHash="sha256:abcdef1234567890",
            outputRelativeDirectories=["output/renders", "output/logs", "temp/cache"],
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/session/complex_path_hash",
            local_manifest_paths=["/local/session/manifests/complex_manifest.json"],
        )

        assert worker_props.root_path == "/complex/path/with spaces/and-dashes_underscores"
        assert worker_props.file_system_location_name == "complex_storage_name"
        assert worker_props.input_manifest_path == "manifests/complex/input_manifest_v2.json"
        assert worker_props.input_manifest_hash == "sha256:abcdef1234567890"
        assert worker_props.output_relative_directories is not None
        assert len(worker_props.output_relative_directories) == 3
        assert "output/renders" in worker_props.output_relative_directories
        assert worker_props.local_root_path == "/local/session/complex_path_hash"
        assert worker_props.local_manifest_paths == [
            "/local/session/manifests/complex_manifest.json"
        ]

    def test_equality_and_comparison(self):
        """Test equality comparison between WorkerManifestProperties instances."""
        manifest_props1 = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        manifest_props2 = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        worker_props1 = WorkerManifestProperties(
            manifest_properties=manifest_props1,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        worker_props2 = WorkerManifestProperties(
            manifest_properties=manifest_props2,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        worker_props3 = WorkerManifestProperties(
            manifest_properties=manifest_props1,
            local_root_path="/different/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        # Test equality (dataclass should provide __eq__)
        assert worker_props1 == worker_props2
        assert worker_props1 != worker_props3

    def test_from_manifest_properties_with_class_method_validation(self):
        """Test that class method properly validates inputs."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        # Test with valid inputs
        worker_props = WorkerManifestProperties.from_manifest_properties(
            manifest_properties=manifest_props, local_root_path="/valid/root"
        )
        assert worker_props.local_root_path == "/valid/root"

        # Test that validation still occurs in class method
        with pytest.raises(ValueError, match="local_root_path must be set"):
            WorkerManifestProperties.from_manifest_properties(
                manifest_properties=manifest_props, local_root_path=""
            )

    def test_local_manifest_paths_default_factory(self):
        """Test that each instance gets its own local_manifest_paths list (mutable default fix)."""
        manifest_props1 = ManifestProperties(
            rootPath="/source/path1", rootPathFormat=PathFormat.POSIX
        )
        manifest_props2 = ManifestProperties(
            rootPath="/source/path2", rootPathFormat=PathFormat.POSIX
        )

        # Create two instances without specifying local_manifest_paths
        worker_props1 = WorkerManifestProperties(
            manifest_properties=manifest_props1, local_root_path="/local/root1"
        )
        worker_props2 = WorkerManifestProperties(
            manifest_properties=manifest_props2, local_root_path="/local/root2"
        )

        # Verify each instance has its own empty list
        assert worker_props1.local_manifest_paths == []
        assert worker_props2.local_manifest_paths == []
        assert worker_props1.local_manifest_paths is not worker_props2.local_manifest_paths

        # Modify one instance's list and verify the other is unaffected
        worker_props1.local_manifest_paths.append("/path/to/manifest1.json")
        assert worker_props1.local_manifest_paths == ["/path/to/manifest1.json"]
        assert worker_props2.local_manifest_paths == []

        # Modify the second instance's list
        worker_props2.local_manifest_paths.extend(
            ["/path/to/manifest2.json", "/path/to/manifest3.json"]
        )
        assert worker_props1.local_manifest_paths == ["/path/to/manifest1.json"]
        assert worker_props2.local_manifest_paths == [
            "/path/to/manifest2.json",
            "/path/to/manifest3.json",
        ]

    def test_local_manifest_paths_initialization_with_provided_list(self):
        """Test initialization with explicitly provided local_manifest_paths."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        provided_paths = ["/provided/manifest1.json", "/provided/manifest2.json"]
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=provided_paths,
        )

        # Verify the provided list is used
        assert worker_props.local_manifest_paths == provided_paths
        # Verify it's the same reference (not a copy)
        assert worker_props.local_manifest_paths is provided_paths

    def test_local_manifest_paths_modification_after_initialization(self):
        """Test that local_manifest_paths can be modified after initialization."""
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # Start with empty list
        assert worker_props.local_manifest_paths == []

        # Add paths using various list methods
        worker_props.local_manifest_paths.append("/manifest1.json")
        assert worker_props.local_manifest_paths == ["/manifest1.json"]

        worker_props.local_manifest_paths.extend(["/manifest2.json", "/manifest3.json"])
        assert len(worker_props.local_manifest_paths) == 3
        assert "/manifest2.json" in worker_props.local_manifest_paths
        assert "/manifest3.json" in worker_props.local_manifest_paths

        # Test insertion
        worker_props.local_manifest_paths.insert(0, "/manifest0.json")
        assert worker_props.local_manifest_paths[0] == "/manifest0.json"
        assert len(worker_props.local_manifest_paths) == 4

    def test_to_dict_serialization(self):
        """Test converting WorkerManifestProperties to dictionary."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared_storage",
            inputManifestPath="input.json",
            inputManifestHash="hash123",
            outputRelativeDirectories=["out1", "out2"],
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest1.json", "/local/manifest2.json"],
        )

        result_dict = worker_props.to_dict()

        # Verify structure
        assert "manifestProperties" in result_dict
        assert "localManifestPaths" in result_dict
        assert "localRootPath" in result_dict
        # Verify manifest field is not present
        assert "manifest" not in result_dict

        # Verify manifestProperties content
        manifest_data = result_dict["manifestProperties"]
        assert manifest_data["rootPath"] == "/source/path"
        assert manifest_data["rootPathFormat"] == "posix"
        assert manifest_data["fileSystemLocationName"] == "shared_storage"
        assert manifest_data["inputManifestPath"] == "input.json"
        assert manifest_data["inputManifestHash"] == "hash123"
        assert manifest_data["outputRelativeDirectories"] == ["out1", "out2"]

        # Verify other fields
        assert result_dict["localManifestPaths"] == [
            "/local/manifest1.json",
            "/local/manifest2.json",
        ]
        assert result_dict["localRootPath"] == "/local/root"

    def test_to_dict_with_none_values(self):
        """Test to_dict with None values for optional fields."""
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.WINDOWS,
            fileSystemLocationName=None,
            inputManifestPath=None,
            inputManifestHash=None,
            outputRelativeDirectories=None,
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        result_dict = worker_props.to_dict()

        manifest_data = result_dict["manifestProperties"]
        assert manifest_data["rootPath"] == "/source/path"
        assert manifest_data["rootPathFormat"] == "windows"
        # None values are not included in the dictionary by ManifestProperties.to_dict()
        assert "fileSystemLocationName" not in manifest_data
        assert "inputManifestPath" not in manifest_data
        assert "inputManifestHash" not in manifest_data
        assert "outputRelativeDirectories" not in manifest_data

        assert result_dict["localManifestPaths"] == []
        assert result_dict["localRootPath"] == "/local/root"

    def test_from_dict_deserialization(self):
        """Test creating WorkerManifestProperties from dictionary."""
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "posix",
                "fileSystemLocationName": "shared_storage",
                "inputManifestPath": "input.json",
                "inputManifestHash": "hash123",
                "outputRelativeDirectories": ["out1", "out2"],
            },
            "localManifestPaths": ["/local/manifest1.json", "/local/manifest2.json"],
            "localRootPath": "/local/root",
        }

        worker_props = WorkerManifestProperties.from_dict(data)

        # Verify manifest properties
        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.POSIX
        assert worker_props.file_system_location_name == "shared_storage"
        assert worker_props.input_manifest_path == "input.json"
        assert worker_props.input_manifest_hash == "hash123"
        assert worker_props.output_relative_directories == ["out1", "out2"]

        # Verify other fields
        assert worker_props.local_manifest_paths == [
            "/local/manifest1.json",
            "/local/manifest2.json",
        ]
        assert worker_props.local_root_path == "/local/root"

    def test_from_dict_with_missing_optional_fields(self):
        """Test from_dict with missing optional fields."""
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "windows",
            },
            "localRootPath": "/local/root",
        }

        worker_props = WorkerManifestProperties.from_dict(data)

        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.WINDOWS
        assert worker_props.file_system_location_name is None
        assert worker_props.input_manifest_path is None
        assert worker_props.input_manifest_hash is None
        assert worker_props.output_relative_directories is None
        assert worker_props.local_manifest_paths == []  # Default empty list
        assert worker_props.local_root_path == "/local/root"

    def test_from_dict_missing_required_fields_raises_error(self):
        """Test that from_dict raises KeyError for missing required fields."""
        # Missing manifestProperties
        data_missing_manifest = {
            "localRootPath": "/local/root",
        }
        with pytest.raises(KeyError):
            WorkerManifestProperties.from_dict(data_missing_manifest)

        # Missing localRootPath
        data_missing_root = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "posix",
            },
        }
        with pytest.raises(KeyError):
            WorkerManifestProperties.from_dict(data_missing_root)

        # Missing rootPath in manifestProperties
        data_missing_root_path = {
            "manifestProperties": {
                "rootPathFormat": "posix",
            },
            "localRootPath": "/local/root",
        }
        with pytest.raises(KeyError):
            WorkerManifestProperties.from_dict(data_missing_root_path)

    def test_from_dict_invalid_path_format_raises_error(self):
        """Test that from_dict raises ValueError for invalid path format."""
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "invalid_format",
            },
            "localRootPath": "/local/root",
        }

        with pytest.raises(ValueError):
            WorkerManifestProperties.from_dict(data)

    def test_dict_roundtrip_serialization(self):
        """Test that dict serialization and deserialization are symmetric."""
        # Create original object with minimal data
        manifest_props = ManifestProperties(
            rootPath="/minimal/path",
            rootPathFormat=PathFormat.POSIX,
        )

        original_worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/minimal",
        )

        # Serialize to dict and back
        data_dict = original_worker_props.to_dict()
        reconstructed_worker_props = WorkerManifestProperties.from_dict(data_dict)

        # Verify they are equal
        assert reconstructed_worker_props == original_worker_props
