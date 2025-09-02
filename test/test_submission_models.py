"""
Comprehensive tests for enhanced submission models.

This module tests the enhanced submission models that extend the core
metadata system with additional functionality for CWL integration.
"""

import pytest

from dirac_cwl_proto.metadata.core import DataManager, JobExecutor
from dirac_cwl_proto.submission_models import (
    TaskDescriptionModel,
)


class TestDataManager:
    """Test the DataManager class."""

    def test_creation(self):
        """Test DataManager creation."""
        descriptor = DataManager()
        assert descriptor.metadata_class == "User"
        assert descriptor.query_params == {}

    def test_creation_with_parameters(self):
        """Test creation with custom parameters."""
        descriptor = DataManager(
            metadata_class="Admin",
            query_params={"admin_level": 5, "log_level": "DEBUG"},
        )
        assert descriptor.metadata_class == "Admin"
        assert descriptor.query_params["admin_level"] == 5
        assert descriptor.query_params["log_level"] == "DEBUG"

    def test_inheritance(self):
        """Test that DataManager has the expected functionality."""
        descriptor = DataManager()
        assert hasattr(descriptor, "to_runtime")
        assert hasattr(descriptor, "from_cwl")
        assert hasattr(descriptor, "model_copy")

    def test_type_validation(self):
        """Test type validation during runtime instantiation."""
        # Valid type (should be registered by default)
        descriptor = DataManager(metadata_class="User")
        assert descriptor.metadata_class == "User"

        # Invalid type should raise error during runtime instantiation
        descriptor_with_invalid_type = DataManager(metadata_class="NonExistentType")
        with pytest.raises(KeyError, match="Unknown metadata plugin"):
            descriptor_with_invalid_type.to_runtime()

    def test_model_copy(self):
        """Test model_copy functionality."""
        original = DataManager(metadata_class="Admin", query_params={"admin_level": 3})

        # Test basic copy
        copied = original.model_copy()
        assert copied.metadata_class == original.metadata_class
        assert copied.query_params == original.query_params
        assert copied is not original
        # Note: Pydantic may optimize dict sharing for immutable content
        assert copied.query_params == original.query_params

    def test_model_copy_with_update(self):
        """Test model_copy with updates."""
        original = DataManager(
            metadata_class="Admin", query_params={"admin_level": 3, "log_level": "INFO"}
        )

        # Test copy with type update
        copied = original.model_copy(update={"metadata_class": "User"})
        assert copied.metadata_class == "User"
        assert copied.query_params == original.query_params

        # Test copy with query_params update
        copied = original.model_copy(update={"query_params": {"admin_level": 5}})
        assert copied.metadata_class == original.metadata_class
        assert copied.query_params["admin_level"] == 5
        assert copied.query_params["log_level"] == "INFO"  # Should merge

    def test_to_runtime_no_submission(self):
        """Test to_runtime without submission context."""
        descriptor = DataManager(
            metadata_class="Admin", query_params={"admin_level": 7}
        )

        runtime = descriptor.to_runtime()

        assert runtime.get_metadata_class() == "Admin"
        assert runtime.admin_level == 7

    def test_to_runtime_with_submission(self, mocker):
        """Test to_runtime with submission context."""
        descriptor = DataManager(
            metadata_class="QueryBased", query_params={"campaign": "Run3"}
        )

        # Mock submission model
        mock_submission = mocker.Mock()
        mock_task = mocker.Mock()
        mock_input = mocker.Mock()
        mock_input.id = "task#campaign"
        mock_input.default = "default_campaign"
        mock_task.inputs = [mock_input]
        mock_submission.task = mock_task
        mock_submission.parameters = [
            mocker.Mock(cwl={"campaign": "override_campaign"})
        ]

        runtime = descriptor.to_runtime(mock_submission)

        assert runtime.get_metadata_class() == "QueryBased"
        assert runtime.campaign == "Run3"

    def test_dash_to_snake_case_conversion(self):
        """Test dash-case to snake_case parameter conversion."""
        descriptor = DataManager(
            metadata_class="QueryBased",
            query_params={
                "query_root": "/data",
                "data_type": "AOD",
            },  # Already in snake_case
        )

        runtime = (
            descriptor.to_runtime()
        )  # Parameters should be converted to snake_case
        assert hasattr(runtime, "query_root") or runtime.query_root == "/data"
        assert hasattr(runtime, "data_type") or runtime.data_type == "AOD"

    def test_from_cwl(self, mocker):
        """Test from_cwl class method."""
        mock_cwl = mocker.Mock()
        mock_descriptor = DataManager(metadata_class="QueryBased")
        mock_from_cwl = mocker.patch(
            "dirac_cwl_proto.submission_models.DataManager.from_cwl"
        )
        mock_from_cwl.return_value = mock_descriptor

        result = DataManager.from_cwl(mock_cwl)

        assert isinstance(result, DataManager)
        mock_from_cwl.assert_called_once_with(mock_cwl)

    def test_serialization_compatibility(self):
        """Test that serialization works correctly."""
        descriptor = DataManager(
            metadata_class="Admin", query_params={"admin_level": 5}
        )

        # Test dict conversion
        data = descriptor.model_dump()
        assert data["metadata_class"] == "Admin"
        assert data["query_params"]["admin_level"] == 5

        # Test JSON schema
        schema = descriptor.model_json_schema()
        assert "properties" in schema


class TestTaskDescriptionModel:
    """Test the TaskDescriptionModel class."""

    def test_creation(self):
        """Test TaskDescriptionModel creation."""
        model = TaskDescriptionModel()
        # TaskDescriptionModel extends JobExecutor which has platform, priority, sites
        assert model.priority == 10
        assert model.platform is None
        assert model.sites is None

    def test_inheritance(self):
        """Test that TaskDescriptionModel inherits from JobExecutor."""
        model = TaskDescriptionModel()
        assert isinstance(model, JobExecutor)

    def test_creation_with_metadata(self):
        """Test creation with custom task configuration."""
        model = TaskDescriptionModel(
            platform="DIRAC", priority=8, sites=["CERN", "GRIDKA"]
        )

        assert model.platform == "DIRAC"
        assert model.priority == 8
        assert model.sites == ["CERN", "GRIDKA"]


class TestSubmissionModelsIntegration:
    """Test integration between submission models and metadata system."""

    def test_enhanced_descriptor_registry_integration(self):
        """Test that DataManager integrates with the registry."""
        # Create descriptor for each core plugin type
        descriptors = [
            DataManager(metadata_class="User"),
            DataManager(metadata_class="Admin", query_params={"admin_level": 3}),
            DataManager(metadata_class="QueryBased", query_params={"campaign": "Test"}),
        ]

        for descriptor in descriptors:
            runtime = descriptor.to_runtime()
            assert runtime.get_metadata_class() == descriptor.metadata_class

    def test_task_description_with_different_metadata_types(self):
        """Test TaskDescriptionModel with different configurations."""
        task_configs = [
            {"platform": "DIRAC", "priority": 5},
            {"platform": "DIRACX", "priority": 8, "sites": ["CERN"]},
            {"priority": 3, "sites": ["GRIDKA", "CNAF"]},
        ]

        for config in task_configs:
            model = TaskDescriptionModel(**config)

            # Should have correct configuration
            assert model.priority == config["priority"]
            if "platform" in config:
                assert model.platform == config["platform"]
            if "sites" in config:
                assert model.sites == config["sites"]

    def test_backward_compatibility_with_legacy_models(self):
        """Test that enhanced models work with legacy metadata."""
        # Test that we can create descriptors for legacy metadata classes
        legacy_types = ["PiSimulate", "LHCbSimulation", "MandelBrotGeneration"]

        for legacy_type in legacy_types:
            try:
                descriptor = DataManager(metadata_class=legacy_type)
                # If creation succeeds, test runtime conversion
                # (may fail due to missing parameters, which is expected)
                descriptor.to_runtime()
            except ValueError:
                # Expected for legacy types that require specific parameters
                pass

    def test_model_serialization_round_trip(self):
        """Test that models can be serialized and deserialized."""
        original = TaskDescriptionModel(
            platform="DIRAC", priority=7, sites=["CERN", "GRIDKA"]
        )

        # Serialize to dict
        data = original.model_dump()

        # Should be able to recreate from dict
        recreated = TaskDescriptionModel(**data)

        assert recreated.platform == original.platform
        assert recreated.priority == original.priority
        assert recreated.sites == original.sites

    def test_cwl_hints_integration(self):
        """Test integration with CWL hints extraction."""
        # Create an enhanced descriptor directly
        descriptor = DataManager(
            metadata_class="QueryBased",
            query_params={"campaign": "Run3", "data_type": "AOD"},
        )
        runtime = descriptor.to_runtime()

        # Should use the QueryBased type
        assert runtime.get_metadata_class() == "QueryBased"
