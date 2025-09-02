"""
Tests for the core metadata plugin system.

This module tests the foundational classes and interfaces of the metadata
plugin system, including TaskRuntimeBasePlugin, DataManager, and core
abstract interfaces.
"""

from pathlib import Path
from typing import Any, List, Optional, Union

import pytest

from dirac_cwl_proto.metadata.core import (
    DataCatalogInterface,
    DataManager,
    ExecutionHooks,
    SchedulingHint,
    TaskRuntimeBasePlugin,
    TransformationDataManager,
)


class TestExecutionHooks:
    """Test the ExecutionHooks abstract base class."""

    def test_abstract_methods(self):
        """Test that ExecutionHooks cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ExecutionHooks()

    def test_concrete_implementation(self):
        """Test that concrete implementations work correctly."""

        class ConcreteHook(ExecutionHooks):
            def pre_process(self, job_path: Path, command: List[str]) -> List[str]:
                return command + ["--processed"]

            def post_process(self, job_path: Path) -> bool:
                return True

        processor = ConcreteHook()

        # Test pre_process
        result = processor.pre_process(Path("/tmp"), ["echo", "hello"])
        assert result == ["echo", "hello", "--processed"]

        # Test post_process
        assert processor.post_process(Path("/tmp")) is True


class TestDataCatalogInterface:
    """Test the DataCatalogInterface abstract base class."""

    def test_abstract_methods(self):
        """Test that DataCatalogInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            DataCatalogInterface()

    def test_concrete_implementation(self):
        """Test that concrete implementations work correctly."""

        class ConcreteCatalog(DataCatalogInterface):
            def get_input_query(
                self, input_name: str, **kwargs: Any
            ) -> Union[Path, List[Path], None]:
                return Path(f"/data/{input_name}")

            def get_output_query(self, output_name: str) -> Optional[Path]:
                return Path(f"/output/{output_name}")

            def store_output(self, output_name: str, src_path: str) -> None:
                pass

        catalog = ConcreteCatalog()

        # Test get_input_query
        result = catalog.get_input_query("test_input")
        assert result == Path("/data/test_input")

        # Test get_output_query
        result = catalog.get_output_query("test_output")
        assert result == Path("/output/test_output")

        # Test store_output
        catalog.store_output("test_output", "/tmp/test")  # Should not raise an error


class TestTaskRuntimeBasePlugin:
    """Test the TaskRuntimeBasePlugin foundation class."""

    def test_creation(self):
        """Test TaskRuntimeBasePlugin can be instantiated."""

        class TestModel(TaskRuntimeBasePlugin):
            test_field: str = "default"

        model = TestModel()
        assert model.test_field == "default"

        model = TestModel(test_field="custom")
        assert model.test_field == "custom"

    def test_pydantic_validation(self):
        """Test that Pydantic validation works correctly."""

        class TestModel(TaskRuntimeBasePlugin):
            required_field: str
            optional_field: Optional[int] = None

        # Test valid creation
        model = TestModel(required_field="test")
        assert model.required_field == "test"
        assert model.optional_field is None

        # Test validation error
        with pytest.raises(ValueError):
            TestModel()  # Missing required_field

    def test_default_interface_methods(self):
        """Test that default interface methods are implemented."""

        class TestModel(TaskRuntimeBasePlugin):
            pass

        model = TestModel()

        # Test ExecutionHooks methods
        result = model.pre_process(Path("/tmp"), ["echo"])
        assert result == ["echo"]

        assert model.post_process(Path("/tmp")) is True

        # Test DataCatalogInterface methods
        assert model.get_input_query("test") is None
        assert model.get_output_query("test") is None

        # Test store_output raises RuntimeError when no output path is defined
        with pytest.raises(RuntimeError, match="No output path defined"):
            model.store_output("test", "/tmp/file.txt")

    def test_model_serialization(self):
        """Test that model serialization works correctly."""

        class TestModel(TaskRuntimeBasePlugin):
            name: str
            value: int = 42

        model = TestModel(name="test")

        # Test dict conversion
        data = model.model_dump()
        assert data == {"name": "test", "value": 42}

        # Test JSON schema generation
        schema = model.model_json_schema()
        assert "properties" in schema
        assert "name" in schema["properties"]
        assert "value" in schema["properties"]


class TestDataManager:
    """Test the DataManager class."""

    def test_creation(self):
        """Test DataManager creation."""
        descriptor = DataManager(metadata_class="User")
        assert descriptor.metadata_class == "User"
        assert descriptor.vo is None
        assert descriptor.version is None

    def test_creation_with_all_fields(self):
        """Test DataManager creation with all fields."""
        descriptor = DataManager(
            metadata_class="LHCbSimulation",
            vo="lhcb",
            version="2.0",
            custom_param="value",
        )
        assert descriptor.metadata_class == "LHCbSimulation"
        assert descriptor.vo == "lhcb"
        assert descriptor.version == "2.0"
        assert descriptor.custom_param == "value"

    def test_from_cwl(self, mocker):
        """Test extraction from CWL hints."""
        # Mock CWL document
        mock_cwl = mocker.Mock()
        mock_cwl.hints = [
            {
                "class": "dirac:data-management",
                "metadata_class": "QueryBased",
                "vo": "lhcb",
                "campaign": "Run3",
            },
            {"class": "ResourceRequirement", "coresMin": 2},
        ]

        descriptor = DataManager.from_cwl(mock_cwl)

        assert descriptor.metadata_class == "QueryBased"
        assert descriptor.vo == "lhcb"
        assert descriptor.campaign == "Run3"

    def test_from_cwl_no_hints(self, mocker):
        """Test extraction when no hints are present."""
        mock_cwl = mocker.Mock()
        mock_cwl.hints = None

        descriptor = DataManager.from_cwl(mock_cwl)

        # Should create default descriptor
        assert descriptor.metadata_class == "User"

    def test_from_cwl_no_dirac_hints(self, mocker):
        """Test extraction when no DIRAC hints are present."""
        mock_cwl = mocker.Mock()
        mock_cwl.hints = [{"class": "ResourceRequirement", "coresMin": 2}]

        descriptor = DataManager.from_cwl(mock_cwl)

        # Should create default descriptor
        assert descriptor.metadata_class == "User"

    def test_model_copy_merges_dict_fields(self):
        """Test model_copy merges dict fields and updates values."""
        descriptor = DataManager(metadata_class="LHCbSimulation", vo="lhcb")

        updated = descriptor.model_copy(
            update={"metadata_class": "NewClass", "new_field": "value"}
        )

        assert updated.metadata_class == "NewClass"
        assert updated.vo == "lhcb"
        assert getattr(updated, "new_field", None) == "value"

    def test_default_values(self):
        """Test default values without VO."""
        descriptor = DataManager(metadata_class="User", user_id="test123")

        assert descriptor.metadata_class == "User"
        assert descriptor.vo is None
        assert getattr(descriptor, "user_id", None) == "test123"


class TestSchedulingHint:
    """Test the SchedulingHint class."""

    def test_creation(self):
        """Test SchedulingHint creation."""
        descriptor = SchedulingHint()
        assert descriptor.platform is None
        assert descriptor.priority == 10
        assert descriptor.sites is None

    def test_creation_with_values(self):
        """Test SchedulingHint creation with values."""
        descriptor = SchedulingHint(
            platform="DIRAC", priority=5, sites=["LCG.CERN.ch", "LCG.IN2P3.fr"]
        )
        assert descriptor.platform == "DIRAC"
        assert descriptor.priority == 5
        assert descriptor.sites == ["LCG.CERN.ch", "LCG.IN2P3.fr"]

    def test_from_cwl(self, mocker):
        """Test extraction from CWL hints."""
        mock_cwl = mocker.Mock()
        mock_cwl.hints = [
            {
                "class": "dirac:job-execution",
                "platform": "DIRAC-v8",
                "priority": 8,
                "sites": ["LCG.CERN.ch"],
            }
        ]

        descriptor = SchedulingHint.from_cwl(mock_cwl)

        assert descriptor.platform == "DIRAC-v8"
        assert descriptor.priority == 8
        assert descriptor.sites == ["LCG.CERN.ch"]

    def test_serialization(self):
        """Test SchedulingHint serialization."""
        descriptor = SchedulingHint(
            platform="DIRAC", priority=7, sites=["LCG.CERN.ch", "LCG.IN2P3.fr"]
        )

        # Test model serialization
        data = descriptor.model_dump()

        assert data["platform"] == "DIRAC"
        assert data["priority"] == 7
        assert data["sites"] == ["LCG.CERN.ch", "LCG.IN2P3.fr"]


class TestTransformationDataManager:
    """Test the TransformationDataManager class."""

    def test_creation(self):
        """Test TransformationDataManager creation."""
        descriptor = TransformationDataManager(
            metadata_class="QueryBased", group_size={"input_data": 100}
        )
        assert descriptor.metadata_class == "QueryBased"
        assert descriptor.group_size == {"input_data": 100}

    def test_inheritance(self):
        """Test that it inherits from DataManager."""
        descriptor = TransformationDataManager(
            metadata_class="LHCbSimulation",
            vo="lhcb",
            group_size={"sim_data": 50},
            n_events=1000,
        )

        # Test that it has the fields from both classes
        assert descriptor.metadata_class == "LHCbSimulation"
        assert descriptor.vo == "lhcb"
        assert descriptor.group_size == {"sim_data": 50}
        assert getattr(descriptor, "n_events", None) == 1000

    def test_validation(self):
        """Test group_size validation."""
        # Valid group_size
        descriptor = TransformationDataManager(
            metadata_class="User", group_size={"files": 10}
        )
        assert descriptor.group_size == {"files": 10}

        # Test with no group_size
        descriptor2 = TransformationDataManager(metadata_class="User")
        assert descriptor2.group_size is None
