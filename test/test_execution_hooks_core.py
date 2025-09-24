"""
Tests for the execution hooks core classes.

This module tests the foundational classes and interfaces of the execution hooks
plugin system, including ExecutionHooksBasePlugin, ExecutionHooksHint, and core
abstract interfaces.
"""

from pathlib import Path
from typing import Any, List, Optional, Union

import pytest

from dirac_cwl_proto.execution_hooks.core import (
    DataCatalogInterface,
    ExecutionHooksBasePlugin,
    ExecutionHooksHint,
    SchedulingHint,
    TransformationExecutionHooksHint,
)


class TestExecutionHook:
    """Test the ExecutionHooksBasePlugin abstract base class."""

    def test_instantiation(self):
        """Test that ExecutionHooksBasePlugin can be instantiated directly with default behavior."""
        hook = ExecutionHooksBasePlugin()

        # Test default pre_process behavior
        command = ["echo", "hello"]
        result = hook.pre_process(Path("/tmp"), command)
        assert result == command  # Should return command unchanged

        # Test default post_process behavior
        hook.post_process(Path("/tmp"), exit_code=0)  # Should not raise any exception

    def test_concrete_implementation(self):
        """Test that concrete implementations work correctly."""

        class ConcreteHook(ExecutionHooksBasePlugin):
            def pre_process(
                self, job_path: Path, command: List[str], **kwargs: Any
            ) -> List[str]:
                return command + ["--processed"]

        processor = ConcreteHook()

        # Test pre_process
        result = processor.pre_process(Path("/tmp"), ["echo", "hello"])
        assert result == ["echo", "hello", "--processed"]

        # Test post_process
        processor.post_process(Path("/tmp"))  # Should not raise exception


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

            def get_output_query(
                self, output_name: str, **kwargs: Any
            ) -> Optional[Path]:
                return Path(f"/output/{output_name}")

            def store_output(
                self, output_name: str, src_path: str, **kwargs: Any
            ) -> None:
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


class TestExecutionHookExtended:
    """Test the ExecutionHooksBasePlugin foundation class methods."""

    def test_creation(self):
        """Test ExecutionHooksBasePlugin can be instantiated with concrete implementations."""

        class TestModel(ExecutionHooksBasePlugin):
            test_field: str = "default"

        model = TestModel()
        assert model.test_field == "default"

        model = TestModel(test_field="custom")
        assert model.test_field == "custom"

    def test_pydantic_validation(self):
        """Test that Pydantic validation works correctly."""

        class TestModel(ExecutionHooksBasePlugin):
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

        class TestModel(ExecutionHooksBasePlugin):
            pass

        model = TestModel()

        # Test DataCatalogInterface methods
        assert model.get_input_query("test") is None
        assert model.get_output_query("test") is None

        # Test store_output raises RuntimeError when no output path is defined
        with pytest.raises(RuntimeError, match="No output path defined"):
            model.data_catalog.store_output("test", "/tmp/file.txt")

    def test_model_serialization(self):
        """Test that model serialization works correctly."""

        class TestModel(ExecutionHooksBasePlugin):
            field: str = ""
            value: int = 42

        model = TestModel(field="test")

        # Test dict conversion
        data = model.model_dump()
        assert data == {"field": "test", "value": 42}

        # Test JSON schema generation
        schema = model.model_json_schema()
        assert "properties" in schema
        assert "field" in schema["properties"]
        assert "value" in schema["properties"]


class TestExecutionHooksHint:
    """Test the ExecutionHooksHint class."""

    def test_creation(self):
        """Test ExecutionHooksHint creation."""
        descriptor = ExecutionHooksHint(hook_plugin="User")
        assert descriptor.hook_plugin == "User"

    def test_from_cwl(self, mocker):
        """Test extraction from CWL hints."""
        # Mock CWL document
        mock_cwl = mocker.Mock()
        mock_cwl.hints = [
            {
                "class": "dirac:execution-hooks",
                "hook_plugin": "QueryBased",
                "campaign": "Run3",
            },
            {"class": "ResourceRequirement", "coresMin": 2},
        ]

        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)

        assert descriptor.hook_plugin == "QueryBased"
        assert descriptor.campaign == "Run3"

    def test_from_cwl_no_hints(self, mocker):
        """Test extraction when no hints are present."""
        mock_cwl = mocker.Mock()
        mock_cwl.hints = None

        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)

        # Should create default descriptor
        assert descriptor.hook_plugin == "UserPlugin"

    def test_from_cwl_no_dirac_hints(self, mocker):
        """Test extraction when no DIRAC hints are present."""
        mock_cwl = mocker.Mock()
        mock_cwl.hints = [{"class": "ResourceRequirement", "coresMin": 2}]

        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)

        # Should create default descriptor
        assert descriptor.hook_plugin == "UserPlugin"

    def test_model_copy_merges_dict_fields(self):
        """Test model_copy merges dict fields and updates values."""
        descriptor = ExecutionHooksHint(hook_plugin="LHCbSimulationPlugin")

        updated = descriptor.model_copy(
            update={"hook_plugin": "NewClass", "new_field": "value"}
        )

        assert updated.hook_plugin == "NewClass"
        assert getattr(updated, "new_field", None) == "value"

    def test_default_values(self):
        """Test default values."""
        descriptor = ExecutionHooksHint(hook_plugin="UserPlugin", user_id="test123")

        assert descriptor.hook_plugin == "UserPlugin"
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
                "class": "dirac:scheduling",
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


class TestTransformationExecutionHooksHint:
    """Test the TransformationExecutionHooksHint class."""

    def test_creation(self):
        """Test TransformationExecutionHooksHint creation."""
        descriptor = TransformationExecutionHooksHint(
            hook_plugin="QueryBasedPlugin", group_size={"input_data": 100}
        )
        assert descriptor.hook_plugin == "QueryBasedPlugin"
        assert descriptor.group_size == {"input_data": 100}

    def test_inheritance(self):
        """Test that it inherits from ExecutionHooksHint."""
        descriptor = TransformationExecutionHooksHint(
            hook_plugin="LHCbSimulationPlugin",
            group_size={"sim_data": 50},
            n_events=1000,
        )

        # Test that it has the fields from both classes
        assert descriptor.hook_plugin == "LHCbSimulationPlugin"
        assert descriptor.group_size == {"sim_data": 50}
        assert getattr(descriptor, "n_events", None) == 1000

    def test_validation(self):
        """Test group_size validation."""
        # Valid group_size
        descriptor = TransformationExecutionHooksHint(
            hook_plugin="UserPlugin", group_size={"files": 10}
        )
        assert descriptor.group_size == {"files": 10}

        # Test with no group_size
        descriptor2 = TransformationExecutionHooksHint(hook_plugin="UserPlugin")
        assert descriptor2.group_size is None
