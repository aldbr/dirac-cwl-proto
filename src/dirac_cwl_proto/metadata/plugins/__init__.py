"""Plugin package initialization.

This module ensures that core plugins are automatically registered
when the metadata system is imported.
"""

from .core import AdminMetadata, QueryBasedMetadata, TaskWithMetadataQueryPlugin, UserMetadata
from .gaussian import DataGenerationMetadata, GaussianFitMetadata
from .lhcb import (
    LHCbAnalysisMetadata,
    LHCbReconstructionMetadata,
    LHCbSimulationMetadata,
)
from .mandelbrot import MandelBrotGenerationMetadata, MandelBrotMergingMetadata
from .pi import PiGatherMetadata, PiSimulateMetadata, PiSimulateV2Metadata

# Plugins will be auto-registered through the metaclass or discovery system

__all__ = [
    # Core plugins
    "AdminMetadata",
    "QueryBasedMetadata",
    "UserMetadata",
    "TaskWithMetadataQueryPlugin",
    # PI simulation plugins
    "PiSimulateMetadata",
    "PiSimulateV2Metadata",
    "PiGatherMetadata",
    # Mandelbrot plugins
    "MandelBrotGenerationMetadata",
    "MandelBrotMergingMetadata",
    # Gaussian fit plugins
    "DataGenerationMetadata",
    "GaussianFitMetadata",
    # LHCb plugins
    "LHCbSimulationMetadata",
    "LHCbReconstructionMetadata",
    "LHCbAnalysisMetadata",
]
