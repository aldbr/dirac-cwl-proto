"""Plugin package initialization.

This module ensures that core plugins are automatically registered
when the metadata system is imported.
"""

from .core import (
    AdminPlugin,
    QueryBasedPlugin,
    TaskWithMetadataQueryPlugin,
    UserPlugin,
)

# from .gaussian import DataGenerationPlugin, GaussianFitPlugin
# from .lhcb import (
#     LHCbAnalysisPlugin,
#     LHCbReconstructionPlugin,
#     LHCbSimulationPlugin,
# )
# from .mandelbrot import MandelBrotGenerationPlugin, MandelBrotMergingPlugin
# from .pi import PiGatherPlugin, PiSimulatePlugin, PiSimulateV2Plugin

# Plugins will be auto-registered through the metaclass or discovery system

__all__ = [
    # Core plugins
    "AdminPlugin",
    "QueryBasedPlugin",
    "UserPlugin",
    "TaskWithMetadataQueryPlugin",
    # # PI simulation plugins
    # "PiSimulatePlugin",
    # "PiSimulateV2Plugin",
    # "PiGatherPlugin",
    # # Mandelbrot plugins
    # "MandelBrotGenerationPlugin",
    # "MandelBrotMergingPlugin",
    # # Gaussian fit plugins
    # "DataGenerationPlugin",
    # "GaussianFitPlugin",
    # # LHCb plugins
    # "LHCbSimulationPlugin",
    # "LHCbReconstructionPlugin",
    # "LHCbAnalysisPlugin",
]
