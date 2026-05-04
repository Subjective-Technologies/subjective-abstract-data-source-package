"""
Subjective Abstract Data Source Package

This package provides abstract base classes for building data source addons
for BrainBoost data pipelines software.

Classes:
    SubjectiveDataSource: Abstract base class for standard data sources
    SubjectiveRealTimeDataSource: Abstract base class for real-time data sources
    SubjectiveOnDemandDataSource: Abstract base class for request/response data sources
"""

from .SubjectiveDataSource import SubjectiveDataSource
from .SubjectiveRealTimeDataSource import SubjectiveRealTimeDataSource
from .SubjectiveOnDemandDataSource import SubjectiveOnDemandDataSource
from .SubjectivePipelineDataSource import SubjectiveDataSourcePipeline, PipelineNode, PipelineAdapter
from .SubjectivePipelineDataSource import SubjectivePipelineDataSource

__version__ = "2.0.0"
__author__ = "Pablo Tomas Borda"
__email__ = "pablo.borda@subjectivetechnologies.com"

__all__ = [
    "SubjectiveDataSource",
    "SubjectiveRealTimeDataSource",
    "SubjectiveOnDemandDataSource",
    "SubjectiveDataSourcePipeline",
    "PipelineNode",
    "PipelineAdapter",
    "SubjectivePipelineDataSource"
] 
