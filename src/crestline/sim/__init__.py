"""The SimPy plant model: job generation, shift calendar, machines, the
answer key it records, and the backfill that writes it to disk."""

from .config import PlantConfig
from .jobs import GeneratedJobs, Job, SalesOrder, SalesOrderLine, generate_jobs
from .plant import Plant, simulate
from .records import SimulationResult
from .scenarios import Scenarios, SetupMultiplier, load_scenarios
from .truth import build_truth_tables, write_truth

__all__ = [
    "GeneratedJobs",
    "Job",
    "Plant",
    "PlantConfig",
    "SalesOrder",
    "SalesOrderLine",
    "Scenarios",
    "SetupMultiplier",
    "SimulationResult",
    "build_truth_tables",
    "generate_jobs",
    "load_scenarios",
    "simulate",
    "write_truth",
]
