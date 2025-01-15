"""dsc.workflows.

All primary functions used by CLI are importable from here.
"""

from dsc.workflows.base import Workflow, WorkflowEvents
from dsc.workflows.base.simple_csv import SimpleCSV
from dsc.workflows.demo import Demo
from dsc.workflows.opencourseware import OpenCourseWare
from dsc.workflows.sccs import SCCS

__all__ = ["SCCS", "Demo", "OpenCourseWare", "SimpleCSV", "Workflow", "WorkflowEvents"]
