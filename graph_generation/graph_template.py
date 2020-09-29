"""
This module implements factory for creating a graph.
Current version supports proto and networkx graphs.
"""

from proto_graph import ProtoGraph
from nx_graph import NxGraph


class GraphTemplate:
    """
   A class to get instance of a selected class for graph generation.

   ...

   Methods:
       get_proto_graph()
           Return instance of a proto graph.

       get_nx_graph()
           Return instance of a networkx graph.
   """
    @staticmethod
    def get_proto_graph():
        """Return instance of a proto graph."""
        return ProtoGraph()

    @staticmethod
    def get_nx_graph():
        """Return instance of a networkx graph."""
        return NxGraph()
