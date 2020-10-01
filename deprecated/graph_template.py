"""
This module implements factory for creating a graph.
Current version supports neo4j, igraph, proto and networkx graphs.
"""

from neo4jgraph import Neo4jGraph
from i_graph import IGraph
from proto_graph import ProtoGraph
from nx_graph import NxGraph


class GraphTemplate:
    """
    A class to get instance of a selected class for graph generation.

    ...

    Methods:
        get_neo4j_graph()
            Return instance of a neo4j graph.

        get_i_graph()
            Return instance of igraph.

        get_proto_graph()
            Return instance of a proto graph.

        get_nx_graph()
            Return instance of a networkx graph.
    """
    @staticmethod
    def get_neo4j_graph(uri, user, password):
        """Return instance of a neo4j graph."""
        return Neo4jGraph(uri, user, password)

    @staticmethod
    def get_i_graph():
        """Return instance of igraph."""
        return IGraph()

    @staticmethod
    def get_proto_graph():
        """Return instance of a proto graph."""
        return ProtoGraph()

    @staticmethod
    def get_nx_graph():
        """Return instance of a networkx graph."""
        return NxGraph()
