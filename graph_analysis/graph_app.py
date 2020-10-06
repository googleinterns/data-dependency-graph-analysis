"""
This module creates a flask app, that renders charts for graph structure.

Usage:
    python3 graph_analysis/graph_app.py \
         --graph_path "graph_1_10.graphml"

    Parameters info:
        graph_path - path to a graph in graphml format
"""


from flask import Flask, render_template
import argparse
import networkx as nx

from graph_feature_extraction import GraphFeatureExtraction
from graph_charts import GraphCharts


def parse_args():
    """Parses input arguments."""
    parser = argparse.ArgumentParser(description='Generate graph based on config.')
    parser.add_argument('-g', '--graph_path', help='Path to a graphml file.', required=True)
    args = parser.parse_args()
    return args


def create_app(charts, config=None):
    app = Flask(__name__)

    @app.route("/")
    def index():
        """Renders template."""
        return render_template('graph.html')

    @app.route("/graph/cycles")
    def data_cycles():
        """Gets json format of a graph chart."""
        chart = charts.get_cycle_chart()
        return chart.to_json()

    @app.route("/graph/graph_structure")
    def data_graph_structure():
        """Gets json format of a graph structure charts."""
        chart = charts.get_graph_structure_charts()
        return chart.to_json()
    return app


if __name__ == "__main__":
    args = parse_args()
    graph_path = args.graph_path

    G = nx.read_graphml(graph_path)
    features = GraphFeatureExtraction(G)
    charts = GraphCharts(features)

    app = create_app(charts)
    app.run()
