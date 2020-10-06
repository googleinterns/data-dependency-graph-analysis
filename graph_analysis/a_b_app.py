"""
This module creates a flask app, that renders charts for A->B analysis.

Usage:
    python3 graph_analysis/a_b_app.py \
         --dataset_A 1069 \
         --dataset_B 8770 \
         --graph_path "graph_1_10.graphml"

    Parameters info:
        dataset_A - id of an upstream dataset
        dataset_B - id of a downstream dataset
        graph_path - path to a graph in graphml format
"""


from flask import Flask, render_template
import argparse
import networkx as nx

from a_b_charts import ABCharts
from a_b_feature_extraction import ABFeatureExtraction


def parse_args():
    """Parses input arguments."""
    parser = argparse.ArgumentParser(description='Generate graph based on config.')
    parser.add_argument('-a', '--dataset_A', type=int, help='Id of a first dataset.', required=True)
    parser.add_argument('-b', '--dataset_B', type=int, help='Id of a second dataset.', required=True)
    parser.add_argument('-g', '--graph_path', help='Path to a graphml file.', required=True)
    args = parser.parse_args()
    return args


def create_app(charts, config=None):
    app = Flask(__name__)

    @app.route("/")
    def index():
        """Renders template."""
        return render_template('a_b.html')

    @app.route("/a_b/paths")
    def data_paths_a_b():
        """Gets json format of a graph chart."""
        chart = charts.get_graph_chart()
        return chart.to_json()

    @app.route("/a_b/slo")
    def data_slo_a_b():
        """Gets json format of a slo chart."""
        chart = charts.get_slo_chart()
        return chart.to_json()

    @app.route("/a_b/data_integrity")
    def data_integrity_a_b():
        """Gets json format of a data integrity time chart."""
        chart = charts.get_data_integrity_chart()
        return chart.to_json()
    return app


if __name__ == "__main__":
    args = parse_args()
    B = args.dataset_B
    A = args.dataset_A
    graph_path = args.graph_path

    G = nx.read_graphml(graph_path)
    features = ABFeatureExtraction(A, B, G)
    charts = ABCharts(features)

    app = create_app(charts)
    app.run()
