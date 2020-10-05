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
#     app.config.update(dict(DEBUG=True))
#     app.config.update(config or {})

    # Setup cors headers to allow all domains
#     CORS(app)

    @app.route("/")
    def index():
        return render_template('a_b.html')

    @app.route("/a_b/paths")
    def data_paths_a_b():
        chart = charts.get_graph_chart()
        return chart.to_json()

    @app.route("/a_b/slo")
    def data_slo_a_b():
        chart = charts.get_slo_chart()
        return chart.to_json()

    @app.route("/a_b/data_integrity")
    def data_integrity_a_b():
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
