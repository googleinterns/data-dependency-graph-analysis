"""This module extracts features in pandas DataFrame format for creating charts."""

import pandas as pd
import networkx as nx


class ABFeatureExtraction:
    """
        A class to extract features from a networkx graph for A->B analysis.

        ...

        Methods:
            find_breakout_intervals(base_path, forked_path)
                Generates intervals for forked path coordinate calculation.

            generate_coordinates(paths, range_x=(10, 890), range_y=(10, 290))
                Generates coordinates for paths and nodes placement in a space.

            get_paths_df()
                Generates dataframes with paths from A->B for a line chart.

            get_point_df()
                Generates dataframe with points for node scatter plot.

            process_time(t)
                Converts time string to seconds.

            get_slo_df(curr_start=50)
                Returns dataframe with slo features.

            get_data_integrity_df()
                Returns dataframe with data integrity features.
    """
    def __init__(self, A, B, G, cutoff=50, n=10):
        self.G = G
        paths = list(nx.all_simple_paths(G, source=f"dataset_{A}", target=f"dataset_{B}", cutoff=cutoff))
        self.shortest_path = nx.shortest_path(G, source=f"dataset_{A}", target=f"dataset_{B}")
        sort_by_length = sorted(paths, key=lambda x: len(x))
        self.top_n_paths = [[node for node in path if not node.startswith("processing")]
                             for path in sort_by_length[:n]]
        self.nodes_coordinates = self.generate_coordinates(self.top_n_paths)
        self.paths_df = self.get_paths_df()
        self.point_df = self.get_point_df()
        self.slo_df = self.get_slo_df()
        self.data_integrity_df = self.get_data_integrity_df()

    @staticmethod
    def find_breakout_intervals(base_path, forked_path):
        """
        For two paths between two nodes finds interval where their nodes don't intersect.
        The base path is main, and always shorter or equal than the forked path.
        The aim is to find fork intervals for forked path, and between which nodes in main path they are located.

        Example:

          Base Path: "a" -> "b"     -> "d"     -> "f"         -> "g"
                                \             /     \       /
          Forked Path:           -> "c" -> "e"        -> "k"

          Base Path:    ["a", "b",    "d",   "f",      "g"]
          Forked Path:  ["a", "b", "c", "e", "f", "k", "g"]

          Nodes in forked path that are not in a base paths are: c, e and k.
          Their intervals that break down from main path are (2,3) -> (c,e) and (5,5) -> (k,k)
          In base path coordinate system they are located between next nodes: (1,3) -> (b,f) and (3,4) -> (f,g)

          Returns:
              [[1, 3], [3, 4]], [[2, 3], [5, 5]]
        """
        node_ids = [i for i in range(len(forked_path)) if forked_path[i] not in base_path]
        forked_path_break_ids = []
        last_saved = 0
        for i in range(1, len(node_ids)):
            if node_ids[i] - node_ids[last_saved] > i - last_saved:
                forked_path_break_ids.append((node_ids[last_saved], node_ids[i - 1]))
                last_saved = i
        forked_path_break_ids.append([node_ids[last_saved], node_ids[-1]])

        base_path_break_ids = [[base_path.index(forked_path[i[0] - 1]), base_path.index(forked_path[i[1] + 1])]
                                for i in forked_path_break_ids]
        return base_path_break_ids, forked_path_break_ids

    def generate_coordinates(self, paths, range_x=(10, 890), range_y=(10, 290)):
        """Generates coordinates for paths and nodes placement in a space."""
        min_x, max_x = range_x
        min_y, max_y = range_y

        alpha = 0.00001
        step_y = (max_y - min_y) / (len(paths) - 1)
        path_ids = [(-1) ** i * i // 2 for i in range(len(paths))]
        central_y = (len(paths) / 2) * step_y
        paths_coords = {}

        for i, base_node in enumerate(paths[0]):
            node_x = i * (max_x - min_x) / (len(paths[0]) - 1)
            paths_coords[base_node] = [node_x, central_y]

        for path_id, path in enumerate(paths[1:]):
            node_y = central_y + path_ids[path_id + 1] * step_y
            base_path_break_ids, new_path_break_ids = ABFeatureExtraction.find_breakout_intervals(paths[0], path)
            for i in range(len(base_path_break_ids)):
                base_start, base_end = base_path_break_ids[i]
                node_start, node_end = new_path_break_ids[i]
                x_start, x_end = paths_coords[paths[0][base_start]][0], paths_coords[paths[0][base_end]][0]
                x_step = (x_end - x_start + alpha) / (node_end - node_start + alpha)

                for j in range(node_end - node_start + 1):
                    node_x = x_start + j * x_step
                    node_name = path[node_start + j]
                    if node_name not in paths_coords:
                        paths_coords[node_name] = [node_x, node_y]

        return paths_coords

    def get_paths_df(self):
        """Generates dataframes with paths from A->B for a chart."""
        paths_df = []
        for path in self.top_n_paths:
            path_nodes = []
            for node in path:
                path_nodes.append([node, self.nodes_coordinates[node][0], self.nodes_coordinates[node][1]])
            paths_df.append(pd.DataFrame(path_nodes, columns = ["node", "x", "y"]))
        return paths_df

    def get_point_df(self):
        """Generates dataframe with points for node scatter plot."""
        point_df = pd.DataFrame.from_dict(self.nodes_coordinates, orient='index', columns = ["x", "y"])
        # Get path id for each node. If node is in multiple paths assign id of the shortest path.
        point_path = {}
        for path_id, path in enumerate(self.top_n_paths[::-1]):
            for point in path:
                point_path[point] = len(self.top_n_paths) - 1 - path_id
        point_df["path"] = pd.Series(point_path)
        point_df = point_df.reset_index()
        point_df["type"] = point_df["index"].apply(lambda x: x.split("_")[0])
        return point_df

    @staticmethod
    def process_time(t):
        """Converts time string to seconds."""
        time_to_seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return int(t[:-1]) * time_to_seconds[t[-1]]

    def get_slo_df(self, curr_start=50):
        """Returns dataframe with slo features."""
        slos = []
        for dataset in self.shortest_path[::4]:
            slos.append(self.process_time(self.G.nodes()[dataset]['slo']))

        slo_ranges = []

        for i in range(len(slos)):
            slo_ranges.append([self.shortest_path[i * 4], curr_start])
            slo_ranges.append([self.shortest_path[i * 4], slos[i] + curr_start])
            curr_start += slos[i]

        slo_df = pd.DataFrame(slo_ranges, columns=["dataset_id", "slo"])
        return slo_df

    def get_data_integrity_df(self):
        """Returns dataframe with data integrity features."""
        dataset_collections = [f"dataset_collection_{self.G.nodes()[d]['dataset_collection_id']}" for d in self.shortest_path[::4]]
        collection_neighbours = [nx.neighbors(self.G, collection) for collection in dataset_collections]
        data_integrity = [i for neighbours in collection_neighbours for i in neighbours if i.startswith("data_integrity")]
        reconstruction_time_string = [self.G.nodes()[di]["data_integrity_rec_time"] for di in data_integrity]
        regeneration_time_string = [self.G.nodes()[di]["data_integrity_reg_time"] for di in data_integrity]
        restoration_time_string = [self.G.nodes()[di]["data_integrity_rest_time"] for di in data_integrity]
        reconstruction_time = [self.process_time(t) for t in reconstruction_time_string]
        regeneration_time = [self.process_time(t) for t in regeneration_time_string]
        restoration_time = [self.process_time(t) for t in restoration_time_string]
        is_volatile = [self.G.nodes()[di]["data_integrity_volat"] for di in data_integrity]

        data_integrity_df = pd.DataFrame({
            "dataset_id": self.shortest_path[::4],
            "dataset_collection_id": dataset_collections,
            "reconstruction_time_duration": reconstruction_time,
            "regeneration_time_duration": regeneration_time,
            "restoration_time_duration": restoration_time,
            "reconstruction_time_string": reconstruction_time_string,
            "regeneration_time_string": regeneration_time_string,
            "restoration_time_string": restoration_time_string,
            "volatility": is_volatile
        })

        return data_integrity_df
