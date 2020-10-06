"""This module extracts features in pandas DataFrame format for creating graph structure charts."""

import networkx as nx
import pandas as pd
from scipy import stats
import numpy as np


class GraphFeatureExtraction:
    """
        A class to extract features from a networkx graph for A->B analysis.

        ...

        Methods:
            get_cycle_stats()
                Finds all cycles in a graph and returns stats and dataframe with node count in graph.

            remove_outliers(df, distributions_max=4)
                Removes outliers from distribution that are more than n standard deviations away from the mean.

            node_distribution_stats()
                Generates dataframes with paths from A->B for a line chart.
    """
    def __init__(self, G):
        self.G = G
        self.node_types = ["dataset", "system", "system_collection", "dataset_collection",
                           "data_integrity", "processing", "collection"]
        cycle_stats = self.get_cycle_stats()
        self.number_of_cycles = cycle_stats["number_of_cycles"]
        self.number_unique_nodes_in_cycles = cycle_stats["number_unique_nodes_in_cycles"]
        self.df_cycle_count = cycle_stats["df_cycle_count"]

        self.df_without_outliers, self.collection_outliers = self.node_distribution_stats()

    def get_cycle_stats(self):
        """Finds all cycles in a graph and returns stats and dataframe with node count in graph."""
        cycles = list(nx.simple_cycles(self.G))
        number_of_cycles = len(cycles)
        length_of_cycles = [len(cycle) for cycle in cycles]
        nodes_in_cycles = {node_type: set() for node_type in self.node_types}

        for cycle in cycles:
            for node in cycle:
                nodes_in_cycles[node.split("_")[0]].add(node)

        count_of_nodes_in_cycles = {k: len(nodes_in_cycles[k]) for k in nodes_in_cycles if k != "processing"}
        df_cycle_count = pd.Series(count_of_nodes_in_cycles).to_frame().reset_index()
        df_cycle_count.columns = ["node_type", "count"]
        number_unique_nodes_in_cycles = sum(df_cycle_count["count"])

        return {"number_of_cycles": number_of_cycles,
                "df_cycle_count": df_cycle_count,
                "number_unique_nodes_in_cycles": number_unique_nodes_in_cycles}

    @staticmethod
    def remove_outliers(df, standard_dev_max=4):
        """Removes outliers from distribution that are more than n standard deviations away from the mean."""
        z_scores = stats.zscore(df)
        abs_z_scores = np.abs(z_scores)
        filtered_entries = (abs_z_scores < standard_dev_max).all(axis=1)
        filtered_df = df[filtered_entries]
        outlier_entries = (abs_z_scores >= standard_dev_max).all(axis=1)
        outliers_df = df[outlier_entries]
        return filtered_df, outliers_df

    def node_distribution_stats(self):
        """
        Extracts features for number of elements in collection.
        Creates a separate dataframe for:
             - datasets in dataset collection
             - systems in system collection
             - dataset collections in collection
             - system colllections in collection
        Removes outliers. Returns list of filtered pandas dataframes, list of outlier dataframes.
        """
        nodes = {node_type: [node for node, data in self.G.nodes(data=True) if data["type"] == node_type]
                 for node_type in self.node_types}

        datasets_in_collection = [len([d for d in self.G.neighbors(dc) if d.startswith("dataset")]) for dc in nodes["dataset_collection"]]
        systems_in_collection = [len([d for d in self.G.neighbors(sc) if d.startswith("system")]) for sc in nodes["system_collection"]]
        dataset_collections_in_collection = [len([dc for dc in self.G.neighbors(c) if dc.startswith("dataset_collection")]) for c in nodes["collection"]]
        system_collections_in_collection = [len([sc for sc in self.G.neighbors(c) if sc.startswith("system_collection")]) for c in nodes["collection"]]

        datasets_in_collection_df = pd.DataFrame(datasets_in_collection, columns=["datasets_in_collection"])
        systems_in_collection_df = pd.DataFrame(systems_in_collection, columns=["systems_in_collection"])
        dataset_collections_in_collection_df = pd.DataFrame(dataset_collections_in_collection,
                                                            columns=["dataset_collections_in_collection"])
        system_collections_in_collection_df = pd.DataFrame(system_collections_in_collection,
                                                           columns=["system_collections_in_collection"])

        collection_dataframes = {"datasets_in_collection": datasets_in_collection_df,
                                 "systems_in_collection": systems_in_collection_df,
                                 "dataset_collections_in_collection": dataset_collections_in_collection_df,
                                 "system_collections_in_collection": system_collections_in_collection_df}
        df_without_outliers = []
        outliers = []
        for df_name in collection_dataframes:
            filtered_df, outliers_df = self.remove_outliers(collection_dataframes[df_name])
            if not outliers_df.empty:
              outliers.append((df_name, outliers_df))
            df_without_outliers.append(filtered_df)

        return df_without_outliers, outliers
