"""
This module implements methods for creating data dependency mapping graph using networkx library.

It allows to create nodes of type:
    collection
    dataset collection
    system collection
    dataset
    system
    environment
    dataset processing
    data integrity

While creating the nodes, the following two way connections will be created:
    dataset collection - collection
    system collection - collection
    dataset - dataset collection
    system - system collection
    system - dataset processing
    dataset processing - dataset
    dataset integrity - dataset

To optimize generation, vertices and edges are not added iteratively, but all at the same time.
The networkx itself will be generated in the method save_to_file.
"""

import networkx as nx
import os
import logging


class NxGraph:
    """
    A class to represent data dependency graph in networkx format.

    ...

    Methods:
        generate_collection(collection_id)
            Generates a collection with the given id.

        generate_dataset_collection(dataset_collection_id, collection_id)
            Generates a dataset collection with a given id.

        generate_system_collection(system_collection_id, collection_id)
            Generates a system collection with a given id.

        generate_dataset(dataset_id, dataset_collection_id, slo)
            Generates a dataset with a given id.

        generate_system(system_id, system_collection_id, system_critic)
            Generates a system with a given id.

        generate_processing(system_id, dataset_id, processing_id, impact, freshness, action="INPUTS")
            Generates a processing node, that represents dataset - system relationship.
            Action parameter denotes if the dataset is an input to the system, or an output.

        generate_data_integrity(data_integrity_id, dataset_id, data_integrity_rec_time, data_integrity_volat,
                                data_integrity_reg_time, data_integrity_rest_time)
            Generates a data integrity node, that corresponds to a specific dataset, having the attributes.

        save_to_file(filename, overwrite=False)
            Loads graph to networkx directed graph (DiGraph) object and saves generated graph message to .net binary.

        read_from_file(filename, overwrite=False)
            Reads graph message from .net binary.
    """
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes = []
        self.edge_types = {"dataset_collection_to_collection": "CONTAINS",
                           "system_collection_to_collection": "CONTAINS",
                           "dataset_to_dataset_collection": "CONTAINS",
                           "system_to_system_collection": "CONTAINS",
                           "dataset_to_system_input": "INPUTS",
                           "dataset_to_system_output": "OUTPUTS",
                           "data_integrity_to_dataset_collection": "HAS"}
        self.edges = {edge: [] for edge in self.edge_types}

    def generate_collection(self, collection_id, description):
        """Generates collection node."""
        node_attributes = {"id": collection_id,
                           "description": description,
                           "type": "collection"}
        node = (f"collection_{collection_id}", node_attributes)
        self.nodes.append(node)
        logging.info(f"NxGraph. Added collection {collection_id}.")

    def generate_dataset_collection(self, dataset_collection_id, collection_id, description):
        """Generates dataset collection node and dataset collection - collection edge."""
        node_attributes = {"id": dataset_collection_id,
                           "collection_id": collection_id,
                           "description": description,
                           "type": "dataset_collection"}
        node = (f"dataset_collection_{dataset_collection_id}", node_attributes)
        self.nodes.append(node)
        self.edges["dataset_collection_to_collection"].append((f"collection_{collection_id}",
                                                               f"dataset_collection_{dataset_collection_id}"))
        logging.info(f"NxGraph. Added dataset collection {dataset_collection_id}.")

    def generate_system_collection(self, system_collection_id, collection_id, description):
        """Generates system collection node and system collection - collection edge."""
        node_attributes = {"id": system_collection_id,
                           "collection_id": collection_id,
                           "description": description,
                           "type": "system_collection"}
        node = (f"system_collection_{system_collection_id}", node_attributes)
        self.nodes.append(node)
        self.edges["system_collection_to_collection"].append((f"collection_{collection_id}",
                                                              f"system_collection_{system_collection_id}"))
        logging.info(f"NxGraph. Added system collection {system_collection_id}.")

    def generate_dataset(self, dataset_id, dataset_collection_id, regex_grouping, name, slo, env, description):
        """Generates dataset node and dataset - dataset collection edge."""
        node_attributes = {"id": dataset_id,
                           "dataset_collection_id": dataset_collection_id,
                           "regex_grouping": regex_grouping,
                           "node_name": name,
                           "description": description,
                           "slo": slo,
                           "env": env,
                           "type": "dataset"}
        node = (f"dataset_{dataset_id}", node_attributes)
        self.nodes.append(node)
        self.edges["dataset_to_dataset_collection"].append((f"dataset_collection_{dataset_collection_id}",
                                                            f"dataset_{dataset_id}"))
        logging.info(f"NxGraph. Added dataset {dataset_id}.")

    def generate_system(self, system_id, system_critic, system_collection_id, regex_grouping, name, env, description):
        """Generates system node and system - system collection edge."""
        node_attributes = {"id": system_id,
                           "system_collection_id": system_collection_id,
                           "regex_grouping": regex_grouping,
                           "node_name": name,
                           "description": description,
                           "system_critic": system_critic,
                           "env": env,
                           "type": "system"}

        node = (f"system_{system_id}", node_attributes)
        self.nodes.append(node)
        self.edges["system_to_system_collection"].append((f"system_collection_{system_collection_id}",
                                                          f"system_{system_id}"))
        logging.info(f"NxGraph. Added system {system_id}.")

    def generate_processing(self, system_id, dataset_id, processing_id, impact, freshness, inputs=True):
        """Generates processing node and processing - dataset, processing - system edges."""
        node_attributes = {"id": processing_id,
                           "impact": impact,
                           "freshness": freshness,
                           "type": "processing"}
        node = (f"processing_{processing_id}", node_attributes)
        self.nodes.append(node)

        if inputs:
            self.edges["dataset_to_system_input"].append((f"dataset_{dataset_id}", f"processing_{processing_id}"))
            self.edges["dataset_to_system_input"].append((f"processing_{processing_id}", f"system_{system_id}"))

        else:
            self.edges["dataset_to_system_output"].append((f"processing_{processing_id}", f"dataset_{dataset_id}"))
            self.edges["dataset_to_system_output"].append((f"system_{system_id}", f"processing_{processing_id}"))
        logging.info(f"NxGraph. Added processing {processing_id}.")

    def generate_data_integrity(self, data_integrity_id, dataset_collection_id, data_integrity_rec_time,
                                data_integrity_volat, data_integrity_reg_time, data_integrity_rest_time):
        """Generates data integrity node, and dataset collection - data integrity edge."""
        node_attributes = {"id": data_integrity_id,
                           "data_integrity_rec_time": data_integrity_rec_time,
                           "data_integrity_rest_time": data_integrity_rest_time,
                           "data_integrity_reg_time": data_integrity_reg_time,
                           "data_integrity_volat": int(data_integrity_volat),
                           "type": "data_integrity"}
        node = (f"data_integrity_{data_integrity_id}", node_attributes)
        self.nodes.append(node)
        self.edges["data_integrity_to_dataset_collection"].append((f"dataset_collection_{dataset_collection_id}",
                                                                   f"data_integrity_{data_integrity_id}"))
        logging.info(f"NxGraph. Added data integrity {data_integrity_id}.")

    def save_to_file(self, filename, overwrite=False):
        """Saves generated graph to .net file.

        Raises:
            ValueError: Graph database with this file already exists.
        """
        self.graph.add_nodes_from(self.nodes)
        for edge_type in self.edges:
            self.graph.add_edges_from(self.edges[edge_type], label=self.edge_types[edge_type])

        if os.path.isfile(filename) and overwrite:
            os.remove(filename)
        elif os.path.isfile(filename):
            raise ValueError("Graph database with this file already exists.")
        nx.write_graphml(self.graph, filename)
        logging.info(f"NxGraph saved to {filename}.")

    def read_from_file(self, filename, overwrite=False):
        """Loads networkx from a binary .net file.

        Raises:
            ValueError: Graph attribute is not empty.
        """
        if len(self.graph) == 0 or overwrite:
            self.graph = nx.read_graphml(filename)
        else:
            raise ValueError("Graph is not empty. Use overwrite arg if this is intended.")
        logging.info(f"NxGraph loaded from {filename}.")
