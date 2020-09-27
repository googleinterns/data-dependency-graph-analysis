"""
This module implements methods for creating data dependency mapping graph using igraph library.

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
The igraph itself will be generated in the method save_to_file.
"""

from igraph import *
import logging


class IGraph:
    """
    A class to represent data dependency graph in igraph format.

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
            Loads graph to igraph object and saves generated graph message to .net binary.

        read_from_file(filename, overwrite=False)
            Reads graph message from .net binary.
    """
    def __init__(self):
        self.graph = Graph(directed=True)

        self.edges = []
        self.edge_type = []

        self.attribute_names = ["id", "description", "collection_id", "dataset_collection_id", "regex_grouping",
                                "node_name", "description", "slo", "env", "system_critic", "impact", "freshness",
                                "data_integrity_rec_time", "data_integrity_reg_time", "data_integrity_rest_time",
                                "data_integrity_volat"]
        self.vertex_attributes = {att: [] for att in self.attribute_names}
        self.vertices = []

    def generate_collection(self, collection_id):
        """Generates collection node."""
        self.vertices.append(f"collection_{collection_id}")
        collection_attributes = ["id", "description"]
        self.vertex_attributes["id"].append(collection_id)
        self.vertex_attributes["description"].append(f"Collection number {collection_id}.")
        for att in self.attribute_names:
            if att not in collection_attributes:
                self.vertex_attributes[att].append(None)
        logging.info(f"IGraph. Added collection {collection_id}.")

    def generate_dataset_collection(self, dataset_collection_id, collection_id):
        """Generates dataset collection node and dataset collection - collection edge."""
        self.vertices.append(f"dataset_collection_{dataset_collection_id}")

        dataset_collection_attributes = ["id", "description", "collection_id"]
        self.vertex_attributes["id"].append(dataset_collection_id)
        self.vertex_attributes["collection_id"].append(collection_id)
        self.vertex_attributes["description"].append(f"Dataset collection number {dataset_collection_id}.")

        # IGraph fills in missing properties with None.
        for att in self.attribute_names:
            if att not in dataset_collection_attributes:
                self.vertex_attributes[att].append(None)

        self.edges.append((f"collection_{collection_id}", f"dataset_collection_{dataset_collection_id}"))
        self.edge_type.append("CONTAINS")
        logging.info(f"IGraph. Added dataset collection {dataset_collection_id}.")

    def generate_system_collection(self, system_collection_id, collection_id):
        """Generates system collection node and system collection - collection edge."""
        self.vertices.append(f"system_collection_{system_collection_id}")

        system_collection_attributes = ["id", "description", "collection_id"]
        self.vertex_attributes["id"].append(system_collection_id)
        self.vertex_attributes["collection_id"].append(collection_id)
        self.vertex_attributes["description"].append(f"System collection number {system_collection_id}.")

        for att in self.attribute_names:
            if att not in system_collection_attributes:
                self.vertex_attributes[att].append(None)

        self.edges.append((f"collection_{collection_id}", f"system_collection_{system_collection_id}"))
        self.edge_type.append("CONTAINS")
        logging.info(f"IGraph. Added system collection {system_collection_id}.")

    def generate_dataset(self, dataset_id, dataset_collection_id, slo, env):
        """Generates dataset node and dataset - dataset collection edge."""
        self.vertices.append(f"dataset_{dataset_id}")

        dataset_attributes = ["id", "dataset_collection_id", "regex_grouping", "node_name", "description", "slo", "env"]
        self.vertex_attributes["id"].append(dataset_id)
        self.vertex_attributes["dataset_collection_id"].append(dataset_collection_id)
        self.vertex_attributes["regex_grouping"].append(f"data.{dataset_id}.*")
        self.vertex_attributes["node_name"].append(f"dataset.{dataset_id}")
        self.vertex_attributes["description"].append(f"Dataset number {dataset_id}")
        self.vertex_attributes["slo"].append(slo)
        self.vertex_attributes["env"].append(env)

        for att in self.attribute_names:
            if att not in dataset_attributes:
                self.vertex_attributes[att].append(None)

        self.edges.append((f"dataset_collection_{dataset_collection_id}", f"dataset_{dataset_id}"))
        self.edge_type.append("CONTAINS")
        logging.info(f"IGraph. Added dataset {dataset_id}.")

    def generate_system(self, system_id, system_critic, system_collection_id, env):
        """Generates system node and system - system collection edge."""
        self.vertices.append(f"system_{system_id}")

        system_attributes = ["id", "system_collection_id", "regex_grouping", "node_name", "description",
                             "system_critic", "env"]
        self.vertex_attributes["id"].append(system_id)
        self.vertex_attributes["dataset_collection_id"].append(system_collection_id)
        self.vertex_attributes["regex_grouping"].append(f"system.{system_id}.*")
        self.vertex_attributes["node_name"].append(f"system.{system_id}")
        self.vertex_attributes["description"].append(f"System number {system_id}")
        self.vertex_attributes["system_critic"].append(system_critic)
        self.vertex_attributes["env"].append(env)

        for att in self.attribute_names:
            if att not in system_attributes:
                self.vertex_attributes[att].append(None)

        self.edges.append((f"system_collection_{system_collection_id}", f"system_{system_id}"))
        self.edge_type.append("CONTAINS")
        logging.info(f"IGraph. Added system {system_id}.")

    def generate_processing(self, system_id, dataset_id, processing_id, impact, freshness, inputs=True):
        """Generates processing node and processing - dataset, processing - system edges."""
        self.vertices.append(f"processing_{processing_id}")

        processing_attributes = ["id", "impact", "freshness"]
        self.vertex_attributes["id"].append(processing_id)
        self.vertex_attributes["impact"].append(impact)
        self.vertex_attributes["freshness"].append(freshness)

        for att in self.attribute_names:
            if att not in processing_attributes:
                self.vertex_attributes[att].append(None)

        if inputs:
            self.edges.append((f"dataset_{dataset_id}", f"processing_{processing_id}"))
            self.edge_type.append("INPUTS")
            self.edges.append((f"processing_{processing_id}", f"system_{system_id}"))
            self.edge_type.append("INPUTS")

        else:
            self.edges.append((f"processing_{processing_id}", f"dataset_{dataset_id}"))
            self.edge_type.append("OUTPUTS")
            self.edges.append((f"system_{system_id}", f"processing_{processing_id}"))
            self.edge_type.append("OUTPUTS")
        logging.info(f"IGraph. Added processing {processing_id}.")

    def generate_data_integrity(self, data_integrity_id, dataset_collection_id, data_integrity_rec_time,
                                data_integrity_volat, data_integrity_reg_time, data_integrity_rest_time):
        """Generates data integrity node, and dataset collection - data integrity edge."""
        self.vertices.append(f"data_integrity_{data_integrity_id}")

        data_integrity_attributes = ["id", "data_integrity_rec_time", "data_integrity_rest_time",
                                     "data_integrity_reg_time", "data_integrity_volat"]
        self.vertex_attributes["id"].append(data_integrity_id)
        self.vertex_attributes["data_integrity_rec_time"].append(data_integrity_rec_time)
        self.vertex_attributes["data_integrity_rest_time"].append(data_integrity_rest_time)
        self.vertex_attributes["data_integrity_reg_time"].append(data_integrity_reg_time)
        self.vertex_attributes["data_integrity_volat"].append(data_integrity_volat)

        for att in self.attribute_names:
            if att not in data_integrity_attributes:
                self.vertex_attributes[att].append(None)

        self.edges.append((f"dataset_collection_{dataset_collection_id}", f"data_integrity_{data_integrity_id}"))
        self.edge_type.append("HAS")
        logging.info(f"IGraph. Added data integrity {data_integrity_id}.")

    def save_to_file(self, filename, overwrite=False):
        """Saves generated graph to .net file.

        Raises:
            ValueError: Graph database with this file already exists.
        """
        self.graph.add_vertices(self.vertices)

        for att in self.vertex_attributes:
            self.graph.vs[att] = self.vertex_attributes[att]
        self.graph.add_edges(self.edges)
        self.graph.es["type"] = self.edge_type

        if os.path.isfile(filename) and overwrite:
            os.remove(filename)
        elif os.path.isfile(filename):
            raise ValueError("Graph database with this file already exists.")
        self.graph.save(filename)
        logging.info(f"IGraph saved to {filename}.")

    def read_from_file(self, filename, overwrite=False):
        """Loads igraph from a binary .net file.

        Raises:
            ValueError: Graph attribute is not empty.
        """
        if self.graph.vcount() == 0 or overwrite:
            self.graph = load(filename)
        else:
            raise ValueError("Graph is not empty. Use overwrite arg if this is intended.")
        logging.info(f"IGraph loaded from {filename}.")
