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
            Saves generated graph message to .net binary.

        read_from_file(filename, overwrite=False)
            Reads graph message from .net binary.
    """
    def __init__(self):
        self.graph = Graph(directed=True)

    def generate_collection(self, collection_id):
        """Generates collection node."""
        collection_vertex = self.graph.add_vertex(f"collection_{collection_id}")
        vertex_id = collection_vertex.index
        self.graph.vs[vertex_id]["collection_id"] = collection_id
        self.graph.vs[vertex_id]["description"] = f"Collection number {collection_id}."
        logging.info(f"IGraph. Added collection {collection_id}.")

    def generate_dataset_collection(self, dataset_collection_id, collection_id):
        """Generates dataset collection node and dataset collection - collection edge."""
        dataset_collection_vertex = self.graph.add_vertex(f"dataset_collection_{dataset_collection_id}")
        vertex_id = dataset_collection_vertex.index
        self.graph.vs[vertex_id]["dataset_collection_id"] = dataset_collection_id
        self.graph.vs[vertex_id]["collection_id"] = collection_id
        self.graph.vs[vertex_id]["description"] = f"Dataset collection number {dataset_collection_id}."
        dataset_collection_to_collection_edge = self.graph.add_edge(f"collection_{collection_id}",
                                                                    f"dataset_collection_{dataset_collection_id}")
        edge_id = dataset_collection_to_collection_edge.index
        self.graph.es[edge_id]["type"] = "CONTAINS"
        logging.info(f"IGraph. Added dataset collection {dataset_collection_id}.")

    def generate_system_collection(self, system_collection_id, collection_id):
        """Generates system collection node and system collection - collection edge."""
        system_collection_vertex = self.graph.add_vertex(f"system_collection_{system_collection_id}")
        vertex_id = system_collection_vertex.index
        self.graph.vs[vertex_id]["system_collection_id"] = system_collection_id
        self.graph.vs[vertex_id]["description"] = f"System collection number {system_collection_id}."
        system_collection_to_collection_edge = self.graph.add_edge(f"collection_{collection_id}",
                                                                   f"system_collection_{system_collection_id}")
        edge_id = system_collection_to_collection_edge.index
        self.graph.es[edge_id]["type"] = "CONTAINS"
        logging.info(f"IGraph. Added system collection {system_collection_id}.")

    def generate_dataset(self, dataset_id, dataset_collection_id, slo, env):
        """Generates dataset node and dataset - dataset collection edge."""
        dataset_vertex = self.graph.add_vertex(f"dataset_{dataset_id}")
        vertex_id = dataset_vertex.index
        self.graph.vs[vertex_id]["dataset_id"] = dataset_id
        self.graph.vs[vertex_id]["dataset_collection_id"] = dataset_collection_id
        self.graph.vs[vertex_id]["regex_grouping"] = f"data.{dataset_id}.*"
        self.graph.vs[vertex_id]["dataset_name"] = f"dataset.{dataset_id}"
        self.graph.vs[vertex_id]["description"] = f"Dataset number {dataset_id}"
        self.graph.vs[vertex_id]["slo"] = slo
        self.graph.vs[vertex_id]["env"] = env

        dataset_to_collection_edge = self.graph.add_edge(f"dataset_collection_{dataset_collection_id}",
                                                         f"dataset_{dataset_id}")
        edge_id = dataset_to_collection_edge.index
        self.graph.es[edge_id]["type"] = "CONTAINS"
        logging.info(f"IGraph. Added dataset {dataset_id}.")

    def generate_system(self, system_id, system_critic, system_collection_id, env):
        """Generates system node and system - system collection edge."""
        system_vertex = self.graph.add_vertex(f"system_{system_id}")
        vertex_id = system_vertex.index
        self.graph.vs[vertex_id]["system_id"] = system_id
        self.graph.vs[vertex_id]["system_collection_id"] = system_collection_id
        self.graph.vs[vertex_id]["regex_grouping"] = f"system.{system_id}.*"
        self.graph.vs[vertex_id]["system_name"] = f"system.{system_id}"
        self.graph.vs[vertex_id]["description"] = f"System number {system_id}"
        self.graph.vs[vertex_id]["system_critic"] = system_critic
        self.graph.vs[vertex_id]["env"] = env

        system_to_collection_edge = self.graph.add_edge(f"system_collection_{system_collection_id}",
                                                        f"system_{system_id}")
        edge_id = system_to_collection_edge.index
        self.graph.es[edge_id]["type"] = "CONTAINS"
        logging.info(f"IGraph. Added system {system_id}.")

    def generate_processing(self, system_id, dataset_id, processing_id, impact, freshness, inputs=True):
        """Generates processing node and processing - dataset, processing - system edges."""
        processing_vertex = self.graph.add_vertex(f"processing_{processing_id}")
        vertex_id = processing_vertex.index
        self.graph.vs[vertex_id]["processing_id"] = processing_id
        self.graph.vs[vertex_id]["impact"] = impact
        self.graph.vs[vertex_id]["freshness"] = freshness

        if inputs:
            dataset_to_processing_edge = self.graph.add_edge(f"dataset_{dataset_id}", f"processing_{processing_id}")
            edge_id = dataset_to_processing_edge.index
            self.graph.es[edge_id]["type"] = "INPUTS"
            processing_to_system_edge = self.graph.add_edge(f"processing_{processing_id}", f"system_{system_id}")
            edge_id = processing_to_system_edge.index
            self.graph.es[edge_id]["type"] = "INPUTS"

        else:
            dataset_to_processing_edge = self.graph.add_edge(f"processing_{processing_id}", f"dataset_{dataset_id}")
            edge_id = dataset_to_processing_edge.index
            self.graph.es[edge_id]["type"] = "OUTPUTS"
            processing_to_system_edge = self.graph.add_edge(f"system_{system_id}", f"processing_{processing_id}")
            edge_id = processing_to_system_edge.index
            self.graph.es[edge_id]["type"] = "OUTPUTS"
            logging.info(f"IGraph. Added processing {processing_id}.")

    def generate_data_integrity(self, data_integrity_id, dataset_collection_id, data_integrity_rec_time,
                                data_integrity_volat, data_integrity_reg_time, data_integrity_rest_time):
        """Generates data integrity node, and dataset collection - data integrity edge."""
        data_integrity_vertex = self.graph.add_vertex(f"data_integrity_{data_integrity_id}")
        vertex_id = data_integrity_vertex.index
        self.graph.vs[vertex_id]["data_integrity_id"] = data_integrity_id
        self.graph.vs[vertex_id]["data_integrity_rec_time"] = data_integrity_rec_time
        self.graph.vs[vertex_id]["data_integrity_volat"] = data_integrity_volat
        self.graph.vs[vertex_id]["data_integrity_reg_time"] = data_integrity_reg_time
        self.graph.vs[vertex_id]["data_integrity_rest_time"] = data_integrity_rest_time

        data_integrity_to_collection = self.graph.add_edge(f"dataset_collection_{dataset_collection_id}",
                                                           f"data_integrity_{data_integrity_id}")
        edge_id = data_integrity_to_collection.index
        self.graph.es[edge_id]["type"] = "HAS"
        logging.info(f"IGraph. Added data integrity {data_integrity_id}.")

    def save_to_file(self, filename, overwrite=False):
        """Saves generated graph to .net file.

        Raises:
            ValueError: Graph database with this file already exists.
        """
        if os.path.isfile(filename) and overwrite:
            os.remove(filename)
        elif os.path.isfile(filename):
            raise ValueError("Graph database with this file already exists.")
        self.graph.save(filename)
        logging.info(f"IGraph saved to {filename}.")

    def read_from_file(self, filename, overwrite=False):
        if self.graph.vcount() == 0 or overwrite:
            self.graph = load(filename)
        else:
            raise ValueError("Graph is not empty. Use overwrite arg if this is intended.")
        logging.info(f"IGraph loaded from {filename}.")
