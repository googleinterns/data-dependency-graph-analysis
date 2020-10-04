"""
This module implements methods for protobuf data dependency mapping graph creation.

It allows to create the following messages:
    collection
    dataset collection
    system collection
    dataset
    system
    environment
    dataset processing
    data integrity
"""

from proto import config_pb2
import logging
import os


class ProtoGraph:
    """
    A class to represent data dependency graph in proto format.

    ...

    Methods:
        generate_collection(collection_id, name)
            Generates a collection with the given id.

        generate_dataset_collection(dataset_collection_id, collection_id, name)
            Generates a dataset collection with a given id.

        generate_system_collection(system_collection_id, collection_id, name)
            Generates a system collection with a given id.

        generate_dataset(dataset_id, dataset_collection_id, regex_grouping, name, slo, env, description)
            Generates a dataset with a given id.

        generate_system(system_id, system_critic, system_collection_id, regex_grouping, name, env, description)
            Generates a system with a given id.

        generate_processing(system_id, dataset_id, processing_id, impact, freshness, action="INPUTS")
            Generates a processing node, that represents dataset - system relationship.
            Action parameter denotes if the dataset is an input to the system, or an output.

        generate_data_integrity(data_integrity_id, dataset_id, data_integrity_rec_time, data_integrity_volat,
                                data_integrity_reg_time, data_integrity_rest_time)
            Generates a data integrity node, that corresponds to a specific dataset, having the attributes.

        save_to_file(filename, overwrite=False)
            Saves generated graph message to proto binary.

        read_from_file(filename, overwrite=False)
            Reads graph message from proto binary.
    """
    def __init__(self):
        self.graph = config_pb2.ProtoGraph()
        self.is_empty = True

    @staticmethod
    def _get_env_enum(env):
        """Returns environment enum from proto config."""
        env_dict = {
            "DEVELOPMENT_ENV": config_pb2.ProtoGraph.Env.DEVELOPMENT_ENV,
            "PERSONAL_ENV": config_pb2.ProtoGraph.Env.PERSONAL_ENV,
            "PRODUCTION_ENV": config_pb2.ProtoGraph.Env.PRODUCTION_ENV,
            "STAGING_ENV": config_pb2.ProtoGraph.Env.STAGING_ENV,
            "TESTING_ENV": config_pb2.ProtoGraph.Env.TESTING_ENV,
            "UNKNOWN_ENV": config_pb2.ProtoGraph.Env.UNKNOWN_ENV
        }
        if env in env_dict:
            return env_dict[env]
        else:
            logging.error("Incorrect environment value. Setting UNKNOWN_ENV as a default.")
            return env_dict["UNKNOWN_ENV"]

    @staticmethod
    def env_enum_to_string(env):
        return config_pb2.ProtoGraph.Env.Name(env)

    @staticmethod
    def _get_system_criticality_enum(system_criticality):
        """Returns system criticality enum from proto config."""
        system_criticality_dict = {
            "NOT_CRITICAL": config_pb2.ProtoGraph.System.SystemCriticality.NOT_CRITICAL,
            "CRITICAL_CAN_CAUSE_S0_OUTAGE": config_pb2.ProtoGraph.System.SystemCriticality.CRITICAL_CAN_CAUSE_S0_OUTAGE,
            "CRITICAL_SIGNIFICANT_RUN_RATE": config_pb2.ProtoGraph.System.SystemCriticality.CRITICAL_SIGNIFICANT_RUN_RATE,
            "CRITICAL_OTHER": config_pb2.ProtoGraph.System.SystemCriticality.CRITICAL_OTHER
        }
        if system_criticality in system_criticality_dict:
            return system_criticality_dict[system_criticality]
        else:
            logging.error("Incorrect system criticality value. Setting NEVER as a default.")
            return system_criticality_dict["CRITICAL_OTHER"]

    @staticmethod
    def criticality_enum_to_string(system_criticality):
        return config_pb2.ProtoGraph.System.SystemCriticality.Name(system_criticality)

    @staticmethod
    def _get_processing_impact_enum(impact):
        """Returns processing impact enum from proto config."""
        impact_dict = {
            "DOWN": config_pb2.ProtoGraph.Processing.Impact.DOWN,
            "SEVERELY_DEGRADED": config_pb2.ProtoGraph.Processing.Impact.SEVERELY_DEGRADED,
            "DEGRADED": config_pb2.ProtoGraph.Processing.Impact.DEGRADED,
            "OPPORTUNITY_LOSS": config_pb2.ProtoGraph.Processing.Impact.OPPORTUNITY_LOSS,
            "NONE": config_pb2.ProtoGraph.Processing.Impact.NONE
        }
        if impact in impact_dict:
            return impact_dict[impact]
        else:
            logging.error("Incorrect processing impact value. Setting NONE as a default.")
            return impact_dict["NONE"]

    @staticmethod
    def processing_impact_enum_to_string(impact):
        return config_pb2.ProtoGraph.Processing.Impact.Name(impact)

    @staticmethod
    def _get_processing_freshness_enum(freshness):
        """Returns processing freshness enum from proto config"""
        freshness_dict = {
            "IMMEDIATE": config_pb2.ProtoGraph.Processing.Freshness.IMMEDIATE,
            "DAY": config_pb2.ProtoGraph.Processing.Freshness.DAY,
            "WEEK": config_pb2.ProtoGraph.Processing.Freshness.WEEK,
            "EVENTUALLY": config_pb2.ProtoGraph.Processing.Freshness.EVENTUALLY,
            "NEVER": config_pb2.ProtoGraph.Processing.Freshness.NEVER
        }
        if freshness in freshness_dict:
            return freshness_dict[freshness]
        else:
            logging.error("Incorrect processing freshness value. Setting NEVER as a default.")
            return freshness_dict["NEVER"]

    @staticmethod
    def processing_freshness_enum_to_string(freshness):
        return config_pb2.ProtoGraph.Processing.Freshness.Name(freshness)

    def generate_collection(self, collection_id, name):
        """Generates collection message."""
        self.is_empty = False
        collection = self.graph.collections.add()
        collection.collection_id = collection_id
        collection.name = name
        logging.info(f"Proto graph. Added collection {collection_id}.")

    def generate_dataset_collection(self, dataset_collection_id, collection_id, name):
        """Generates dataset collection message."""
        dataset_collection = self.graph.dataset_collections.add()
        dataset_collection.dataset_collection_id = dataset_collection_id
        dataset_collection.collection_id = collection_id
        dataset_collection.name = name
        logging.info(f"Proto graph. Added dataset collection {dataset_collection_id}.")

    def generate_system_collection(self, system_collection_id, collection_id, name):
        """Generates system collection message."""
        system_collection = self.graph.system_collections.add()
        system_collection.system_collection_id = system_collection_id
        system_collection.collection_id = collection_id
        system_collection.name = name
        logging.info(f"Proto graph. Added system collection {system_collection_id}.")

    def generate_dataset(self, dataset_id, dataset_collection_id, regex_grouping, name, slo, env, description):
        """Generates dataset message."""
        dataset = self.graph.datasets.add()
        dataset.dataset_id = dataset_id
        dataset.dataset_collection_id = dataset_collection_id
        dataset.slo = slo
        dataset.env = self._get_env_enum(env)
        dataset.regex_grouping = regex_grouping
        dataset.name = name
        dataset.description = description
        logging.info(f"Proto graph. Added dataset {dataset_id}.")

    def generate_system(self, system_id, system_critic, system_collection_id, regex_grouping, name, env, description):
        """Generate system message."""
        system = self.graph.systems.add()
        system.system_id = system_id
        system.system_collection_id = system_collection_id
        system.system_critic = self._get_system_criticality_enum(system_critic)
        system.env = self._get_env_enum(env)
        system.regex_grouping = regex_grouping
        system.name = name
        system.description = description
        logging.info(f"Proto graph. Added system {system_id}.")

    def generate_processing(self, system_id, dataset_id, processing_id, impact, freshness, inputs=True):
        """Generate processing message."""
        processing = self.graph.processings.add()
        processing.system_id = system_id
        processing.dataset_id = dataset_id
        processing.processing_id = processing_id
        processing.impact = self._get_processing_impact_enum(impact)
        processing.freshness = self._get_processing_freshness_enum(freshness)
        processing.inputs = inputs
        logging.info(f"Proto graph. Added processing {processing_id}.")

    def generate_data_integrity(self, data_integrity_id, dataset_collection_id, data_integrity_rec_time,
                                data_integrity_volat, data_integrity_reg_time, data_integrity_rest_time):
        """Generates data integrity message."""
        data_integrity = self.graph.data_integrities.add()
        data_integrity.data_integrity_id = data_integrity_id
        data_integrity.dataset_collection_id = dataset_collection_id
        data_integrity.data_integrity_rec_time = data_integrity_rec_time
        data_integrity.data_integrity_volat = data_integrity_volat
        data_integrity.data_integrity_reg_time = data_integrity_reg_time
        data_integrity.data_integrity_rest_time = data_integrity_rest_time
        logging.info(f"Proto graph. Added data integrity {data_integrity_id}.")

    def save_to_file(self, filename, overwrite=False):
        """
        Saves generated graph message to binary. If overwrite - existing file will be overwritten.

        Raises:
            ValueError: Graph database with this file already exists.
        """
        if os.path.isfile(filename) and overwrite:
            os.remove(filename)
        elif os.path.isfile(filename):
            raise ValueError("Graph database with this file already exists.")
        with open(filename, "wb") as f:
            f.write(self.graph.SerializeToString())
        logging.info(f"Proto graph saved to {filename}.")

    def read_from_file(self, filename, overwrite=False):
        """
        Reads graph message from binary to graph attribute. If overwrite - existing graph will be overwritten.

        Raises:
            ValueError: Graph attribute is not empty.
        """
        if self.is_empty or overwrite:
            with open(filename, "rb") as f:
                self.graph.ParseFromString(f.read())
            logging.info(f"Proto graph loaded from {filename}.")
        else:
            raise ValueError("Graph is not empty. Use overwrite arg if this is intended.")
