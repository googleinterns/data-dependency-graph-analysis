"""
This module implements methods for generating random attributes from nodes in a graph based on distribution and range.

Method generate() will create all the necessary attributes for the graph:
    Collection: name.
    Dataset collection: name.
    System collection: name.
    System: system criticality, environment, description, name, regex grouping.
    Dataset: slo, environment, description, name, regex grouping.
    Data integrity: reconstruction time, volatility, regeneration time, restoration time.
    Dataset processing: impact, freshness.
"""

import random


class AttributeGenerator:
    """
    A class to generate random attributes for nodes based on distribution or range of values.
    ...
    Attributes:
        collection_params: Instance of CollectionParams.
        dataset_params: Instance of DatasetParams.
        system_params: Instance of SystemParams.
        data_integrity_params: Instance of DataIntegrityParams.
        processing_params: Instance of ProcessingParams.
        connection_params: Instance of ConnectionParams.

        dataset_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        system_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        dataset_processing_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        data_integrity_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.

    Methods:
        _generate_time()
            Generates time strings from given range in seconds.
        _generate_from_proba()
            Generates value from given probability map.
        _generate_dataset_attributes()
            Generates all necessary dataset attributes.
        _generate_system_attributes()
            Generates all necessary system attributes.
        _generate_processing_attributes()
            Generates all dataset processing attributes.
        _generate_data_integrity_attributes()
            Generates all data integrity attributes.
        generate()
            Generates all the needed attributes for data dependency mapping graph.
    """
    def __init__(self, collection_params, dataset_params, system_params, data_integrity_params, processing_params,
                 connection_params):
        self.collection_params = collection_params
        self.dataset_params = dataset_params
        self.system_params = system_params
        self.data_integrity_params = data_integrity_params
        self.processing_params = processing_params
        self.connection_params = connection_params

        self.collection_attributes = {}
        self.dataset_collection_attributes = {}
        self.system_collection_attributes = {}
        self.dataset_attributes = {}
        self.system_attributes = {}
        self.dataset_processing_attributes = {}
        self.data_integrity_attributes = {}

    @staticmethod
    def _generate_time(n=1):
        """Generates n random time strings in format 1d / 25h / 121m / 46s"""
        generated_time = []
        time_ranges = {
            "d": (1, 30),
            "h": (1, 120),
            "m": (1, 720),
            "s": (1, 360)
        }
        for i in range(n):
            time_metric = random.choice(list(time_ranges.keys()))
            time_value = random.randint(time_ranges[time_metric][0], time_ranges[time_metric][1])
            generated_time.append(f"{time_value}{time_metric}")
        return generated_time

    @staticmethod
    def _generate_from_proba(proba_map, n=1):
        """Generates n random values with replacement from map using their probability."""
        population = list(proba_map.keys())
        probability = list(proba_map.values())

        # Normalise probability
        probability = [i / sum(probability) for i in probability]
        return random.choices(population, probability, k=n)

    @staticmethod
    def _generate_description(node_type, node_id):
        """Generates random description for a node (ex. Dataset number 1.)."""
        return f"{node_type.capitalize()} number {node_id}."

    @staticmethod
    def _generate_regex(node_type, node_id):
        """Generates random regex grouping."""
        return f"{node_type}.{node_id}.*"

    @staticmethod
    def _generate_name(node_type, node_id):
        """Generates random node name."""
        return f"{node_type}.{node_id}"

    def _generate_collection_attributes(self):
        """Generates name for collections."""
        collection_names = [self._generate_name("collection", i) for i in range(self.collection_params.collection_count)]
        self.collection_attributes["names"] = collection_names

    def _generate_dataset_collection_attributes(self):
        """Generates name for dataset collections."""
        dataset_collection_names = [self._generate_name("dataset collection", i)
                                    for i in range(self.collection_params.dataset_collection_count)]
        self.dataset_collection_attributes["names"] = dataset_collection_names

    def _generate_system_collection_attributes(self):
        """Generates name for system collections."""
        system_collection_names = [self._generate_name("system collection", i)
                                   for i in range(self.collection_params.system_collection_count)]
        self.system_collection_attributes["names"] = system_collection_names

    def _generate_dataset_attributes(self):
        """Generates slo, environments, regex groupings and names for datasets."""
        dataset_descriptions = [self._generate_description("dataset", i) for i in range(self.dataset_params.dataset_count)]
        dataset_regexs = [self._generate_regex("dataset", i) for i in range(self.dataset_params.dataset_count)]
        dataset_names = [self._generate_name("dataset", i) for i in range(self.dataset_params.dataset_count)]

        dataset_slos = self._generate_time(n=self.dataset_params.dataset_count)
        # View counts as probability of being picked
        dataset_environments = self._generate_from_proba(self.dataset_params.dataset_env_count_map,
                                                         n=self.dataset_params.dataset_count)

        self.dataset_attributes["descriptions"] = dataset_descriptions
        self.dataset_attributes["names"] = dataset_names
        self.dataset_attributes["regex_groupings"] = dataset_regexs
        self.dataset_attributes["dataset_slos"] = dataset_slos
        self.dataset_attributes["dataset_environments"] = dataset_environments

    def _generate_system_attributes(self):
        """Generates system criticality, system environments, regex groupings, names and descriptions for systems."""
        system_descriptions = [self._generate_description("system", i) for i in range(self.system_params.system_count)]
        system_regexs = [self._generate_regex("system", i) for i in range(self.system_params.system_count)]
        system_names = [self._generate_name("system", i) for i in range(self.system_params.system_count)]

        system_criticalities = self._generate_from_proba(self.system_params.system_criticality_proba_map,
                                                         n=self.system_params.system_count)
        # View counts as probability of being picked
        system_environments = self._generate_from_proba(self.system_params.system_env_count_map,
                                                        n=self.system_params.system_count)

        self.system_attributes["regex_groupings"] = system_regexs
        self.system_attributes["names"] = system_names
        self.system_attributes["descriptions"] = system_descriptions
        self.system_attributes["system_criticalities"] = system_criticalities
        self.system_attributes["system_environments"] = system_environments

    def _generate_processing_attributes(self):
        """Generates dataset impacts and dataset freshness."""
        dataset_impacts = self._generate_from_proba(self.processing_params.dataset_impact_proba_map,
                                                    n=self.connection_params.dataset_system_connection_count)
        dataset_freshness = self._generate_from_proba(self.processing_params.dataset_criticality_proba_map,
                                                      n=self.connection_params.dataset_system_connection_count)

        self.dataset_processing_attributes["dataset_impacts"] = dataset_impacts
        self.dataset_processing_attributes["dataset_freshness"] = dataset_freshness

    def _generate_data_integrity_attributes(self):
        """Generates restoration, regeneration, reconstruction times and volatility for each dataset collection."""
        data_restoration_time = self._generate_time(n=self.collection_params.dataset_collection_count)
        data_regeneration_time = self._generate_time(n=self.collection_params.dataset_collection_count)
        data_reconstruction_time = self._generate_time(n=self.collection_params.dataset_collection_count)
        data_volatility = self._generate_from_proba(self.data_integrity_params.data_volatility_proba_map,
                                                    n=self.collection_params.dataset_collection_count)
        self.data_integrity_attributes["data_restoration_time"] = data_restoration_time
        self.data_integrity_attributes["data_regeneration_time"] = data_regeneration_time
        self.data_integrity_attributes["data_reconstruction_time"] = data_reconstruction_time
        self.data_integrity_attributes["data_volatility"] = data_volatility

    def generate(self):
        """Generates all needed attributes."""
        self._generate_collection_attributes()
        self._generate_dataset_collection_attributes()
        self._generate_system_collection_attributes()
        self._generate_dataset_attributes()
        self._generate_system_attributes()
        self._generate_processing_attributes()
        self._generate_data_integrity_attributes()
