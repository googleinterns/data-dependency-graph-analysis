"""
This module implements methods for generating random attributes from nodes in a graph based on distribution and range.

Method generate() will create all the necessary attributes for the graph:
    System: system criticality, environment.
    Dataset: slo, environment.
    Data integrity: reconstruction time, volatility, regeneration time, restoration time.
    Dataset processing: impact, freshness.
"""

import random


class AttributeGenerator:
    """
    A class to generate random attributes for nodes based on distribution or range of values.
    ...
    Attributes:
        dataset_count: Integer of how many datasets are in a graph.
        system_count: Integer of how many systems are in a graph.
        dataset_system_connection_count: Integer of how many dataset - systems connections are in a graph.
        env_count_map: Dictionary that maps environment type to count.
        dataset_slo_range: List with min and max of dataset slo (seconds).
        data_restoration_range_seconds: List with min and max of data restoration range (seconds).
        data_regeneration_range_seconds: List with min and max of data regeneration range (seconds).
        data_reconstruction_range_seconds: List with min and max of data reconstruction range (seconds).
        data_volatility_proba_map: Dictionary that maps data volatility to probability.
        dataset_impact_proba_map: Dictionary that maps dataset impact type to probability.
        dataset_criticality_proba_map: Dictionary that maps dataset criticality type to probability.
        system_criticality_proba_map: Dictionary that maps system criticality type to probability.

        dataset_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        system_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        dataset_processing_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.
        data_integrity_attributes: Dictionary with keys as attribute type, and value lists of generated attributes.

    Methods:
        generate_time()
            Generates time strings from given range in seconds.
        generate_from_proba()
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
    def __init__(self, dataset_params, system_params, data_integrity_params, processing_params, connection_params):
        self.system_count = system_params.system_count
        self.system_env_count_map = system_params.system_env_count_map
        self.system_criticality_proba_map = system_params.system_criticality_proba_map

        self.dataset_count = dataset_params.dataset_count
        self.dataset_env_count_map = dataset_params.dataset_env_count_map
        self.dataset_slo_range = dataset_params.dataset_slo_range

        self.data_restoration_range_seconds = data_integrity_params.data_restoration_range_seconds
        self.data_regeneration_range_seconds = data_integrity_params.data_regeneration_range_seconds
        self.data_reconstruction_range_seconds = data_integrity_params.data_reconstruction_range_seconds
        self.data_volatility_proba_map = data_integrity_params.data_volatility_proba_map

        self.dataset_impact_proba_map = processing_params.dataset_impact_proba_map
        self.dataset_criticality_proba_map = processing_params.dataset_criticality_proba_map

        self.dataset_system_connection_count = connection_params.dataset_system_connection_count

        self.dataset_attributes = {}
        self.system_attributes = {}
        self.dataset_processing_attributes = {}
        self.data_integrity_attributes = {}

    @staticmethod
    def generate_time(seconds_range, n=1):
        """Generates n random time strings in given seconds range in format '1d 2h 10m 46s'"""
        generated_time = []
        for i in range(n):
            total_seconds = random.randint(seconds_range[0], seconds_range[1])
            minutes, seconds = divmod(total_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            days, hours = divmod(hours, 24)
            generated_time.append(f"{days}d {hours}h {minutes}m {seconds}s")
        return generated_time

    @staticmethod
    def generate_from_proba(proba_map, n=1):
        """Generates n random values with replacement from map using their probability."""
        population = list(proba_map.keys())
        probability = list(proba_map.values())

        # Normalise probability
        probability = [i / sum(probability) for i in probability]
        return random.choices(population, probability, k=n)

    def _generate_dataset_attributes(self):
        """Generates slo and environments for dataset."""
        dataset_slos = self.generate_time(self.dataset_slo_range, n=self.dataset_count)
        # View counts as probability of being picked
        dataset_environments = self.generate_from_proba(self.dataset_env_count_map, n=self.dataset_count)

        self.dataset_attributes["dataset_slos"] = dataset_slos
        self.dataset_attributes["dataset_environments"] = dataset_environments

    def _generate_system_attributes(self):
        """Generate system criticality and system environments."""
        system_criticalities = self.generate_from_proba(self.system_criticality_proba_map, n=self.system_count)
        # View counts as probability of being picked
        system_environments = self.generate_from_proba(self.system_env_count_map, n=self.system_count)

        self.system_attributes["system_criticalities"] = system_criticalities
        self.system_attributes["system_environments"] = system_environments

    def _generate_processing_attributes(self):
        """Generate dataset impacts and dataset freshness."""
        dataset_impacts = self.generate_from_proba(self.dataset_impact_proba_map,
                                                   n=self.dataset_system_connection_count)
        dataset_freshness = self.generate_from_proba(self.dataset_criticality_proba_map,
                                                     n=self.dataset_system_connection_count)

        self.dataset_processing_attributes["dataset_impacts"] = dataset_impacts
        self.dataset_processing_attributes["dataset_freshness"] = dataset_freshness

    def _generate_data_integrity_attributes(self):
        """Generate restoration, regeneration, reconstruction times and volatility for each dataset."""
        data_restoration_time = self.generate_time(self.data_restoration_range_seconds, n=self.dataset_count)
        data_regeneration_time = self.generate_time(self.data_regeneration_range_seconds, n=self.dataset_count)
        data_reconstruction_time = self.generate_time(self.data_reconstruction_range_seconds, n=self.dataset_count)
        data_volatility = self.generate_from_proba(self.data_volatility_proba_map, n=self.dataset_count)
        self.data_integrity_attributes["data_restoration_time"] = data_restoration_time
        self.data_integrity_attributes["data_regeneration_time"] = data_regeneration_time
        self.data_integrity_attributes["data_reconstruction_time"] = data_reconstruction_time
        self.data_integrity_attributes["data_volatility"] = data_volatility

    def generate(self):
        """Generate all needed attributes."""
        self._generate_dataset_attributes()
        self._generate_system_attributes()
        self._generate_processing_attributes()
        self._generate_data_integrity_attributes()
