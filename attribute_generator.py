import random


class AttributeGenerator:
    def __init__(self,
                 dataset_count,
                 system_count,
                 dataset_system_connection_count,
                 env_count_map,
                 dataset_slo_range,
                 data_restoration_range_seconds,
                 data_regeneration_range_seconds,
                 data_reconstruction_range_seconds,
                 data_volatility_count_map,
                 dataset_impact_proba_map,
                 dataset_criticality_proba_map,
                 system_criticality_proba_map):
        self.dataset_count = dataset_count
        self.system_count = system_count
        self.dataset_system_connection_count = dataset_system_connection_count

        self.env_count_map = env_count_map

        self.dataset_slo_range = dataset_slo_range

        self.data_restoration_range_seconds = data_restoration_range_seconds
        self.data_regeneration_range_seconds = data_regeneration_range_seconds
        self.data_reconstruction_range_seconds = data_reconstruction_range_seconds
        self.data_volatility_count_map = data_volatility_count_map

        self.dataset_impact_proba_map = dataset_impact_proba_map
        self.dataset_criticality_proba_map = dataset_criticality_proba_map

        self.system_criticality_proba_map = system_criticality_proba_map

        self.dataset_attributes = {}
        self.system_attributes = {}
        self.dataset_processing_attributes = {}
        self.data_integrity_attributes = {}

    @staticmethod
    def generate_time(seconds_range, n=1):
        total_seconds = [random.randint(seconds_range[0], seconds_range[1]) for _ in range(n)]
        seconds = [i % 60 for i in total_seconds]
        minutes = [i // 60 for i in total_seconds]
        hours = [i // 60 for i in minutes]
        days = [i // 24 for i in hours]
        return [f"{days[i]}d {hours[i]}h {minutes[i]}m {seconds[i]}s" for i in range(n)]

    @staticmethod
    def generate_from_proba(proba_map, n=1):
        population = list(proba_map.keys())
        probability = list(proba_map.values())

        # Normalise probability
        probability = [i / sum(probability) for i in probability]
        return random.choices(population, probability, k=n)

    def generate_dataset_attributes(self):
        dataset_slos = self.generate_time(self.dataset_slo_range, n=self.dataset_count)
        # View counts as probability of being picked
        dataset_environments = self.generate_from_proba(self.env_count_map, n=self.dataset_count)

        self.dataset_attributes["dataset_slos"] = dataset_slos
        self.dataset_attributes["dataset_environments"] = dataset_environments

    def generate_system_attributes(self):
        system_criticalities = self.generate_from_proba(self.system_criticality_proba_map, n=self.system_count)
        # View counts as probability of being picked
        system_environments = self.generate_from_proba(self.env_count_map, n=self.system_count)

        self.system_attributes["system_criticalities"] = system_criticalities
        self.system_attributes["system_environments"] = system_environments

    def generate_processing_attributes(self):
        dataset_impacts = self.generate_from_proba(self.dataset_impact_proba_map,
                                                   n=self.dataset_system_connection_count)
        dataset_freshness = self.generate_from_proba(self.dataset_criticality_proba_map,
                                                     n=self.dataset_system_connection_count)

        self.dataset_processing_attributes["dataset_impacts"] = dataset_impacts
        self.dataset_processing_attributes["dataset_freshness"] = dataset_freshness

    def generate_data_integrity_attributes(self):
        data_restoration_time = self.generate_time(self.data_restoration_range_seconds, n=self.dataset_count)
        data_regeneration_time = self.generate_time(self.data_regeneration_range_seconds, n=self.dataset_count)
        data_reconstruction_time = self.generate_time(self.data_reconstruction_range_seconds, n=self.dataset_count)
        data_volatility = self.generate_from_proba(self.data_volatility_count_map, n=self.dataset_count)
        self.data_integrity_attributes["data_restoration_time"] = data_restoration_time
        self.data_integrity_attributes["data_regeneration_time"] = data_regeneration_time
        self.data_integrity_attributes["data_reconstruction_time"] = data_reconstruction_time
        self.data_integrity_attributes["data_volatility"] = data_volatility

    def generate(self):
        self.generate_dataset_attributes()
        self.generate_system_attributes()
        self.generate_processing_attributes()
        self.generate_data_integrity_attributes()
