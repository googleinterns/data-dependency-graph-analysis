"""Graph Generation Config Reader
This script reads the config from full_config.yaml file and processes it to be usable.
It saves config to variables, and converts map strings to python dictionaries.
"""

import yaml
from graph_generation.connection_generator import ConnectionGenerator

from graph_generation.config_params.collection_params import CollectionParams
from graph_generation.config_params.data_integrity_params import DataIntegrityParams
from graph_generation.config_params.dataset_params import DatasetParams
from graph_generation.config_params.dataset_to_system_params import DatasetToSystemParams
from graph_generation.config_params.processing_params import ProcessingParams
from graph_generation.config_params.system_params import SystemParams


def process_map(config_map, proba=False, enum=False):
    """Converts string map in format [key1:value1 key2:value2] into a dictionary.
       key - represents a count of connections (integer) or enums (string).
       value - represents a count of occurrences (integer) or probability (float).
   Args:
       config_map: string that contains the map
       proba: boolean, true if values are probabilities, not counts.
       enum: boolean, true if keys are enums, not integers.

   Returns:
       A dictionary of keys and values in converted format.
   """
    processed_map = {j[0]: j[1] for j in [i.split(":") for i in config_map.strip("[]").split()]}
    keys = [int(k) if not enum else k for k in processed_map.keys()]
    values = [int(v) if not proba else float(v) for v in processed_map.values()]
    return {keys[i]: values[i] for i in range(len(keys))}


if __name__ == '__main__':
    # Read config file
    with open('configs/full_config.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    dataset_params = DatasetParams(
        dataset_count=config["dataset"]["dataset_count"],
        dataset_env_count_map=process_map(config["dataset"]["dataset_env_count_map"], enum=True),
        dataset_slo_range=config["dataset"]["dataset_slo_range_seconds"]
    )

    system_params = SystemParams(
        system_count=config["system"]["system_count"],
        system_env_count_map=process_map(config["system"]["system_env_count_map"], enum=True),
        system_criticality_proba_map=process_map(config["system"]["system_criticality_proba_map"], enum=True,
                                                 proba=True)
    )

    dataset_to_system_params = DatasetToSystemParams(
        dataset_read_count_map=process_map(config["dataset"]["dataset_read_count_map"]),
        dataset_write_count_map=process_map(config["dataset"]["dataset_write_count_map"]),
        system_input_count_map=process_map(config["system"]["system_inputs_count_map"]),
        system_output_count_map=process_map(config["system"]["system_outputs_count_map"])
    )

    collection_params = CollectionParams(
        dataset_count_map=process_map(config["dataset_collection"]["dataset_count_map"]),
        system_count_map=process_map(config["system_collection"]["system_count_map"])
    )

    processing_params = ProcessingParams(
        dataset_criticality_proba_map=process_map(config["data_processing"]["dataset_criticality_proba_map"],
                                                  proba=True, enum=True),
        dataset_impact_proba_map=process_map(config["data_processing"]["dataset_impact_proba_map"], proba=True,
                                             enum=True)
    )

    data_integrity_params = DataIntegrityParams(
        data_restoration_range_seconds=config["data_integrity"]["restoration_range_seconds"],
        data_regeneration_range_seconds=config["data_integrity"]["regeneration_range_seconds"],
        data_reconstruction_range_seconds=config["data_integrity"]["reconstruction_range_seconds"],
        data_volatility_proba_map=process_map(config["data_integrity"]["volatality_proba_map"], proba=True)
    )

    graph_connections = ConnectionGenerator(
        dataset_params=dataset_params,
        system_params=system_params,
        dataset_to_system_params=dataset_to_system_params,
        collection_params=collection_params
    )

    graph_connections.generate()
