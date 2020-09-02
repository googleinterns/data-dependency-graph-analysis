"""Graph Generation Config Reader

This script reads the config from full_config.yaml file and processes it to be usable.
It saves config to variables, and converts map strings to python dictionaries.
"""

import yaml


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
    with open('full_config.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # Dataset params
    dataset_count = config["dataset"]["dataset_count"]

    dataset_read_count_map = process_map(config["dataset"]["dataset_read_count_map"])
    dataset_read_count = sum(dataset_read_count_map.values())
    dataset_write_count_map = process_map(config["dataset"]["dataset_write_count_map"])
    dataset_write_count = sum(dataset_write_count_map.values())

    dataset_slo_range_seconds = config["dataset"]["dataset_slo_range"]

    # System params
    system_count = config["system"]["system_count"]
    system_input_count_map = process_map(config["system"]["system_inputs_count_map"])
    system_input_count = sum(system_input_count_map.values())
    system_output_count_map = process_map(config["system"]["system_outputs_count_map"])
    system_output_count = sum(system_output_count_map.values())

    system_criticality_proba_map = process_map(config["system"]["system_criticality_proba_map"], enum=True, proba=True)

    # Dataset collection params
    dataset_collection_count = config["dataset_collection"]["dataset_collection_count"]
    datasets_in_collection_count_map = process_map(config["dataset_collection"]["dataset_count_map"])

    # System collection params
    system_collection_count = config["system_collection"]["system_collection_count"]
    systems_in_collection_count_map = process_map(config["system_collection"]["system_count_map"])

    # Env params
    env_count = config["env"]["env_count"]
    env_type_count_map = process_map(config["env"]["env_count_map"], enum=True)

    # Data processing params
    dataset_impact_proba_map = process_map(config["data_processing"]["dataset_impact_proba_map"], proba=True, enum=True)
    dataset_criticality_proba_map = process_map(config["data_processing"]["dataset_criticality_proba_map"],
                                                proba=True, enum=True)

    # Data integrity params
    data_restoration_range_seconds = config["data_integrity"]["restoration_range_seconds"]
    data_regeneration_range_seconds = config["data_integrity"]["regeneration_range_seconds"]
    data_reconstruction_range_seconds = config["data_integrity"]["reconstruction_range_seconds"]
    data_volatility_value, data_volatility_count = process_map(config["data_integrity"]["volatality_proba_map"],
                                                               proba=True)
