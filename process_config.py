import yaml
from connection_generator import ConnectionGenerator

from config_params.collection_params import CollectionParams
from config_params.data_integrity_params import DataIntegrityParams
from config_params.dataset_params import DatasetParams
from config_params.dataset_to_system_params import DatasetToSystemParams
from config_params.processing_params import ProcessingParams
from config_params.system_params import SystemParams


def process_map(config_map, proba=False, enum=False):
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
