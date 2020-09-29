"""
This module generates a random data-dependancy mapping graph from config file.
You can create graph in proto format, or in networkx format.

Usage:
    python3 graph_generation/generate_from_config.py \
         --output_file "output.graphml" \
         --config_file "graph_generation/configs/config_15_09_20.yaml" \
         --graph_type "networkx" \
         --overwrite
"""

import yaml
import time
import logging
import argparse

from connection_generator import ConnectionGenerator
from attribute_generator import AttributeGenerator
from graph_template import GraphTemplate

from config_params.collection_params import CollectionParams
from config_params.data_integrity_params import DataIntegrityParams
from config_params.dataset_params import DatasetParams
from config_params.dataset_to_system_params import DatasetToSystemParams
from config_params.processing_params import ProcessingParams
from config_params.system_params import SystemParams
from config_params.connection_params import ConnectionParams


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


def parse_args():
    """Parses input arguments."""
    parser = argparse.ArgumentParser(description='Generate graph based on config.')
    parser.add_argument('-f', '--output_file', help='Path to output graphml or proto binary.', required=True)
    parser.add_argument('-c', '--config_file', help='Path to yaml config file with graph parameters.', required=True)
    parser.add_argument('-t', '--graph_type', help='Type of the graph to generate. Can be proto or networkx.',
                        default="networkx", choices=["proto", "networkx"])
    parser.add_argument('-o', '--overwrite', help='If output file exists, overwrite it.', type=bool, default=False)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    # Parse command line arguments.
    args = parse_args()
    output_file = args.output_file
    config_path = args.config_file
    graph_type = args.graph_type
    overwrite = args.overwrite

    # Load config file.
    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # Generate parameters.
    dataset_params = DatasetParams(
        dataset_count=config["dataset"]["dataset_count"],
        dataset_env_count_map=process_map(config["dataset"]["dataset_env_count_map"], enum=True),
        dataset_slo_range=config["dataset"]["dataset_slo_range_seconds"]
    )

    system_params = SystemParams(
        system_count=config["system"]["system_count"],
        system_env_count_map=process_map(config["system"]["system_env_count_map"], enum=True),
        system_criticality_proba_map=process_map(config["system"]["system_criticality_proba_map"], enum=True, proba=True)
    )

    dataset_to_system_params = DatasetToSystemParams(
        dataset_read_count_map=process_map(config["dataset"]["dataset_read_count_map"]),
        dataset_write_count_map=process_map(config["dataset"]["dataset_write_count_map"]),
        system_input_count_map=process_map(config["system"]["system_inputs_count_map"]),
        system_output_count_map=process_map(config["system"]["system_outputs_count_map"])
    )

    collection_params = CollectionParams(
        dataset_count_map=process_map(config["dataset_collection"]["dataset_count_map"]),
        system_count_map=process_map(config["system_collection"]["system_count_map"]),
        dataset_collection_count=config["dataset_collection"]["dataset_collection_count"],
        dataset_collection_count_map=process_map(config["collection"]["dataset_collection_count_map"]),
        system_collection_count=config["system_collection"]["system_collection_count"],
        system_collection_count_map=process_map(config["collection"]["system_collection_count_map"]),
        collection_count=config["collection"]["collection_count"]
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

    # Generate random connections between nodes.
    graph_connections = ConnectionGenerator(
        dataset_params=dataset_params,
        system_params=system_params,
        dataset_to_system_params=dataset_to_system_params,
        collection_params=collection_params
    )
    graph_connections.generate()
    logging.info(f"Successfully generated connections.")

    # Number of connections can not be predicted, so we need to get them for attribute generation.
    system_write_conn_count = sum([len(i) for i in graph_connections.dataset_write_conn_systems.values()])
    system_read_conn_count = sum([len(i) for i in graph_connections.dataset_read_conn_systems.values()])
    connection_params = ConnectionParams(system_write_conn_count, system_read_conn_count)

    # Generate attributes.
    graph_attributes = AttributeGenerator(dataset_params, system_params, data_integrity_params, processing_params,
                                          connection_params)
    graph_attributes.generate()
    logging.info(f"Successfully generated attributes.")

    # Create a graph.
    if graph_type == "proto":
        graph = GraphTemplate().get_proto_graph()
    else:
        graph = GraphTemplate().get_nx_graph()
    logging.info(f"Created empty {graph_type} graph.")

    # Generate collections.
    start = time.time()
    for collection_id in range(1, collection_params.collection_count + 1):
        graph.generate_collection(collection_id)
    logging.info(f"Generated collections in {round(time.time() - start, 1)} seconds.")

    # Generate dataset collections.
    start = time.time()
    for collection_id in graph_connections.dataset_collections_conn_collection:
        for dataset_collection_id in graph_connections.dataset_collections_conn_collection[collection_id]:
            graph.generate_dataset_collection(dataset_collection_id, collection_id)
    logging.info(f"Generate dataset collections in {round(time.time() - start, 1)} seconds.")

    # Generate system collections.
    start = time.time()
    for collection_id in graph_connections.system_collections_conn_collection:
        for system_collection_id in graph_connections.system_collections_conn_collection[collection_id]:
            graph.generate_system_collection(system_collection_id, collection_id)
    logging.info(f"Generate system collections in {round(time.time() - start, 1)} seconds.")

    # Generate datasets.
    start = time.time()
    for dataset_collection_id in graph_connections.datasets_conn_collection:
        for dataset_id in graph_connections.datasets_conn_collection[dataset_collection_id]:
            graph.generate_dataset(dataset_id=dataset_id,
                                   dataset_collection_id=dataset_collection_id,
                                   slo=graph_attributes.dataset_attributes["dataset_slos"][dataset_id - 1],
                                   env=graph_attributes.dataset_attributes["dataset_environments"][dataset_id - 1])
    logging.info(f"Generate datasets in {round(time.time() - start, 1)} seconds.")

    # Generate systems.
    start = time.time()
    for system_collection_id in graph_connections.systems_conn_collection:
        for system_id in graph_connections.systems_conn_collection[system_collection_id]:
            graph.generate_system(system_id=system_id,
                                  system_collection_id=system_collection_id,
                                  system_critic=graph_attributes.system_attributes["system_criticalities"][system_id - 1],
                                  env=graph_attributes.system_attributes["system_environments"][system_id - 1])
    logging.info(f"Generate systems in {round(time.time() - start, 1)} seconds.")

    # Generate data integrity.
    start = time.time()
    for i in range(1, collection_params.dataset_collection_count + 1):
        restoration_time = graph_attributes.data_integrity_attributes["data_restoration_time"][i - 1]
        regeneration_time = graph_attributes.data_integrity_attributes["data_regeneration_time"][i - 1]
        reconstruction_time = graph_attributes.data_integrity_attributes["data_reconstruction_time"][i - 1]
        volatility = graph_attributes.data_integrity_attributes["data_volatility"][i - 1]
        graph.generate_data_integrity(dataset_collection_id=i,
                                      data_integrity_id=i,
                                      data_integrity_rec_time=restoration_time,
                                      data_integrity_reg_time=regeneration_time,
                                      data_integrity_rest_time=reconstruction_time,
                                      data_integrity_volat=volatility)
    logging.info(f"Generate data integrity in {round(time.time() - start, 1)} seconds.")

    # Generate connections between dataset read and system input
    start = time.time()
    processing_id = 1
    for dataset_read in graph_connections.dataset_read_conn_systems:
        for system_input in graph_connections.dataset_read_conn_systems[dataset_read]:
            graph.generate_processing(system_id=system_input,
                                      dataset_id=dataset_read,
                                      processing_id=processing_id,
                                      impact=graph_attributes.dataset_processing_attributes["dataset_impacts"][processing_id - 1],
                                      freshness=graph_attributes.dataset_processing_attributes["dataset_freshness"][processing_id - 1],
                                      inputs=True)
            processing_id += 1
    logging.info(f"Generate dataset read connections in {round(time.time() - start, 1)} seconds.")

    # Generate connections between system output and dataset write.
    start = time.time()
    for dataset_write in graph_connections.dataset_write_conn_systems:
        for system_output in graph_connections.dataset_write_conn_systems[dataset_write]:
            graph.generate_processing(system_id=system_output,
                                      dataset_id=dataset_write,
                                      processing_id=processing_id,
                                      impact=graph_attributes.dataset_processing_attributes["dataset_impacts"][processing_id - 1],
                                      freshness=graph_attributes.dataset_processing_attributes["dataset_freshness"][processing_id - 1],
                                      inputs=False)
            processing_id += 1
    logging.info(f"Generate dataset write connections in {round(time.time() - start, 1)} seconds.")

    # Save graph to file.
    start = time.time()
    graph.save_to_file(output_file, overwrite=overwrite)
    logging.info(f"Finished generation and saved graph to file in {round(time.time() - start, 1)} seconds.")
