"""
This module generates a data-dependancy mapping graph based on proto file and saves it to networkx graph.

Usage:
    python3 graph_generation/proto_to_nx.py \
         --proto_file "proto.bin" \
         --nx_file "nx.graphml" \
         --overwrite

    Parameters info:
        Input proto file has .bin extension and output nx file has .graphml extension
        overwrite if not specified equals to False. If it is used (ex. above) - it will overwrite the existing graph.
"""

import argparse
import logging
import time

from proto_graph import ProtoGraph
from nx_graph import NxGraph


def parse_args():
    """Parses input arguments."""
    parser = argparse.ArgumentParser(description='Convert proto graph to netwrokx graph.')
    parser.add_argument('-p', '--proto_file', help='Path to an input proto binary.', required=True)
    parser.add_argument('-n', '--nx_file', help='Path to an output for networkx graph.', required=True)
    parser.add_argument('-o', '--overwrite', help='If output file exists, overwrite it.', type=bool, default=False)
    return parser.parse_args()


def convert_proto_to_nx_graph(proto_graph, nx_graph, nx_file, overwrite):
    """Parses all nodes in proto graph, converts enums to strings, creates networkx graph and saves it."""
    for collection in proto_graph.graph.collections:
        nx_graph.generate_collection(collection.collection_id, collection.name)
    logging.info(f"Added all collection nodes from proto to nx.")

    for dataset_collection in proto_graph.graph.dataset_collections:
        nx_graph.generate_dataset_collection(dataset_collection.dataset_collection_id,
                                             dataset_collection.collection_id,
                                             dataset_collection.name)
    logging.info(f"Added all dataset collection nodes from proto to nx.")

    for system_collection in proto_graph.graph.system_collections:
        nx_graph.generate_system_collection(system_collection.system_collection_id,
                                            system_collection.collection_id,
                                            system_collection.name)
    logging.info(f"Added all system collection nodes from proto to nx.")

    for dataset in proto_graph.graph.datasets:
        dataset_env = proto_graph.env_enum_to_string(dataset.env)
        nx_graph.generate_dataset(dataset.dataset_id, dataset.dataset_collection_id, dataset.regex_grouping,
                                  dataset.name, dataset.slo, dataset_env, dataset.description)
    logging.info(f"Added all dataset nodes from proto to nx.")

    for system in proto_graph.graph.systems:
        system_env = proto_graph.env_enum_to_string(system.env)
        criticality = proto_graph.criticality_enum_to_string(system.system_critic)
        nx_graph.generate_system(system.system_id, criticality, system.system_collection_id, system.regex_grouping,
                                 system.name, system_env, system.description)
    logging.info(f"Added all system nodes from proto to nx.")

    for processing in proto_graph.graph.processings:
        impact = proto_graph.processing_impact_enum_to_string(processing.impact)
        freshness = proto_graph.processing_freshness_enum_to_string(processing.freshness)
        nx_graph.generate_processing(processing.system_id, processing.dataset_id, processing.processing_id,
                                     impact, freshness, inputs=processing.inputs)
    logging.info(f"Added all processing nodes from proto to nx.")

    for data_integrity in proto_graph.graph.data_integrities:
        nx_graph.generate_data_integrity(data_integrity.data_integrity_id, data_integrity.dataset_collection_id,
                                         data_integrity.data_integrity_rec_time, data_integrity.data_integrity_volat,
                                         data_integrity.data_integrity_reg_time, data_integrity.data_integrity_rest_time)
    logging.info(f"Added all data integrity nodes from proto to nx.")

    # Save graph to file.
    start = time.time()
    nx_graph.save_to_file(nx_file, overwrite=overwrite)
    logging.info(f"Finished generation and saved nx graph to file in {round(time.time() - start, 1)} seconds.")


if __name__ == "__main__":
    # Parse arguments.
    args = parse_args()
    proto_file = args.proto_file
    nx_file = args.nx_file
    overwrite = args.overwrite

    # Read proto graph from file.
    proto_graph = ProtoGraph()
    proto_graph.read_from_file(proto_file)

    # Create an empty networkx graph.
    nx_graph = NxGraph()

    # Convert proto to nx and save it to output file.
    convert_proto_to_nx_graph(proto_graph, nx_graph, nx_file, overwrite)
