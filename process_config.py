import yaml
from connection_generator import ConnectionGenerator
from attribute_generator import AttributeGenerator
from neo4jgraph import Neo4jGraph


def process_map(config_map, proba=False, enum=False):
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
    data_volatility_proba_map = process_map(config["data_integrity"]["volatality_proba_map"], proba=True)

    graph_connections = ConnectionGenerator(dataset_count=dataset_count,
                                            dataset_count_map=datasets_in_collection_count_map,
                                            system_count=system_count,
                                            system_count_map=systems_in_collection_count_map,
                                            dataset_read_count=dataset_read_count,
                                            system_input_count=system_input_count,
                                            dataset_write_count=dataset_write_count,
                                            system_output_count=system_output_count,
                                            dataset_read_count_map=dataset_read_count_map,
                                            system_input_count_map=system_input_count_map,
                                            dataset_write_count_map=dataset_write_count_map,
                                            system_output_count_map=system_output_count_map)

    graph_connections.generate()
    system_write_conn_count = sum([len(i) for i in graph_connections.dataset_write_conn_systems.values()])
    system_read_conn_count = sum([len(i) for i in graph_connections.dataset_read_conn_systems.values()])
    dataset_system_connection_count = system_write_conn_count + system_read_conn_count

    graph_attributes = AttributeGenerator(
        dataset_count,
        system_count,
        dataset_system_connection_count,
        env_type_count_map,
        dataset_slo_range_seconds,
        data_restoration_range_seconds,
        data_regeneration_range_seconds,
        data_reconstruction_range_seconds,
        data_volatility_proba_map,
        dataset_impact_proba_map,
        dataset_criticality_proba_map,
        system_criticality_proba_map)

    graph_attributes.generate()

    graph = Neo4jGraph("bolt://0.0.0.0", "neo4j", "password")

    # Generate dataset collections
    for i in range(1, dataset_collection_count + 1):
        graph.generate_dataset_collection(i)

    # Generate system collection
    for i in range(1, system_collection_count + 1):
        graph.generate_system_collection(i)

    # Generate datasets
    for i in range(1, dataset_count + 1):
        graph.generate_dataset(dataset_id=i,
                               dataset_collection_id=graph_connections.datasets_conn_collection[i],
                               slo=graph_attributes.dataset_attributes["dataset_slos"][i - 1],
                               env=graph_attributes.dataset_attributes["dataset_environments"][i - 1])

    # Generate systems
    for i in range(1, system_count + 1):
        graph.generate_system(system_id=i,
                              system_collection_id=graph_connections.systems_conn_collection[i],
                              system_critic=graph_attributes.system_attributes["system_criticalities"][i - 1],
                              env=graph_attributes.system_attributes["system_environments"][i - 1])

    # Generate data integrity
    for i in range(1, dataset_count + 1):
        restoration_time = graph_attributes.data_integrity_attributes["data_restoration_time"][i - 1]
        regeneration_time = graph_attributes.data_integrity_attributes["data_regeneration_time"][i - 1]
        reconstruction_time = graph_attributes.data_integrity_attributes["data_reconstruction_time"][i - 1]
        volatility = graph_attributes.data_integrity_attributes["data_volatility"][i - 1]
        graph.generate_data_integrity(dataset_id=i,
                                      data_integrity_id=i,
                                      data_integrity_rec_time=restoration_time,
                                      data_integrity_reg_time=regeneration_time,
                                      data_integrity_rest_time=reconstruction_time,
                                      data_integrity_volat=volatility)

    # Generate connections between dataset read and system input
    processing_id = 1
    for dataset_read in graph_connections.dataset_read_conn_systems:
        for system_input in graph_connections.dataset_read_conn_systems[dataset_read]:
            graph.generate_processing(system_id=system_input,
                                      dataset_id=dataset_read,
                                      processing_id=processing_id,
                                      impact=graph_attributes.dataset_processing_attributes["dataset_impacts"][processing_id - 1],
                                      freshness=graph_attributes.dataset_processing_attributes["dataset_freshness"][processing_id - 1])
            processing_id += 1

    # Generate connections between system output and dataset write
    for dataset_write in graph_connections.dataset_write_conn_systems:
        for system_output in graph_connections.dataset_write_conn_systems[dataset_write]:
            graph.generate_processing(system_id=system_output,
                                      dataset_id=dataset_write,
                                      processing_id=processing_id,
                                      impact=graph_attributes.dataset_processing_attributes["dataset_impacts"][processing_id - 1],
                                      freshness=graph_attributes.dataset_processing_attributes["dataset_freshness"][processing_id - 1],
                                      action="OUTPUTS")
            processing_id += 1
