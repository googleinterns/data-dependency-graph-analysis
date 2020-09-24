class DatasetToSystemParams:
    def __init__(self, system_input_count_map, system_output_count_map, dataset_read_count_map, dataset_write_count_map):
        self.dataset_read_count = sum(dataset_read_count_map.values())
        self.dataset_write_count = sum(dataset_write_count_map.values())
        self.system_input_count_map = system_input_count_map
        self.system_output_count_map = system_output_count_map
        self.system_input_count = sum(system_input_count_map.values())
        self.system_output_count = sum(system_output_count_map.values())
        self.dataset_read_count_map = dataset_read_count_map
        self.dataset_write_count_map = dataset_write_count_map
