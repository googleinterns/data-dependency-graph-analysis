from itertools import islice
import random


class ConnectionGenerator:
    def __init__(self, dataset_count, dataset_count_map, system_count, system_count_map, dataset_read_count,
                 system_input_count, dataset_write_count, system_output_count, dataset_read_count_map,
                 dataset_write_count_map, system_input_count_map, system_output_count_map):
        self.dataset_count = dataset_count
        self.dataset_count_map = dataset_count_map

        self.system_count = system_count
        self.system_count_map = system_count_map

        self.dataset_read_count = dataset_read_count
        self.dataset_write_count = dataset_write_count
        self.system_input_count = system_input_count
        self.system_output_count = system_output_count
        self.dataset_read_count_map = dataset_read_count_map
        self.system_input_count_map = system_input_count_map
        self.dataset_write_count_map = dataset_write_count_map
        self.system_output_count_map = system_output_count_map

        self.datasets_conn_collection = None
        self.systems_conn_collection = None
        self.dataset_read_conn_systems = None
        self.dataset_write_conn_systems = None

    @staticmethod
    def get_one_to_many_connections(element_count, element_count_map):
        # Create element ids
        element_values = list(range(1, element_count + 1))

        # Get number of elements in each group
        elements_per_group = [i for i in element_count_map for j in range(element_count_map[i])]

        # Randomise element ids and groups
        random.shuffle(element_values)
        random.shuffle(elements_per_group)

        # Create an iterator
        it = iter(element_values)

        # Split element ids into uneven groups generated before
        group_to_elements = {i + 1: set(islice(it, 0, elements_per_group[i])) for i in range(len(elements_per_group))}

        return group_to_elements

    @staticmethod
    def get_many_to_many_connections(element_1_count, element_2_count, element_1_count_map, element_2_count_map):
        # Count zeros
        element_1_zeros = element_1_count_map[0] if 0 in element_1_count_map else 0
        element_2_zeros = element_2_count_map[0] if 0 in element_2_count_map else 0

        # Create element ids
        element_1_values = list(range(1, element_1_count - element_1_zeros + 1))
        element_2_values = list(range(1, element_2_count - element_2_zeros + 1))

        # Get number of elements in each group and remove groups with 0 elements
        elements_per_group_1 = [i for i in element_1_count_map for j in range(element_1_count_map[i]) if i != 0]
        elements_per_group_2 = [i for i in element_2_count_map for j in range(element_2_count_map[i]) if i != 0]
        element_1_group_counter = {i + 1: elements_per_group_1[i] for i in range(len(elements_per_group_1))}
        element_2_group_counter = {i + 1: elements_per_group_2[i] for i in range(len(elements_per_group_2))}

        # Create connection dictionary
        element_1_conn_element_2 = {i: set() for i in element_1_values}

        # Loop until run out of elements in any group
        while element_1_values and element_2_values:
            # Generate a random connection
            element_1_gen = random.choice(element_1_values)
            element_2_gen = random.choice(element_2_values)

            # Check if connection doesn't exist
            if not element_2_gen in element_1_conn_element_2[element_1_gen]:
                # Add to existing connections and reduce count
                element_1_conn_element_2[element_1_gen].add(element_2_gen)
                element_1_group_counter[element_1_gen] -= 1
                element_2_group_counter[element_2_gen] -= 1

                # If created needed number of connections, remove id from possible options
                if element_1_group_counter[element_1_gen] == 0:
                    element_1_values.remove(element_1_gen)
                if element_2_group_counter[element_2_gen] == 0:
                    element_2_values.remove(element_2_gen)

            # Check if all leftover elements aren't already included
            elif set(element_2_values).issubset(element_1_conn_element_2[element_1_gen]):
                element_1_values.remove(element_1_gen)

        return element_1_conn_element_2

    def dataset_to_dataset_collection(self):
        self.datasets_conn_collection = self.get_one_to_many_connections(self.dataset_count, self.dataset_count_map)

    def system_to_system_collection(self):
        self.systems_conn_collection = self.get_one_to_many_connections(self.system_count, self.system_count_map)

    def dataset_read_to_system_input(self):
        self.dataset_read_conn_systems = self.get_many_to_many_connections(self.dataset_read_count,
                                                                           self.system_input_count,
                                                                           self.dataset_read_count_map,
                                                                           self.system_input_count_map)

    def dataset_write_to_system_output(self):
        self.dataset_write_conn_systems = self.get_many_to_many_connections(self.dataset_write_count,
                                                                            self.system_output_count,
                                                                            self.dataset_write_count_map,
                                                                            self.system_output_count_map)

    def generate(self):
        self.dataset_to_dataset_collection()
        self.system_to_system_collection()
        self.dataset_read_to_system_input()
        self.dataset_write_to_system_output()
