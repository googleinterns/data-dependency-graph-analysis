"""
This module implements methods for generating random connections between nodes in a graph.

Method generate() will create all the necessary connections for the graph:
    dataset <-> dataset collection
    system <-> system collection
    dataset collection <-> collection
    system collection <-> collection
    dataset read <-> system input
    dataset write <-> system output
"""

from itertools import islice
import random


class ConnectionGenerator:
    """
    A class to generate random connections between node ids, based on distribution maps.

    ...

    Attributes:
        dataset_count: Integer of how many datasets are in a graph.
        dataset_count_map: Dictionary int:int that maps number of datasets in collection to count of its collections.
        system_count: Integer of how many systems are in a graph.
        system_count_map: Dictionary int:int that maps number of systems in collection to count of system collections.
        dataset_read_count: Integer of how many dataset reads are in a graph.
        dataset_write_count: Integer of how many dataset writes are in a graph.
        system_input_count: Integer of how many system inputs are in a graph.
        system_output_count: Integer of how many system outputs are in a graph.
        dataset_read_count_map: Dictionary int:int that maps number of system inputs of dataset read to count of
                                dataset reads.
        system_input_count_map: Dictionary int:int that maps number of dataset reads by system input to count of
                                system inputs.
        dataset_write_count_map: Dictionary int:int that maps number of system outputs of dataset write to count of
                                 dataset writes.
        system_output_count_map: Dictionary int:int that maps number of dataset writes by system output to count of
                                 system outputs.
        dataset_collections_conn_collection: Dictionary int:[int] that maps collection id to dataset collection ids.
        system_collections_conn_collection: Dictionary int:[int] that maps collection id to system collection ids.
        datasets_conn_collection: Dictionary int:[int] that maps dataset collection id to dataset ids.
        systems_conn_collection: Dictionary int:[int] that maps system collection id to system ids.
        dataset_read_conn_systems: Dictionary int:[int] that maps dataset read id to system ids this dataset inputs to.
        dataset_write_conn_systems: Dictionary int:[int] that maps dataset write id to system ids this dataset outputs from.

    Methods:
        get_one_to_many_connections()
            Creates connections between an element and a group. Each element belongs to one group exactly.

        get_many_to_many_connections()
            Creates connections between two groups with many to many relationship.

        _dataset_to_dataset_collection()
            Generates dataset - dataset collection connections.

        _system_to_system_collection()
            Generates system - system collection connections.

        _dataset_read_to_system_input()
            Generates connections between dataset reads and system inputs.

        _dataset_write_to_system_output()
            Generates connections between dataset write and system outputs.

        generate()
            Generates all the needed connections for data dependency mapping graph.
    """
    def __init__(self, dataset_params, system_params, dataset_to_system_params, collection_params):
        """
        Args:
             dataset_params: DatasetParams object.
             system_params: SystemParams object.
             dataset_to_system_params: DatasetToSystemParams object.
             collection_params: CollectionParams object.
        """
        self.dataset_count = dataset_params.dataset_count
        self.dataset_count_map = collection_params.dataset_count_map
        self.dataset_collection_count = collection_params.dataset_collection_count
        self.dataset_collection_count_map = collection_params.dataset_collection_count_map

        self.system_count = system_params.system_count
        self.system_count_map = collection_params.system_count_map
        self.system_collection_count = collection_params.system_collection_count
        self.system_collection_count_map = collection_params.system_collection_count_map

        self.dataset_read_count = dataset_to_system_params.dataset_read_count
        self.dataset_write_count = dataset_to_system_params.dataset_write_count
        self.system_input_count = dataset_to_system_params.system_input_count
        self.system_output_count = dataset_to_system_params.system_output_count
        self.dataset_read_count_map = dataset_to_system_params.dataset_read_count_map
        self.system_input_count_map = dataset_to_system_params.system_input_count_map
        self.dataset_write_count_map = dataset_to_system_params.dataset_write_count_map
        self.system_output_count_map = dataset_to_system_params.system_output_count_map

        self.dataset_collections_conn_collection = {}
        self.system_collections_conn_collection = {}
        self.datasets_conn_collection = {}
        self.systems_conn_collection = {}
        self.dataset_read_conn_systems = {}
        self.dataset_write_conn_systems = {}

    @staticmethod
    def get_one_to_many_connections(element_count, element_count_map):
        """Generate group id for each element, based on number of element in group distribution.

        Args:
            element_count: Total number of elements.
            element_count_map: Dictionary int:int that maps element count in a group to number of groups with that count.

        Returns:
            Dictionary int:[int] that maps group id to a list of element ids.
        """
        # Create element ids.
        element_values = list(range(1, element_count + 1))

        # Get number of elements for each group id from their count.

        elements_per_group = [i for i in element_count_map for _ in range(element_count_map[i])]

        # Randomise element ids and group ids.
        random.shuffle(element_values)
        random.shuffle(elements_per_group)

        # Split element ids into chunks to get connections for each group.
        group_to_elements = {}

        last_index = 0
        for i in range(len(elements_per_group)):
            group_to_elements[i + 1] = element_values[last_index:last_index + elements_per_group[i]]
            last_index += elements_per_group[i]

        # In case we don't have a full config - assign rest of elements to a last group.
        if last_index != element_count - 1:
            group_to_elements[len(elements_per_group)] += element_values[last_index:]

        return group_to_elements

    @staticmethod
    def get_many_to_many_connections(element_1_count, element_2_count, element_1_count_map, element_2_count_map):
        """Generates random connections between elements of type 1 and type 2 that have many-to-many relationship.
        Generation is based on element count maps. The output distribution is expected to be exact for most counts,
        except for large element group outliers.

        Args:
            element_1_count: Total number of elements of type 1.
            element_2_count: Total number of elements of type 2.
            element_1_count_map: Dictionary int:int that maps element 1 count in element 2 group to number of elements 2.
            element_2_count_map: Dictionary int:int that maps element 2 count in element 1 group to number of elements 1.

        Returns:
            Dictionary that maps group 1 id to a list of group 2 ids.
        """
        # Count zeros for each group.
        element_1_zeros = element_1_count_map[0] if 0 in element_1_count_map else 0
        element_2_zeros = element_2_count_map[0] if 0 in element_2_count_map else 0

        # Create element ids.
        element_1_values = list(range(1, element_1_count - element_1_zeros + 1))
        element_2_values = list(range(1, element_2_count - element_2_zeros + 1))

        # Get number of elements in each group and remove groups with 0 elements.
        elements_per_group_1 = [i for i in element_1_count_map for j in range(element_1_count_map[i]) if i != 0]
        elements_per_group_2 = [i for i in element_2_count_map for j in range(element_2_count_map[i]) if i != 0]
        element_1_group_counter = {i + 1: elements_per_group_1[i] for i in range(len(elements_per_group_1))}
        element_2_group_counter = {i + 1: elements_per_group_2[i] for i in range(len(elements_per_group_2))}

        # Create connection dictionary.
        element_1_conn_element_2 = {i: set() for i in element_1_values}

        # Loop until any group runs out of elements.
        while element_1_values and element_2_values:
            # Generate a random connection
            element_1_gen = random.choice(element_1_values)
            element_2_gen = random.choice(element_2_values)

            # Check if connection doesn't already exist.
            if not element_2_gen in element_1_conn_element_2[element_1_gen]:
                # Add to existing connections and reduce count.
                element_1_conn_element_2[element_1_gen].add(element_2_gen)
                element_1_group_counter[element_1_gen] -= 1
                element_2_group_counter[element_2_gen] -= 1

                # If have all needed number of connections, remove id from possible options.
                if element_1_group_counter[element_1_gen] == 0:
                    element_1_values.remove(element_1_gen)
                if element_2_group_counter[element_2_gen] == 0:
                    element_2_values.remove(element_2_gen)

            # Check if all leftover elements aren't already included in this group.
            elif set(element_2_values).issubset(element_1_conn_element_2[element_1_gen]):
                element_1_values.remove(element_1_gen)

        return element_1_conn_element_2

    def _system_collection_to_collection(self):
        """Generates collection - system collection one to many connections."""
        self.system_collections_conn_collection = self.get_one_to_many_connections(self.system_collection_count,
                                                                                   self.system_collection_count_map)

    def _dataset_collection_to_collection(self):
        """Generates collection - dataset collection one to many connections."""
        self.dataset_collections_conn_collection = self.get_one_to_many_connections(self.dataset_collection_count,
                                                                                    self.dataset_collection_count_map)

    def _dataset_to_dataset_collection(self):
        """Generates dataset collection - dataset one to many connections."""
        self.datasets_conn_collection = self.get_one_to_many_connections(self.dataset_count, self.dataset_count_map)

    def _system_to_system_collection(self):
        """Generates system collection - system one to many connections."""
        self.systems_conn_collection = self.get_one_to_many_connections(self.system_count, self.system_count_map)

    def _dataset_read_to_system_input(self):
        """Generates dataset reads and system inputs many to many connections."""
        self.dataset_read_conn_systems = self.get_many_to_many_connections(self.dataset_read_count,
                                                                           self.system_input_count,
                                                                           self.dataset_read_count_map,
                                                                           self.system_input_count_map)

    def _dataset_write_to_system_output(self):
        """Generates dataset write and system outputs many to many connections."""
        self.dataset_write_conn_systems = self.get_many_to_many_connections(self.dataset_write_count,
                                                                            self.system_output_count,
                                                                            self.dataset_write_count_map,
                                                                            self.system_output_count_map)

    def generate(self):
        """Generate all connections for a graph."""
        self._dataset_collection_to_collection()
        self._system_collection_to_collection()
        self._dataset_to_dataset_collection()
        self._system_to_system_collection()
        self._dataset_read_to_system_input()
        self._dataset_write_to_system_output()
