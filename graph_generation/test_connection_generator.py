"""
Module to test graph connection generation.
The main goal is to generate similar distributions as an input config.
Distribution comparison is done using Kolmogorov-Smirnov test.

Usage:
    python3 graph_generation/test_connection_generator.py
"""

import unittest
from connection_generator import ConnectionGenerator
from scipy.stats import ks_2samp
from collections import Counter

from config_params.collection_params import CollectionParams
from config_params.dataset_params import DatasetParams
from config_params.dataset_to_system_params import DatasetToSystemParams
from config_params.system_params import SystemParams

import random
random.seed(1)


class TestNxGraph(unittest.TestCase):
    @staticmethod
    def get_dummy_params():
        """Creates empty parameters for connection generator initialization."""
        p1 = DatasetParams(0, {})
        p2 = SystemParams(0, {}, {})
        p3 = DatasetToSystemParams({}, {}, {}, {})
        p4 = CollectionParams({}, {}, 0, {}, 0, {}, 0)
        return p1, p2, p3, p4

    @staticmethod
    def generate_list_by_params(list_len, list_sum):
        """This function generates a random list of positive integers of certain length and sum."""
        random_array = []
        for i in range(list_len):
            # Each element should be at least 1, so we pick random between 1 and a half of list.
            curr_element = random.randint(1, max(list_sum // 2, 1))
            random_array.append(curr_element)
            list_sum -= curr_element

        # Fix the sum of the list if last element was too large.
        random_array[random_array.index(max(random_array))] += list_sum
        return random_array

    def test_get_one_to_many_connections(self):
        """
        Tests if method get_one_to_many_connection generates connections with given distribution.
        To compare config parameter distribuion, and generated distribution Kolmogorov-Smirnov statistic is used.
        """
        # Arrange: generate a random integer list, get a count map and number of elements.
        original_element_count = [random.randint(1, 500) for i in range(10000)]
        count_map = dict(Counter(original_element_count))
        element_count = sum(original_element_count)

        p1, p2, p3, p4 = self.get_dummy_params()
        # Act: Create a sample of the class, and get connections for the generate count map.
        generator = ConnectionGenerator(p1, p2, p3, p4)

        generated_connections = generator.get_one_to_many_connections(element_count, count_map)
        # Calculate number of connections generated.
        generated_element_count = []
        for group in generated_connections:
            generated_element_count.append(len(generated_connections[group]))

        # Assert: Calculate Kolmogorov-Smirnov statistic to compare distributions.
        ks_test = ks_2samp(original_element_count, generated_element_count)

        # Null hypothesis is that two samples are drawn from the same distribution.
        # P value is expected to be almost 1, so we can't reject the H0.
        self.assertTrue(ks_test.pvalue > 0.99)
        self.assertEqual(len(generated_connections), len(original_element_count))

    def test_get_many_to_many_connections(self):
        """
        Tests if method get_many_to_many_connection generates connections with given distributions.
        Kolmogorov-Smirnov tests are applied on both element_1 and element_2 connection distributions.
        """
        # Arrange: Expected values of system / dataset groups from config (zeros omitted).
        element_1_count = 7000
        element_2_count = 20000
        number_of_connections = 30000

        # Generate random lists for connections between element_1 and element_2.
        # Number of connections should be the same (sum of the list), and length correspond to number of elements.
        original_element_1_count = self.generate_list_by_params(element_1_count, number_of_connections)
        element_1_count_map = dict(Counter(original_element_1_count))
        original_element_2_count = self.generate_list_by_params(element_2_count, number_of_connections)
        element_2_count_map = dict(Counter(original_element_2_count))

        # Act: Create an instance of a class, generate many-to-many connections.
        p1, p2, p3, p4 = self.get_dummy_params()
        generator = ConnectionGenerator(p1, p2, p3, p4)

        generated_1_to_2_connections = generator.get_many_to_many_connections(element_1_count, element_2_count,
                                                                              element_1_count_map, element_2_count_map)

        # Calculate connections for the second group (reverse the output dictionary).
        generated_2_to_1_connections = {}
        for group_1 in generated_1_to_2_connections:
            for group_2 in generated_1_to_2_connections[group_1]:
                if group_2 not in generated_2_to_1_connections:
                    generated_2_to_1_connections[group_2] = [group_1]
                else:
                    generated_2_to_1_connections[group_2].append(group_1)

        # Calculate number of connections generated.
        generated_element_2_count = []
        for group in generated_2_to_1_connections:
            generated_element_2_count.append(len(generated_2_to_1_connections[group]))

        generated_element_1_count = []
        for group in generated_1_to_2_connections:
            generated_element_1_count.append(len(generated_1_to_2_connections[group]))

        # Assert: Calculate Kolmogorov-Smirnov statistics for both elements.
        ks_test_1 = ks_2samp(original_element_1_count, generated_element_1_count)
        ks_test_2 = ks_2samp(original_element_2_count, generated_element_2_count)

        self.assertTrue(ks_test_1.pvalue > 0.99)
        self.assertTrue(ks_test_2.pvalue > 0.99)


if __name__ == '__main__':
    unittest.main()
