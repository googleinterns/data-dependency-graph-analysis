"""
Module to test graph attribute generation.

Usage:
    python3 graph_generation/test_attribute_generator.py
"""

import unittest
from attribute_generator import AttributeGenerator

from config_params.collection_params import CollectionParams
from config_params.data_integrity_params import DataIntegrityParams
from config_params.dataset_params import DatasetParams
from config_params.dataset_to_system_params import DatasetToSystemParams
from config_params.processing_params import ProcessingParams
from config_params.system_params import SystemParams
from config_params.connection_params import ConnectionParams


class TestAttributeGenerator(unittest.TestCase):
    @staticmethod
    def get_dummy_params():
        """Creates empty parameters for attribute generator initialization."""
        dataset_params = DatasetParams(0, {})
        system_params = SystemParams(0, {}, {})
        dataset_to_system_params = DatasetToSystemParams({}, {}, {}, {})
        collection_params = CollectionParams({}, {}, 0, {}, 0, {}, 0)
        data_integrity_params = DataIntegrityParams({})
        processing_params = ProcessingParams({}, {})
        connection_params = ConnectionParams(0, 0)
        return collection_params, dataset_params, system_params, data_integrity_params, processing_params, connection_params

    def test__generate_time(self):
        """Tests if random generated time is within allowed range."""
        p1, p2, p3, p4, p5, p6 = self.get_dummy_params()
        generator = AttributeGenerator(p1, p2, p3, p4, p5, p6)
        random_time = generator._generate_time()[0]
        allowed_range = {"d": (1, 30), "h": (1, 120), "m": (1, 720), "s": (1, 360)}
        time_value = random_time[:-1]
        time_type = random_time[-1]
        self.assertTrue(time_type in allowed_range.keys())
        self.assertTrue(int(time_value) in range(allowed_range[time_type][0], allowed_range[time_type][1]))

    def test_generate_from_proba(self):
        """Tests random element generation from probability map."""
        p1, p2, p3, p4, p5, p6 = self.get_dummy_params()
        generator = AttributeGenerator(p1, p2, p3, p4, p5, p6)
        element_proba_map = {1: 0.2, 2: 0.2, 3:0.15, 4:0.15, 100: 0.3}
        random_values_from_map = generator._generate_from_proba(element_proba_map, n=5)
        self.assertEqual(len(random_values_from_map), 5)
        for n in random_values_from_map:
            self.assertTrue(n in element_proba_map)

    def test__generate_description(self):
        """Tests node description generation."""
        p1, p2, p3, p4, p5, p6 = self.get_dummy_params()
        generator = AttributeGenerator(p1, p2, p3, p4, p5, p6)
        description = generator._generate_description("dataset", 5)
        self.assertEqual(description, "Dataset number 5.")

    def test__generate_regex(self):
        """Tests element regex grouping generation."""
        p1, p2, p3, p4, p5, p6 = self.get_dummy_params()
        generator = AttributeGenerator(p1, p2, p3, p4, p5, p6)
        regex_grouping = generator._generate_regex("dataset", 5)
        self.assertEqual(regex_grouping, "dataset.5.*")

    def test__generate_name(self):
        """Tests node name generation."""
        p1, p2, p3, p4, p5, p6 = self.get_dummy_params()
        generator = AttributeGenerator(p1, p2, p3, p4, p5, p6)
        name = generator._generate_name("dataset", 5)
        self.assertEqual(name, "dataset.5")

if __name__ == '__main__':
    unittest.main()
