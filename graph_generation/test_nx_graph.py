"""
Module to test networkx graph generation.

Usage:
    python3 graph_generation/test_nx_graph.py
"""

import unittest
from nx_graph import NxGraph


class TestNxGraph(unittest.TestCase):
    def test_generate_collection(self):
        """Tests if collection is added."""
        graph = NxGraph()
        graph.generate_collection(1, "collection 1")
        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "collection_1")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("node_name" in node_attributes)
        self.assertTrue("type" in node_attributes)
        self.assertEqual(node_attributes["id"], 1)
        self.assertEqual(node_attributes["node_name"], "collection 1")
        self.assertEqual(node_attributes["type"], "collection")

    def test_generate_dataset_collection(self):
        """Tests if dataset collection node, collection - dataset collection edge are added."""
        graph = NxGraph()
        graph.generate_dataset_collection(5, 5, "dataset collection 5")

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "dataset_collection_5")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("collection_id" in node_attributes)
        self.assertTrue("node_name" in node_attributes)
        self.assertTrue("type" in node_attributes)
        self.assertEqual(node_attributes["id"], 5)
        self.assertEqual(node_attributes["collection_id"], 5)
        self.assertEqual(node_attributes["node_name"], "dataset collection 5")
        self.assertEqual(node_attributes["type"], "dataset_collection")

        self.assertEqual(len(graph.edges["dataset_collection_to_collection"]), 1)
        self.assertEqual(len(graph.edges["dataset_collection_to_collection"][0]), 2)
        self.assertEqual(graph.edges["dataset_collection_to_collection"][0][0], "collection_5")
        self.assertEqual(graph.edges["dataset_collection_to_collection"][0][1], "dataset_collection_5")

    def test_generate_system_collection(self):
        """Tests if system collection node, collection - system collection edge are added."""
        graph = NxGraph()
        graph.generate_system_collection(5, 5, "system collection 5")

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "system_collection_5")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("collection_id" in node_attributes)
        self.assertTrue("node_name" in node_attributes)
        self.assertTrue("type" in node_attributes)
        self.assertEqual(node_attributes["id"], 5)
        self.assertEqual(node_attributes["collection_id"], 5)
        self.assertEqual(node_attributes["node_name"], "system collection 5")
        self.assertEqual(node_attributes["type"], "system_collection")

        self.assertEqual(len(graph.edges["system_collection_to_collection"]), 1)
        self.assertEqual(len(graph.edges["system_collection_to_collection"][0]), 2)
        self.assertEqual(graph.edges["system_collection_to_collection"][0][0], "collection_5")
        self.assertEqual(graph.edges["system_collection_to_collection"][0][1], "system_collection_5")

    def test_generate_dataset(self):
        """Tests if dataset node, dataset collection - dataset edge are added."""
        graph = NxGraph()
        graph.generate_dataset(5, 5, "dataset.*", "dataset 5", "3d", "PRODUCTION_ENV", "Dataset number 5")

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "dataset_5")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("dataset_collection_id" in node_attributes)
        self.assertTrue("regex_grouping" in node_attributes)
        self.assertTrue("node_name" in node_attributes)
        self.assertTrue("slo" in node_attributes)
        self.assertTrue("env" in node_attributes)
        self.assertTrue("description" in node_attributes)
        self.assertTrue("type" in node_attributes)

        self.assertEqual(node_attributes["id"], 5)
        self.assertEqual(node_attributes["dataset_collection_id"], 5)
        self.assertEqual(node_attributes["regex_grouping"], "dataset.*")
        self.assertEqual(node_attributes["node_name"], "dataset 5")
        self.assertEqual(node_attributes["slo"], "3d")
        self.assertEqual(node_attributes["env"], "PRODUCTION_ENV")
        self.assertEqual(node_attributes["description"], "Dataset number 5")
        self.assertEqual(node_attributes["type"], "dataset")

        self.assertEqual(len(graph.edges["dataset_to_dataset_collection"]), 1)
        self.assertEqual(len(graph.edges["dataset_to_dataset_collection"][0]), 2)
        self.assertEqual(graph.edges["dataset_to_dataset_collection"][0][0], "dataset_collection_5")
        self.assertEqual(graph.edges["dataset_to_dataset_collection"][0][1], "dataset_5")

    def test_generate_system(self):
        """Tests if system node, system collection - system edge are added."""
        graph = NxGraph()
        graph.generate_system(5, "NOT_CRITICAL", 5, "system.*", "system 5", "PRODUCTION_ENV", "System number 5")

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "system_5")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("system_collection_id" in node_attributes)
        self.assertTrue("regex_grouping" in node_attributes)
        self.assertTrue("node_name" in node_attributes)
        self.assertTrue("system_critic" in node_attributes)
        self.assertTrue("env" in node_attributes)
        self.assertTrue("description" in node_attributes)
        self.assertTrue("type" in node_attributes)

        self.assertEqual(node_attributes["id"], 5)
        self.assertEqual(node_attributes["system_collection_id"], 5)
        self.assertEqual(node_attributes["regex_grouping"], "system.*")
        self.assertEqual(node_attributes["node_name"], "system 5")
        self.assertEqual(node_attributes["system_critic"], "NOT_CRITICAL")
        self.assertEqual(node_attributes["env"], "PRODUCTION_ENV")
        self.assertEqual(node_attributes["description"], "System number 5")
        self.assertEqual(node_attributes["type"], "system")

        self.assertEqual(len(graph.edges["system_to_system_collection"]), 1)
        self.assertEqual(len(graph.edges["system_to_system_collection"][0]), 2)
        self.assertEqual(graph.edges["system_to_system_collection"][0][0], "system_collection_5")
        self.assertEqual(graph.edges["system_to_system_collection"][0][1], "system_5")

    def test_generate_processing(self):
        """Tests if dataset processing node, processing - system and processing - dataset edges are added."""
        graph = NxGraph()
        graph.generate_processing(1, 2, 3, "DOWN", "DAY", inputs=False)

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "processing_3")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("impact" in node_attributes)
        self.assertTrue("freshness" in node_attributes)
        self.assertTrue("type" in node_attributes)

        self.assertEqual(node_attributes["id"], 3)
        self.assertEqual(node_attributes["impact"], "DOWN")
        self.assertEqual(node_attributes["freshness"], "DAY")
        self.assertEqual(node_attributes["type"], "processing")

        self.assertEqual(len(graph.edges["dataset_to_system_output"]), 2)
        self.assertEqual(len(graph.edges["dataset_to_system_output"][0]), 2)
        self.assertEqual(len(graph.edges["dataset_to_system_output"][1]), 2)
        self.assertEqual(graph.edges["dataset_to_system_output"][0][0], "processing_3")
        self.assertEqual(graph.edges["dataset_to_system_output"][0][1], "dataset_2")
        self.assertEqual(graph.edges["dataset_to_system_output"][1][0], "system_1")
        self.assertEqual(graph.edges["dataset_to_system_output"][1][1], "processing_3")

        graph.generate_processing(1, 2, 3, "DOWN", "DAY", inputs=True)
        self.assertEqual(len(graph.edges["dataset_to_system_input"]), 2)
        self.assertEqual(len(graph.edges["dataset_to_system_input"][0]), 2)
        self.assertEqual(len(graph.edges["dataset_to_system_input"][1]), 2)
        self.assertEqual(graph.edges["dataset_to_system_input"][0][0], "dataset_2")
        self.assertEqual(graph.edges["dataset_to_system_input"][0][1], "processing_3")
        self.assertEqual(graph.edges["dataset_to_system_input"][1][0], "processing_3")
        self.assertEqual(graph.edges["dataset_to_system_input"][1][1], "system_1")

    def test_generate_data_integrity(self):
        """Tests if data integrity node and data integrity -> dataset collection edge are added."""
        graph = NxGraph()
        graph.generate_data_integrity(1, 2, "1d", True, "2m", "3s")

        self.assertEqual(len(graph.nodes), 1)
        self.assertEqual(len(graph.nodes[0]), 2)

        node_name = graph.nodes[0][0]
        node_attributes = graph.nodes[0][1]

        self.assertEqual(node_name, "data_integrity_1")
        self.assertTrue("id" in node_attributes)
        self.assertTrue("data_integrity_rec_time" in node_attributes)
        self.assertTrue("data_integrity_volat" in node_attributes)
        self.assertTrue("data_integrity_rest_time" in node_attributes)
        self.assertTrue("data_integrity_reg_time" in node_attributes)
        self.assertTrue("type" in node_attributes)

        self.assertEqual(node_attributes["id"], 1)
        self.assertEqual(node_attributes["data_integrity_rec_time"], "1d")
        self.assertEqual(node_attributes["data_integrity_volat"], True)
        self.assertEqual(node_attributes["data_integrity_rest_time"], "3s")
        self.assertEqual(node_attributes["data_integrity_reg_time"], "2m")
        self.assertEqual(node_attributes["type"], "data_integrity")

        self.assertEqual(len(graph.edges["data_integrity_to_dataset_collection"]), 1)
        self.assertEqual(len(graph.edges["data_integrity_to_dataset_collection"][0]), 2)
        self.assertEqual(graph.edges["data_integrity_to_dataset_collection"][0][0], "dataset_collection_2")
        self.assertEqual(graph.edges["data_integrity_to_dataset_collection"][0][1], "data_integrity_1")


if __name__ == '__main__':
    unittest.main()
