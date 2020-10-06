"""
Module to test proto graph generation.

Usage:
    python3 graph_generation/test_proto_graph.py
"""

import unittest
from proto_graph import ProtoGraph


class TestProtoGraph(unittest.TestCase):
    def test__get_env_enum(self):
        """Tests if getting correct env enum from string."""
        env_options = ["DEVELOPMENT_ENV", "PERSONAL_ENV", "PRODUCTION_ENV", "STAGING_ENV", "TESTING_ENV", "UNKNOWN_ENV"]
        for index, value in enumerate(env_options):
            self.assertEqual(ProtoGraph._get_env_enum(value), index)
        self.assertEqual(ProtoGraph._get_env_enum("NON_EXIST"), 5)

    def test__get_system_criticality_enum(self):
        """Tests if getting correct system criticality enum from string."""
        criticality_options = ["NOT_CRITICAL", "CRITICAL_CAN_CAUSE_S0_OUTAGE", "CRITICAL_SIGNIFICANT_RUN_RATE",
                               "CRITICAL_OTHER"]
        for index, value in enumerate(criticality_options):
            self.assertEqual(ProtoGraph._get_system_criticality_enum(value), index)
        self.assertEqual(ProtoGraph._get_system_criticality_enum("NON_EXIST"), 3)

    def test__get_processing_impact_enum(self):
        """Tests if getting correct processing impact enum from string."""
        impact_options = ["DOWN", "SEVERELY_DEGRADED", "DEGRADED", "OPPORTUNITY_LOSS", "NONE"]
        for index, value in enumerate(impact_options):
            self.assertEqual(ProtoGraph._get_processing_impact_enum(value), index)
        self.assertEqual(ProtoGraph._get_processing_impact_enum("NON_EXIST"), 4)

    def test__get_processing_freshness_enum(self):
        """Tests if getting correct processing freshness enum from string."""
        freshness_options = ["IMMEDIATE", "DAY", "WEEK", "EVENTUALLY", "NEVER"]
        for index, value in enumerate(freshness_options):
            self.assertEqual(ProtoGraph._get_processing_freshness_enum(value), index)
        self.assertEqual(ProtoGraph._get_processing_freshness_enum("NON_EXIST"), 4)

    def test_env_enum_to_string(self):
        """Tests if env enum is correctly converted to string."""
        env_enum = 0
        self.assertEqual(ProtoGraph.env_enum_to_string(env_enum), "DEVELOPMENT_ENV")

    def test_criticality_enum_to_string(self):
        """Tests if system criticality enum is correctly converted to string."""
        criticality_enum = 0
        self.assertEqual(ProtoGraph.criticality_enum_to_string(criticality_enum), "NOT_CRITICAL")

    def test_processing_impact_enum_to_string(self):
        """Tests if processing impact enum is correctly converted to string."""
        impact_enum = 0
        self.assertEqual(ProtoGraph.processing_impact_enum_to_string(impact_enum), "DOWN")

    def test_processing_freshness_enum_to_string(self):
        """Tests if processing freshness enum is correctly converted to string."""
        freshness_enum = 0
        self.assertEqual(ProtoGraph.processing_freshness_enum_to_string(freshness_enum), "IMMEDIATE")

    def test_generate_collection(self):
        """Tests if collection is added."""
        proto_graph = ProtoGraph()
        proto_graph.generate_collection(5, "collection 5")

        self.assertTrue(hasattr(proto_graph.graph, "collections"))
        self.assertTrue(hasattr(proto_graph.graph.collections[-1], "collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.collections[-1], "name"))

        self.assertEqual(len(proto_graph.graph.collections), 1)
        self.assertEqual(proto_graph.graph.collections[-1].collection_id, 5)
        self.assertEqual(proto_graph.graph.collections[-1].name, "collection 5")

    def test_generate_dataset_collection(self):
        """Tests if dataset collection is added."""
        proto_graph = ProtoGraph()
        proto_graph.generate_dataset_collection(5, 5, "dataset collection 5")

        self.assertTrue(hasattr(proto_graph.graph, "dataset_collections"))
        self.assertTrue(hasattr(proto_graph.graph.dataset_collections[-1], "dataset_collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.dataset_collections[-1], "collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.dataset_collections[-1], "name"))

        self.assertEqual(len(proto_graph.graph.dataset_collections), 1)
        self.assertEqual(proto_graph.graph.dataset_collections[-1].collection_id, 5)
        self.assertEqual(proto_graph.graph.dataset_collections[-1].dataset_collection_id, 5)
        self.assertEqual(proto_graph.graph.dataset_collections[-1].name, "dataset collection 5")

    def test_generate_system_collection(self):
        """Tests if system collection is added."""
        proto_graph = ProtoGraph()
        proto_graph.generate_system_collection(5, 5, "system collection 5")

        self.assertTrue(hasattr(proto_graph.graph, "system_collections"))
        self.assertTrue(hasattr(proto_graph.graph.system_collections[-1], "system_collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.system_collections[-1], "collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.system_collections[-1], "name"))

        self.assertEqual(len(proto_graph.graph.system_collections), 1)
        self.assertEqual(proto_graph.graph.system_collections[-1].collection_id, 5)
        self.assertEqual(proto_graph.graph.system_collections[-1].system_collection_id, 5)
        self.assertEqual(proto_graph.graph.system_collections[-1].name, "system collection 5")

    def test_generate_dataset(self):
        """Tests if dataset is added."""
        env = "DEVELOPMENT_ENV"
        proto_graph = ProtoGraph()
        proto_graph.generate_dataset(5, 5, "dataset.dataset5.*", "dataset 5", "5d", env, "dataset number 5")

        self.assertTrue(hasattr(proto_graph.graph, "datasets"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "dataset_id"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "dataset_collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "regex_grouping"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "name"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "slo"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "env"))
        self.assertTrue(hasattr(proto_graph.graph.datasets[-1], "description"))

        self.assertEqual(len(proto_graph.graph.datasets), 1)
        self.assertEqual(proto_graph.graph.datasets[-1].dataset_id, 5)
        self.assertEqual(proto_graph.graph.datasets[-1].dataset_collection_id, 5)
        self.assertEqual(proto_graph.graph.datasets[-1].regex_grouping, "dataset.dataset5.*")
        self.assertEqual(proto_graph.graph.datasets[-1].name, "dataset 5")
        self.assertEqual(proto_graph.graph.datasets[-1].slo, "5d")
        self.assertEqual(proto_graph.graph.datasets[-1].env, proto_graph._get_env_enum(env))
        self.assertEqual(proto_graph.graph.datasets[-1].description, "dataset number 5")

    def test_generate_system(self):
        """Tests if system is added."""
        env = "TEST_ENV"
        system_criticality = 5
        proto_graph = ProtoGraph()
        proto_graph.generate_system(5, system_criticality, 5, "system.system5.*", "system 5", env, "system number 5")

        self.assertTrue(hasattr(proto_graph.graph, "systems"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "system_id"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "system_collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "system_critic"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "env"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "regex_grouping"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "name"))
        self.assertTrue(hasattr(proto_graph.graph.systems[-1], "description"))

        self.assertEqual(len(proto_graph.graph.systems), 1)
        self.assertEqual(proto_graph.graph.systems[-1].system_id, 5)
        self.assertEqual(proto_graph.graph.systems[-1].system_collection_id, 5)
        self.assertEqual(proto_graph.graph.systems[-1].system_critic, proto_graph._get_system_criticality_enum(system_criticality))
        self.assertEqual(proto_graph.graph.systems[-1].regex_grouping, "system.system5.*")
        self.assertEqual(proto_graph.graph.systems[-1].name, "system 5")
        self.assertEqual(proto_graph.graph.systems[-1].env, proto_graph._get_env_enum(env))
        self.assertEqual(proto_graph.graph.systems[-1].description, "system number 5")

    def test_generate_processing(self):
        """Tests if dataset processing is added."""
        impact = "TEST_ENV"
        freshness = "TEST_FRESH"
        proto_graph = ProtoGraph()
        proto_graph.generate_processing(5, 5, 5, impact, freshness)

        self.assertTrue(hasattr(proto_graph.graph, "processings"))
        self.assertTrue(hasattr(proto_graph.graph.processings[-1], "system_id"))
        self.assertTrue(hasattr(proto_graph.graph.processings[-1], "dataset_id"))
        self.assertTrue(hasattr(proto_graph.graph.processings[-1], "processing_id"))
        self.assertTrue(hasattr(proto_graph.graph.processings[-1], "impact"))
        self.assertTrue(hasattr(proto_graph.graph.processings[-1], "freshness"))

        self.assertEqual(len(proto_graph.graph.processings), 1)
        self.assertEqual(proto_graph.graph.processings[-1].system_id, 5)
        self.assertEqual(proto_graph.graph.processings[-1].dataset_id, 5)
        self.assertEqual(proto_graph.graph.processings[-1].processing_id, 5)
        self.assertEqual(proto_graph.graph.processings[-1].impact, proto_graph._get_processing_impact_enum(impact))
        self.assertEqual(proto_graph.graph.processings[-1].freshness, proto_graph._get_processing_freshness_enum(freshness))

    def test_generate_data_integrity(self):
        """Tests if data integrity is added."""
        proto_graph = ProtoGraph()
        proto_graph.generate_data_integrity(5, 5, "t", True, "t", "t")

        self.assertTrue(hasattr(proto_graph.graph, "data_integrities"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "data_integrity_id"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "dataset_collection_id"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "data_integrity_rec_time"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "data_integrity_volat"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "data_integrity_reg_time"))
        self.assertTrue(hasattr(proto_graph.graph.data_integrities[-1], "data_integrity_rest_time"))

        self.assertEqual(len(proto_graph.graph.data_integrities), 1)
        self.assertEqual(proto_graph.graph.data_integrities[-1].data_integrity_id, 5)
        self.assertEqual(proto_graph.graph.data_integrities[-1].dataset_collection_id, 5)
        self.assertEqual(proto_graph.graph.data_integrities[-1].data_integrity_rec_time, "t")
        self.assertEqual(proto_graph.graph.data_integrities[-1].data_integrity_volat, True)
        self.assertEqual(proto_graph.graph.data_integrities[-1].data_integrity_reg_time, "t")
        self.assertEqual(proto_graph.graph.data_integrities[-1].data_integrity_rest_time, "t")


if __name__ == '__main__':
    unittest.main()
