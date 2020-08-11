from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import logging


class Neo4jGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)

    def close(self):
        self.driver.close()

    def run_query(self, query):
        # Function to allow run any cypher query
        with self.driver.session() as session:
            result = session.write_transaction(self._run_query, query)
            for row in result:
                print(row)

    @staticmethod
    def _run_query(tx, query):
        return tx.run(query)

    def generate_dc(self, dc_id):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_dc, dc_id)
            for dc_id in result:
                print(f"Generated dataset collection {dc_id}")

    @staticmethod
    def _generate_dc(tx, dc_id):
        # Generate a dataset collection node
        query = (
            f'CREATE (dc:dataset_collection {{dc_id: {dc_id}, description: "Dataset collection number {dc_id}"}}) '
            f'RETURN dc'
        )

        result = tx.run(query)
        try:
            return [row["dc"]["dc_id"] for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_sc(self, sc_id):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_sc, sc_id)
            for sc_id in result:
                print(f"Generated system collection {sc_id}")

    @staticmethod
    def _generate_sc(tx, sc_id):
        # Generate system collection node
        query = (
            f'CREATE (sc:system_collection {{sc_id: {sc_id}, description: "System collection number {sc_id}"}}) '
            f'RETURN sc'
        )

        result = tx.run(query)
        try:
            return [row["sc"]["sc_id"] for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_dataset(self, d_id, dc_id, slo):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_dataset, d_id, dc_id, slo)
            for row in result:
                print(f'Generated dataset {row["d"][0]} with regex grouping {row["d"][1]} and dataset collection {row["dc"]}')

    @staticmethod
    def _generate_dataset(tx, d_id, dc_id, slo):
        # Generate a dataset node having a dataset id and dataset collection it belongs to
        query = (
            f'MATCH (dc:dataset_collection) WHERE dc.dc_id = {dc_id} '
            f'CREATE (d:dataset {{d_id: {d_id}, dc_id: {dc_id}, regex_grouping: "data.{d_id}.*", '
            f'dataset_name: "dataset.{d_id}", dataset_description: "Dataset number {d_id}", slo: {slo}}}), '
            f'(dc) -[:CONTAINS] -> (d) '
            f'RETURN dc, d'
        )

        result = tx.run(query)

        try:
            return [{"dc": row["dc"]["dc_id"],
                     "d": (row["d"]["d_id"], row["d"]["regex_grouping"])} for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_system(self, s_id, s_critic, sc_id):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_system, s_id, sc_id, s_critic)
            for row in result:
                print(f'Generated system {row["s"][0]} with regex grouping {row["s"][1]} and system collection {row["sc"]}')

    @staticmethod
    def _generate_system(tx, s_id, sc_id, s_critic):
        # Generate a system node having a system id and system collection it belongs to
        query = (
            f'MATCH (sc:system_collection) WHERE sc.sc_id = {sc_id} '
            f'CREATE (s:system {{s_id: {s_id}, sc_id: {sc_id}, regex_grouping: "system.{s_id}.*", system_name: "system.{s_id}", '
            f'system_description: "System number {s_id}", s_critic: {s_critic}}}), '
            f'(sc) -[:CONTAINS] -> (s) '
            f'RETURN sc, s'
        )

        result = tx.run(query)

        try:
            return [{"sc": row["sc"]["sc_id"],
                     "s": (row["s"]["s_id"], row["s"]["regex_grouping"])} for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_processing(self, s_id, d_id, p_id, impact, freshness):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_processing, s_id, d_id, p_id, impact, freshness)
            for row in result:
                print(f'Generated processing {row["p"]} of dataset {row["d"]} by system {row["s"]}.')

    @staticmethod
    def _generate_processing(tx, s_id, d_id, p_id, impact, freshness, action="INPUTS"):
        # Generate processing node created by system that takes dataset as an input
        # Processing can either input or output the dataset
        query = (
            f'MATCH (s:system), (d:dataset) WHERE s.s_id = {s_id} AND d.d_id = {d_id} '
            f'CREATE (p:processing {{p_id: {p_id}, impact: {impact}, freshness: {freshness}}}), '
            f'(p) -[:{action}] -> (d), (s) - [:CREATES] -> (p) '
            f'RETURN d, s, p '
        )

        result = tx.run(query)

        try:
            return [{"d": row["d"]["d_id"],
                     "s": row["s"]["s_id"],
                     "p": row["p"]["p_id"]} for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_env(self, env_id, env_name, env_owner, env_oncall):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_env, env_id, env_name, env_owner, env_oncall)
            for row in result:
                print(f'Generated environment {row["e"]}.')

    @staticmethod
    def _generate_env(tx, env_id, env_name, env_owner, env_oncall):
        query = (
            f'CREATE (e:environment {{env_id: {env_id}, name: "{env_name}", owner: "{env_owner}", oncall:"{env_oncall}"}}) '
            f'RETURN e'
        )

        result = tx.run(query)

        try:
            return [{"e": row["e"]["env_id"]} for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def generate_data_integrity(self, di_id, d_id, di_rec_time, di_volat, di_reg_time, di_rest_time):
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_data_integrity, di_id, d_id,
                                               di_rec_time, di_volat, di_reg_time, di_rest_time)
            for row in result:
                print(f'Generated data integrity {row["d"]} for dataset {row["di"]}.')

    @staticmethod
    def _generate_data_integrity(tx, di_id, d_id, di_rec_time, di_volat, di_reg_time, di_rest_time):
        query = (
            f'MATCH (d:dataset) WHERE d.d_id = {d_id} '
            f'CREATE (di:data_integrity {{di_id: {di_id}, di_rec_time: {di_rec_time}, '
            f'di_volat: {di_volat}, di_reg_time: {di_reg_time}, di_rest_time: {di_rest_time} }}), '
            f'(d) -[:has] -> (di) '
            f'RETURN d, di '
        )

        result = tx.run(query)

        try:
            return [{"d": row["d"]["d_id"],
                     "di": row["di"]["di_id"]} for row in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
