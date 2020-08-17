"""
This module implements methods for data dependency mapping graph creation
in a neo4j database.

It allows to create nodes of type:
    dataset collection
    system collection
    dataset
    system
    environment
    dataset processing
    data integrity

While creating the nodes, the following two way connections will be created:
    dataset - dataset collection
    system - system collection
    system - dataset processing
    dataset processing - dataset
    dataset integrity - dataset

To use this module, neo4j server has to be started and running.
Neo4j Docker image can be used to set it up and can be found here: https://hub.docker.com/_/neo4j.
"""

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import logging


class Neo4jGraph:
    """
    A class to handle cypher queries and create nodes and connections in neo4j graph.

    ...

    Attributes
    ----------
    driver : GraphDatabase.driver object
        Establishes connections with neo4j database, including server URIs, credentials and other configuration.

    Methods
    -------
    close()
        Closes the neo4j driver.

    generate_dataset_collection(dataset_collection_id)
        Generates a dataset collection with a given id.

    generate_system_collection(system_collection_id)
        Generates a system collection with a given id.

    generate_dataset(dataset_id, dataset_collection_id, slo)
        Generates a dataset with a given id, creates a connection to its dataset collection.

    generate_system(system_id, system_collection_id, system_critic)
        Generates a system with a given id, creates a connection to its system collection.

    generate_processing(system_id, dataset_id, processing_id, impact, freshness, action="INPUTS")
        Generates a processing node, that represents dataset - system relationship.
        This method also creates connections: dataset - processing, system - processing.
        Action parameter denotes if the dataset is an input to the system, or an output.

    generate_env(env_id, env_name, env_owner, env_oncall)
        Generates an environment with a given id and attributes.

    generate_data_integrity(data_integrity_id, dataset_id, data_integrity_rec_time, data_integrity_volat,
                            data_integrity_reg_time, data_integrity_rest_time)
        Generates a data integrity node, that corresponds to a specific dataset, having the attributes.

    """
    def __init__(self, uri, user, password):
        """
        Parameters
        ----------
        uri : str
            The connection URI for the driver.
        user : str
            Username for authentication.
        password : str
            Password for authentication.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)

    def close(self):
        """Closes the driver object."""
        self.driver.close()

    def generate_dataset_collection(self, dataset_collection_id):
        """Generates a dataset collection node with id and description based on the id.
        Creating a dataset collection node with id 1 will create a node with these attributes:
            dataset_collection_id: 1,
            description: Dataset collection number 1.

        Parameters
        ----------
        dataset_collection_id : int
            Dataset collection id, is unique.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                generated_id = session.write_transaction(self._generate_dataset_collection, dataset_collection_id)
                logging.info(f"Generated dataset collection {generated_id}.")

            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_dataset_collection(tx, dataset_collection_id):
        """Creates and runs a query for dataset collection creation."""
        query = (
            f'MERGE (dataset_collection:dataset_collection {{dataset_collection_id: {dataset_collection_id}, '
            f'description: "Dataset collection number {dataset_collection_id}"}}) '
            f'RETURN dataset_collection'
        )

        query_output = tx.run(query)  # Returns an iterative with one dataset collection
        logging.info(f"Run cypher query to generate dataset collection: {query}.")
        return [row["dataset_collection"]["dataset_collection_id"] for row in query_output][0]

    def generate_system_collection(self, system_collection_id):
        """Generates a system collection node with id and description based on the id.
        Creating a system collection with id 1 will create a node with these attributes:
            system_collection_id: 1,
            description: System collection number 1.

        Parameters
        ----------
        system_collection_id : int
            System collection id, is unique.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                generated_id = session.write_transaction(self._generate_system_collection, system_collection_id)
                logging.info(f"Generated system collection {generated_id}")

            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_system_collection(tx, system_collection_id):
        """Creates and runs a query for system collection creation."""
        query = (
            f'MERGE (system_collection:system_collection {{system_collection_id: {system_collection_id}, '
            f'description: "System collection number {system_collection_id}"}}) '
            f'RETURN system_collection'
        )

        query_output = tx.run(query)  # Returns an iterative with one system collection
        logging.info(f"Run cypher query to generate system collection: {query}.")
        return [row["system_collection"]["system_collection_id"] for row in query_output][0]

    def generate_dataset(self, dataset_id, dataset_collection_id, slo):
        """Generates a dataset node with id, collection id, regex, name, slo and description.
        Creating a dataset with id 1, collection id 2, slo '01:00:00' will create a node with these attributes:
            dataset_id: 1,
            dataset_collection_id: 2
            regex_grouping: data.1.*
            dataset_name: dataset.1
            dataset_description: Dataset number 1
            slo: 01:00:00

        Creating connection between dataset with id 1 and dataset collection with id 2.

        Parameters
        ----------
        dataset_id : int
            Dataset id, is unique.

        dataset_collection_id: int
            Dataset collection this dataset belongs to.

        slo: str
            Represents how often the dataset is written.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                session_log = session.write_transaction(self._generate_dataset, dataset_id, dataset_collection_id, slo)
                for row in session_log:
                    logging.info(f'Generated dataset {row["dataset"][0]} with regex grouping {row["dataset"][1]} '
                                 f'and dataset collection {row["dataset_collection"]}')
            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_dataset(tx, dataset_id, dataset_collection_id, slo):
        """Creates and runs a query for dataset creation. Creates a link to a corresponding dataset collection."""
        query = (
            f'MATCH (dataset_collection:dataset_collection) '
            f'WHERE dataset_collection.dataset_collection_id = {dataset_collection_id} '
            f'MERGE (dataset:dataset {{dataset_id: {dataset_id}, dataset_collection_id: {dataset_collection_id}, '
            f'regex_grouping: "data.{dataset_id}.*", dataset_name: "dataset.{dataset_id}", '
            f'dataset_description: "Dataset number {dataset_id}", slo: "{slo}" }}) '
            f'MERGE (dataset_collection) -[:CONTAINS] -> (dataset) '
            f'RETURN dataset_collection, dataset'
        )

        query_output = tx.run(query)
        logging.info(f"Run cypher query to create a dataset: {query}.")
        return [{"dataset_collection": row["dataset_collection"]["dataset_collection_id"],
                 "dataset": (row["dataset"]["dataset_id"], row["dataset"]["regex_grouping"])} for row in query_output]

    def generate_system(self, system_id, system_critic, system_collection_id):
        """Generates a system with id and description based on the id.
        Creating a system with id 1, collection id 2, system_critic 'LEVEL_1' will create a node with these attributes:
            system_id: 1,
            system_collection_id: 2
            regex_grouping: system.1.*
            system_name: system.1
            system_description: System number 1
            system_critic: LEVEL_1

        Creating a connection between system with id 1 and system collection with id 2.

        Parameters
        ----------
        system_id : int
            System id, is unique.

        system_critic: str
            System criticality type.

        system_collection_id: int
            System collection this system belongs to.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                session_log = session.write_transaction(self._generate_system, system_id, system_collection_id, system_critic)
                for row in session_log:
                    logging.info(f'Generated system {row["system"][0]} with regex grouping {row["system"][1]} and '
                                 f'system collection {row["system_collection"]}')

            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_system(tx, system_id, system_collection_id, system_critic):
        """Creates and runs a query for system creation. Creates a link to a corresponding system collection."""
        query = (
            f'MATCH (system_collection:system_collection) '
            f'WHERE system_collection.system_collection_id = {system_collection_id} '
            f'MERGE (system:system {{system_id: {system_id}, system_collection_id: {system_collection_id}, '
            f'regex_grouping: "system.{system_id}.*", system_name: "system.{system_id}", '
            f'system_description: "System number {system_id}", system_critic: {system_critic}}}) '
            f'MERGE (system_collection) -[:CONTAINS] -> (system) '
            f'RETURN system_collection, system'
        )

        query_output = tx.run(query)
        logging.info(f"Run cypher query to create a system: {query}.")
        return [{"system_collection": row["system_collection"]["system_collection_id"],
                 "system": (row["system"]["system_id"], row["system"]["regex_grouping"])} for row in query_output]

    def generate_processing(self, system_id, dataset_id, processing_id, impact, freshness, action="INPUTS"):
        """Generates a dataset processing node with id, impact and freshness.

        Creating a processing node with id 1, dataset id 2, system is 3, impact LEVEL_2, freshness LEVEL_1,
        action=INPUTS, will create a node with these attributes:
            processing_id: 1,
            impact: LEVEL_2
            freshness: LEVEL_1

        Also, will create connections between dataset with id 2 and processing with id 1, connection INPUTS between
        system id 3 and processing id 1.

        Parameters
        ----------
        system_id: int
            System id that creates this processing.

        dataset_id : int
            Dataset id that inputs or outputs the system.

        processing_id: int
            Dataset processing id, is unique.

        impact: str
            Represents the impact of the dataset on the system.

        freshness: str
            Represent freshness criticality for dataset staleness.

        action: str, optional
            Denotes if the dataset is an input to the system, or the output.
            Can be one of: INPUTS, or OUTPUTS. INPUTS by default.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                session_log = session.write_transaction(self._generate_processing, system_id, dataset_id, processing_id,
                                                        impact, freshness, action)
                for row in session_log:
                    logging.info(f'Generated processing {row["processing"]} of dataset '
                                 f'{row["dataset"]} by system {row["system"]}.')

            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_processing(tx, system_id, dataset_id, processing_id, impact, freshness, action):
        """Creates and runs a query for dataset processing creation.
        Creates two links to each a corresponding dataset, and a corresponding system."""
        query = (
            f'MATCH (system:system), (dataset:dataset) '
            f'WHERE system.system_id = {system_id} AND dataset.dataset_id = {dataset_id} '
            f'MERGE (processing:processing {{processing_id: {processing_id}, impact: {impact}, freshness: {freshness}}}) '
            f'MERGE (processing) -[:{action}] -> (dataset) '
            f'MERGE (system) - [:CREATES] -> (processing) '
            f'RETURN dataset, system, processing '
        )

        query_output = tx.run(query)
        logging.info(f"Run cypher query to create dataset processing: {query}.")
        return [{"dataset": row["dataset"]["dataset_id"],
                 "system": row["system"]["system_id"],
                 "processing": row["processing"]["processing_id"]} for row in query_output]

    def generate_env(self, env_id, env_name, env_owner, env_oncall):
        """Generates an environment node.
        Creating an environment with id 1, env_name test, env owner John Smith, and env oncall Sammy Jones
        will create a node with these attributes:
            env_id: 1,
            name: test,
            owner: John Smith,
            oncall: Sammy Jones

        Parameters
        ----------
        env_id : int
            Environment id, is unique.

        env_name: str
            Name of the environment

        env_owner: str
            Owner of the environment

        env_oncall: str
            Oncall for the environment

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """
        with self.driver.session() as session:
            try:
                generated_id = session.write_transaction(self._generate_env, env_id, env_name, env_owner, env_oncall)
                logging.info(f'Generated environment {generated_id}.')
            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_env(tx, env_id, env_name, env_owner, env_oncall):
        """Creates and runs a query for environment creation."""
        query = (
            f'MERGE (env:env {{env_id: {env_id}, name: "{env_name}", owner: "{env_owner}", oncall:"{env_oncall}"}}) '
            f'RETURN env'
        )
        query_output = tx.run(query)  # Returns an iterable with one environment
        logging.info(f"Run cypher query to generate environment: {query}.")
        return [row["env"]["env_id"] for row in query_output][0]

    def generate_data_integrity(self, data_integrity_id, dataset_id, data_integrity_rec_time, data_integrity_volat,
                                data_integrity_reg_time, data_integrity_rest_time):
        """Generates a data integrity node.
        Creating a data integrity with id 1, dataset_id 2, reconstruction time 100 s, volatality True,
        regeneration time 150 s, restoration time 10 s will create a node with these attributes:
            data_integrity_id: 1,
            data_integrity_rec_time: 100,
            data_integrity_volat: True,
            data_integrity_reg_time: 150,
            data_integrity_rest_time: 10

        Parameters
        ----------
        data_integrity_id : int
            Data integrity id, is unique.

        dataset_id: int
            Id of the dataset with this data integrity.

        data_integrity_rec_time: int
            Reconstruction time of the dataset, seconds.

        data_integrity_volat: bool
            Is the dataset volatile

        data_integrity_reg_time: int
            Regeneration time of the dataset, seconds.

        data_integrity_rest_time: int
            Restoration time of the dataset, seconds.

        Raises
        ------
        ServiceUnavailable
            Web socket error that occurs when having problems connecting to neo4j.
        """

        with self.driver.session() as session:
            try:
                session_log = session.write_transaction(self._generate_data_integrity, data_integrity_id, dataset_id,
                                                        data_integrity_rec_time, data_integrity_volat, data_integrity_reg_time,
                                                        data_integrity_rest_time)
                for row in session_log:
                    logging.info(f'Generated data integrity {row["dataset"]} for dataset {row["data_integrity"]}.')
            except ServiceUnavailable as exception:
                logging.error(exception)
                raise

    @staticmethod
    def _generate_data_integrity(tx, data_integrity_id, dataset_id, data_integrity_rec_time, data_integrity_volat,
                                 data_integrity_reg_time, data_integrity_rest_time):
        """Creates and runs a query for data integrity creation. Creates a link to a corresponding dataset."""
        query = (
            f'MATCH (dataset:dataset) WHERE dataset.dataset_id = {dataset_id} '
            f'MERGE (data_integrity:data_integrity {{data_integrity_id: {data_integrity_id}, '
            f'data_integrity_rec_time: {data_integrity_rec_time}, data_integrity_volat: {data_integrity_volat}, '
            f'data_integrity_reg_time: {data_integrity_reg_time}, data_integrity_rest_time: {data_integrity_rest_time} }}) '
            f'MERGE (dataset) -[:has] -> (data_integrity) '
            f'RETURN dataset, data_integrity '
        )

        query_output = tx.run(query)
        logging.info(f"Run cypher query to generate data integrity: {query}.")
        return [{"dataset": row["dataset"]["dataset_id"],
                 "data_integrity": row["data_integrity"]["data_integrity_id"]} for row in query_output]
