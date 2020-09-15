class ConnectionParams:
    def __init__(self, system_write_conn_count, system_read_conn_count):
        self.system_write_conn_count = system_write_conn_count
        self.system_read_conn_count = system_read_conn_count
        self.dataset_system_connection_count = system_write_conn_count + system_read_conn_count
