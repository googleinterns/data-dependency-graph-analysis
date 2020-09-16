class ConnectionParams:
    def __init__(self, system_write_connection_count, system_read_connection_count):
        self.system_write_connection_count = system_write_connection_count
        self.system_read_connection_count = system_read_connection_count
        self.dataset_system_connection_count = system_write_connection_count + system_read_connection_count
