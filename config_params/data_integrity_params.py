class DataIntegrityParams:
    def __init__(self, data_restoration_range_seconds, data_regeneration_range_seconds,
                 data_reconstruction_range_seconds, data_volatility_proba_map):
        self.data_restoration_range_seconds = data_restoration_range_seconds
        self.data_regeneration_range_seconds = data_regeneration_range_seconds
        self.data_reconstruction_range_seconds = data_reconstruction_range_seconds
        self.data_volatility_proba_map = data_volatility_proba_map
