import math

class PreprocessingData():
    def sanitize_data(data):
        if isinstance(data, dict):
            return {k: PreprocessingData.sanitize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [PreprocessingData.sanitize_data(v) for v in data]
        elif isinstance(data, float) and (math.isinf(data) or math.isnan(data)):
            return None
        return data