from utils.json_utils import open_file
from model.config import Config
from model.parameters import Parameters
from processor.ingestion_test_processor import IngestionTestProcessor


def test():
    config = Config.parse_obj(open_file("config.json"))
    params = Parameters.parse_obj(open_file("parameters.json"))
    test_processor = IngestionTestProcessor(config, params)
    test_processor.ingestion_test_process()


if __name__ == "__main__":
    test()
