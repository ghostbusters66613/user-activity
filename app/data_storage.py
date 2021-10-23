from typing import AnyStr


class DataStorage:
    def __init__(self, spark):
        self._spark = spark

    def load_json(self, file_path: AnyStr):
        return self._spark.read.json(file_path)

    def load_parquet(self, file_path: AnyStr):
        return self._spark.read.parquet(file_path)

    @staticmethod
    def save_to_parquet(df, output_path: AnyStr):
        df.write\
            .mode('overwrite')\
            .parquet(output_path)
