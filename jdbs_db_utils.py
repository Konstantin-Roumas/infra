import os

from pyspark.sql import SparkSession, DataFrame
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

read_table_schema=os.environ.get("DB_SCHEMA_WRITE",'params')
write_table_schema=os.environ.get("DB_SCHEMA_READ",'params')
creds = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "driver": "org.postgresql.Driver",
    'batchsize': '5000',
    'reWriteBatchedInserts': 'True'

}
output_table_postfix=''

jdbc_url = os.environ.get("DB_HOST")
#TODO add jdbc_url un signature
def read_jdbc_data(table_name: str, spark: SparkSession, schema: str= read_table_schema,properties=creds) -> DataFrame:
    """Reads data from database table to dataframe."""
    logger.info(f"Reading table: params.{table_name}")
    return spark.read.jdbc(url=jdbc_url, table=schema+'.' + table_name, properties=properties)


def write_jdbc_data(dataframe: DataFrame, table_name: str, mode :str= 'append', schema: str= write_table_schema ,properties=creds) -> None:
    """Writes data from dataframe to database table."""
    # if dataframe.isEmpty():
    #     logger.warning(f"DataFrame for table params.{table_name} is empty. Skipping write operation.")
    #     return
    #TODO- включить только по договоренности.Сильно снижает быстродействие.
    #count = dataframe.count()
    count=''
    logger.info(f"Writing {count} rows to params.{table_name}")
    dataframe.write.jdbc(url=jdbc_url, mode=mode, table=schema+'.' + table_name + output_table_postfix, properties=properties)