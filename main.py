from urllib.parse import urlparse
import logging
import polars as pl
import pyarrow.parquet as pq
import os, time
from datetime import datetime
import connectorx as cx

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)


def mustEnv(env_name, default=None) -> str:
    env_var = os.getenv(env_name, default)
    if env_var == None:
        logging.fatal(f"missing env variable {env_name}")
    return str(env_var)


TABLE = mustEnv("TABLE")
SCHEMA = mustEnv("SCHEMA", "public")
DATABASE_URL = mustEnv("DATABASE_URL")
WRITE_PATH = mustEnv("WRITE_PATH")

QUERY_OVERWRITE = mustEnv("QUERY_OVERWRITE", "")
SCHEMA = mustEnv("SCHEMA", "public")

WRITE_PARTITIONED = mustEnv("WRITE_PARTITIONED", "")
PARTITION_NUMBER = mustEnv("PARTITION_NUMBER", "4")
READ_PARTITIONED = mustEnv("READ_PARTITIONED", "")


# Athena does not support ns timestamps. This function will find all ns timestamps and cast them to ms
def date_to_ms(df: pl.DataFrame) -> pl.DataFrame:
    dfl = df.lazy()
    for ix, col_type in enumerate(df.dtypes):
        if col_type == pl.datatypes.Datetime:
            logging.info(
                f"Going to cast column {df.columns[ix]} from {col_type} to datetime[ms]"
            )
            dfl = dfl.with_column(
                pl.col(df.columns[ix]).dt.cast_time_unit("ms").alias(df.columns[ix])
            )
    return dfl.collect()


def read_sql(
    sql: list[str] | str,
    connection_uri: str,
    partition_on: str | None = None,
    partition_range: tuple[int, int] | None = None,
    partition_num: int | None = None,
) -> pl.DataFrame:
    arrow_table = cx.read_sql(
        query=sql,
        conn=connection_uri,
        return_type="arrow2",
        partition_on=partition_on,
        partition_range=partition_range,
        partition_num=partition_num,
        protocol="binary",
    )
    return pl.from_arrow(arrow_table)


# this function will read all data from a database and write it to a destination path
# it will write one or more files (depending on WRITE_PARTITIONED), but always overwriting the previus results
def overwriteRun(conn):
    query = QUERY_OVERWRITE
    if QUERY_OVERWRITE == "":
        query = f"select * from {SCHEMA}.{TABLE}"
    df = pl.DataFrame
    if READ_PARTITIONED:
        logging.info("READING DATASET FROM DATABASE WITH PARTITIONS")
        df = pl.read_sql(
            query,
            conn,
            partition_on=READ_PARTITIONED,
            partition_num=int(PARTITION_NUMBER),
        )
    else:
        logging.info("READING DATASET FROM DATABASE")
        df = read_sql(query, conn)
    df = df.with_column(
        pl.lit(datetime.now()).alias("_datalake_insert_date").dt.cast_time_unit("ms")
    )
    df = date_to_ms(df)
    if WRITE_PARTITIONED:
        write_path = WRITE_PATH
        logging.info(
            f"Writing with partitions on column {WRITE_PARTITIONED} in prefix: {write_path}"
        )

        pq.write_to_dataset(
            df.to_arrow(),
            root_path=write_path,
            existing_data_behavior="overwrite_or_ignore",
            partition_cols=[WRITE_PARTITIONED],
        )
    else:
        write_path = f"{WRITE_PATH}/{TABLE}.snappy.parquet"
        logging.info(f"Writing in one file in {write_path}")
        pq.write_table(
            df.to_arrow(),
            where=write_path,
            version="2.6",
            compression="snappy",
            data_page_version="2.0",
        )


def main():
    urlparse(DATABASE_URL)
    conn = DATABASE_URL
    overwriteRun(conn)

    logging.info("Write successful")


def elapsed_time(start):
    end = time.time()
    dur = end - start
    logging.info(f"Took {dur} seconds")


if __name__ == "__main__":
    start = time.time()
    try:
        main()
        elapsed_time(start)
        logging.info("SUCCESS")
        exit(0)
    except Exception as e:
        logging.error("ERROR: exception when trying to run main")
        elapsed_time(start)
        logging.error(e)
        raise (e)
