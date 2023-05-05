from urllib.parse import urlparse
import logging
import polars as pl
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import math, os, time
from typing import Iterator
import connectorx as cx

GB = 1000000000

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)


def mustEnv(env_name, default=None) -> str:
    env_var = os.getenv(env_name, default)
    if env_var is None:
        logging.fatal(f"missing env variable {env_name}")
    return str(env_var)


TABLE = mustEnv("TABLE")
SCHEMA = mustEnv("SCHEMA", "public")
DATABASE_URL = mustEnv("DATABASE_URL")
WRITE_PATH = mustEnv("WRITE_PATH")
COMPRESSION = mustEnv("COMPRESSION", "zstd")  # snappy is another good option
SPLIT_OUTPUT = mustEnv("SPLIT_OUTPUT", False)
QUERY_OVERWRITE = mustEnv("QUERY_OVERWRITE", "")
WRITE_PARTITIONED = mustEnv("WRITE_PARTITIONED", "")

PARTITION_COLUMN = mustEnv("PARTITION_COLUMN", "")
GB_IN_SPLIT = mustEnv(
    "GB_IN_SPLIT", "1.0"
)  # for the partitioned reading with "PARTITION_COLUMN"
try:
    GB_IN_SPLIT = float(GB_IN_SPLIT)
except:
    GB_IN_SPLIT = 1.0


# Athena does not support ns timestamps. This function will find all ns timestamps and cast them to ms
def date_to_ms(df: pl.LazyFrame) -> pl.LazyFrame:
    for ix, col_type in enumerate(df.dtypes):
        if col_type == pl.Datetime:
            logging.info(
                f"Going to cast column {df.columns[ix]} from {col_type} to datetime[ms]"
            )
            df = df.with_columns(
                [pl.col(df.columns[ix]).dt.cast_time_unit("ms").alias(df.columns[ix])]
            )
    return df


query_prefix = """-- polars extractor query
"""


def read_sql(
    sql: list[str] | str,
    connection_uri: str,
    partition_on: str | None = None,
    partition_range: tuple[int, int] | None = None,
    partition_num: int | None = None,
) -> pl.DataFrame:
    logging.info(f"Running query: {sql}")
    arrow_table = cx.read_sql(
        query=query_prefix + sql,
        conn=connection_uri,
        return_type="arrow2",
        partition_on=partition_on,
        partition_range=partition_range,
        partition_num=partition_num,
        protocol="binary",
    )
    return pl.from_arrow(arrow_table)


# read_plain is the simplest way to read data. This can be used on most tables.
# the important part: the entire table will be in memory after reading.
# if your table is too big, use read_partitioned_iterator
def read_plain(conn) -> pl.LazyFrame:
    if QUERY_OVERWRITE == "":
        query = f"select * from {SCHEMA}.{TABLE}"

    logging.info(f"READING DATASET FROM DATABASE FROM TABLE {TABLE}")
    df = read_sql(query, conn).lazy()
    return df


# this method uses partitioning to make the extraction faster.
# The usefulness of this may vary.
def read_partitioned_connectorx(conn) -> pl.LazyFrame:
    query = QUERY_OVERWRITE
    if QUERY_OVERWRITE == "":
        query = f"select * from {SCHEMA}.{TABLE}"

    logging.info(f"READING DATASET FROM DATABASE WITH PARTITIONS FROM TABLE {TABLE}")
    df = read_sql(query, conn, partition_on=PARTITION_COLUMN, partition_num=None).lazy()
    return df


# read_partitioned_iterator is for tables that are too big to fit in memory.
# Instead of reading the entire table, it relies on using a column as a cursor
def read_partitioned_iterator(conn) -> Iterator[pl.LazyFrame]:
    if PARTITION_COLUMN == "":
        logging.error("PARTITION_COLUMN EMPTY.")
        return

    # ranges query
    query_min_max = f"""select min({PARTITION_COLUMN}) as min_id, 
                max({PARTITION_COLUMN}) as max_id, 
                pg_total_relation_size('{SCHEMA}.{TABLE}') as size 
            from {SCHEMA}.{TABLE}"""

    df_min_max = read_sql(query_min_max, conn).to_dicts()[0]

    parts = math.ceil(df_min_max["size"] / (GB_IN_SPLIT * GB))
    logging.info(f"Will read database in {parts} parts.")
    logging.info(f"Total database size {int(round(df_min_max.get('size') / GB))} GB")

    # this will create a Generator. It is a special type of iterator
    # where its contents are only retrieved when iterated.
    # In this case, read_sql will be called only when needed.
    # and its a inline function for no good reason
    def query_generator_ranges(start, stop, parts) -> Iterator[pl.LazyFrame]:
        step = math.ceil((stop - start) / parts)

        query = QUERY_OVERWRITE
        if QUERY_OVERWRITE == "":
            query = f"select * from {SCHEMA}.{TABLE}"

        return (
            (
                read_sql(
                    f"""select sb.* from ({query}) sb 
                where {PARTITION_COLUMN} >= {start + step * i} 
                and {PARTITION_COLUMN} < {start + step * (i + 1)}""",
                    conn,
                ).lazy()
            )
            for i in range(0, parts)
        )

    return query_generator_ranges(df_min_max["min_id"], df_min_max["max_id"], parts)


# this is useful when you have a column that can be used as a partition when running
# queries in your datalake "year" is a common one
def write_partitioned_basic(df: pl.LazyFrame, write_path: str):
    logging.info(
        f"Writing with partitions on column {WRITE_PARTITIONED} in prefix: {write_path}"
    )
    file_options = ds.ParquetFileFormat().make_write_options(compression=COMPRESSION)

    pq.write_to_dataset(
        df.collect().to_arrow(),
        root_path=write_path,
        format="parquet",
        file_options=file_options,
        compression=COMPRESSION,
        partition_cols=[WRITE_PARTITIONED],
        existing_data_behavior="delete_matching",
    )


# writes a single file with the output
def write_plain(df: pl.LazyFrame, write_path: str):
    logging.info(f"Writing in one file in {write_path}")
    df: pl.DataFrame = df.collect()
    if len(df) == 0:
        logging.warning(f"Empty dataframe {write_path}")
        return
    try:
        df.write_parquet(write_path, compression=COMPRESSION)

    except Exception as e:
        logging.error(e, "using PyArrow")
        pq.write_table(
            df.to_arrow(),
            where=write_path,
            compression=COMPRESSION,
        )


# writes multiple files instead of a single one
def write_multiple_files(df: pl.LazyFrame, write_path: str):
    file_options = ds.ParquetFileFormat().make_write_options(compression=COMPRESSION)
    logging.info(f"Writing in multiple files in {write_path}")
    ds.write_dataset(
        df.collect().to_arrow(),
        base_dir=write_path,
        format="parquet",
        file_options=file_options,
        existing_data_behavior="delete_matching",
    )


def main():
    urlparse(DATABASE_URL)
    conn = DATABASE_URL

    df: pl.LazyFrame

    if PARTITION_COLUMN:
        dfs = read_partitioned_iterator(conn)
        part = 0
        result_files = []
        for df in dfs:
            df = date_to_ms(df)
            # Check
            write_path = f"{WRITE_PATH}-part-{part}.parquet"
            write_plain(df, write_path)
            del df  # lets help the GC and tell it to delete the dataframe
            result_files.append(write_path)
            part += 1

        logging.info(f"Write successful. Files created: {result_files}")
        return

    else:
        df = read_plain(conn)

        df = date_to_ms(df)

        if WRITE_PARTITIONED:
            write_partitioned_basic(df, WRITE_PATH)
        elif SPLIT_OUTPUT:
            write_multiple_files(df, WRITE_PATH)
        else:
            write_plain(df, WRITE_PATH + ".parquet")
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
