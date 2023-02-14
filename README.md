# Polars Extractor

A simple data extraction script that can be deployed as a container.
This uses Python, with Polars, ConnectorX and Pyarrow. The first two are implemented in Rust, and the second in C++.
These libs do the heavy lifting, while Python binds it all.

This can be a lot less resource intensive then running PySpark (uses JVM), and a lot faster than using Pandas (Python implementation).

## Parameters:


| NAME              | DESCRIPTION                                        | DEFAULT                    |
|-------------------|----------------------------------------------------|----------------------------|
| DATABASE_URL      | Database connection URL with credentials           |                            |
| TABLE             | DB Table                                           |                            |
| SCHEMA            | DB Schema                                          | public                     |
| WRITE_PATH        | Destination Path or fsspec URL                     |                            |
| QUERY_OVERWRITE   | Query string to overwrite the default              | select * from schema.table |
| WRITE_PARTITIONED | If filled, will use column as hive like partition  |                            |
| READ_PARTITIONED  | Makes the script read the data in parts (in parallel)            |                            |
| PARTITION_NUMBER  | If READ_PARTITIONED, instructs the amount of them. | 4                          |



## Running it locally:

```
DATABASE_URL=postgresql://user:password@localhost:5432/cast_concursos TABLE=table_name WRITE_PATH="." WRITE_PARTITIONED="partition_column" python main.py
```