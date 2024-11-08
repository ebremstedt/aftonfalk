# Aftonfalk

Aftonfalk is a module that makes it easier to interact with microsoft sql server. You can see it as an extension to pyodbc

## How to install

```sh
pip install aftonfalk
```

## Example usage

Instantiate a driver:
```python
driver = MssqlDriver(
    dsn="mssql://sa:Password1!@host.docker.internal:31433",
    driver=r"{ODBC Driver 18 for SQL Server}"
)
```

Dropping a table using `execute`

```python
driver.execute(sql=f"DROP TABLE source_system.schema.table;")
```

`read` & `write` data
```python
data = driver.read(query="select * from tablex")
driver.write(sql="INSERT INTO tablex (column_name) VALUES ('column_value')", data=data)
```

Manage tables. Generate them programatically.

```python
from aftonfalk.mssql.enums_ import (
    SqlServerDataType,
    SqlServerTimeZone,
    WriteMode,
    SqlServerIndexType,
)
from aftonfalk.mssql.types_ import Column, Table, Index, Path

source_database = "source_database"
source_schema = "source_schema"

destination_database = "destination_database"
destination_schema = "destination_schema"

DATA_MODIFIED = Column(
    name="data_modified", data_type="DATETIMEOFFSET", constraints="NOT NULL"
)

DEFAULT_COLUMNS = [
    Column(name="metadata_modified", data_type="DATETIMEOFFSET", constraints="NOT NULL"),
    DATA_MODIFIED,
    Column(name="data", data_type="NVARCHAR(MAX)", constraints="NOT NULL"),
]

table_and_key = {
    "FactTable": "FactKey",
    "Dimensiontable": "DimensionKey"
}
tables = {}

for name, key in table_and_key.items():
    tables[name] = Table(
        source_path=Path(database=source_database, schema=source_schema, table=name),
        destination_path=Path(database=destination_database, schema=destination_schema, table=name),
        source_data_modified_column_name="RowUpdatedAt",
        timezone=SqlServerTimeZone.CENTRAL_EUROPEAN_STANDARD_TIME,
        default_columns=DEFAULT_COLUMNS,
        write_mode=WriteMode.APPEND,
        batch_size=200,
        unique_columns=[
            Column(
                name=f"{key}",
                data_type=SqlServerDataType.NVARCHAR.with_length(50),
                constraints="NOT NULL",
            )
        ],
        indexes=[
            Index(
                index_type=SqlServerIndexType.NONCLUSTERED,
                columns=[DATA_MODIFIED]
            )
        ]
    )

```

then you can do things like easily:

creating tables
``` python
driver.create_table(path=table.destination_path, ddl=table.table_ddl())
```

inserting into tables
```python
data = ...
driver.write(sql=table.insert_sql(), data=data)
```
