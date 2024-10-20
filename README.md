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

Manage tables.

```python
from aftonfalk.models.enums_ import SqlServerDataType
from aftonfalk.models.types_ import Column
from aftonfalk.models.tables.clean import RawTable

raw_table = RawTable(
    unique_columns=[
        Column(
            name="unique_key",
            data_type=SqlServerDataType.NVARCHAR.with_length(50),
            constraints="NOT NULL",
        ),
    ]
)
```

then you can do things like easily:

creating tables
``` python
path = "catalog.schema.table"
ddl = raw_table.table_ddl(path=path)
driver.create_table(path=path, ddl=ddl)
```

inserting into tables
```python
data = ...
path = "catalog.schema.table"
insert_stmt = raw_table.insert_sql(path=path)
driver.write(sql=insert_stmt, data=data)
```

## Notes